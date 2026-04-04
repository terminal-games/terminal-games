// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

import { OpusAudioPlayer } from './opus-audio-player.js';

const MIN_BUFFER_MS = 50;
const MAX_BUFFER_MS = 1000;

await document.fonts.ready;

const term = new Terminal({
    scrollback: 0,
    fontFamily: '"JetBrainsMonoNL NFM", monospace',
    fontSize: 14,
});

term.write("\x1b[?1049h");

const fitAddon = new FitAddon.FitAddon();
term.loadAddon(fitAddon);

try {
    const webglAddon = new WebglAddon.WebglAddon();
    term.loadAddon(webglAddon);
} catch (e) {
    console.warn('WebGL renderer not available, using default renderer:', e);
}

term.open(document.getElementById('terminal'));
fitAddon.fit();

const audioPlayer = new OpusAudioPlayer();
await audioPlayer.init();

const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const baseQuery = new URLSearchParams(window.location.search);
let socket = null;

let smoothedPeakLatency = 0;
let lastTargetBufferMs = 0;
let pingInterval = null;
const spinnerFrames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const POW_SPINNER_INTERVAL_MS = 120;
let spinnerInterval = null;
let spinnerFrame = 0;
let overlay = null;
const titleLabel = ' Terminal Games ';
const styledTitle = '\x1b[38;2;23;23;23m\x1b[48;2;152;195;121m Terminal Games \x1b[0m';
const restoreTerminalState =
    '\x1b[0m\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1004l\x1b[?1005l\x1b[?1006l\x1b[?1015l\x1b[?2004l\x1b[?1l\x1b[>4;0m\x1b[<u\x1b>\x1b[?1049l\x1b[?25h';

const measureLatency = () => {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(`ping:${performance.now()}`);
    }
};

const onSocketMessage = async (event) => {
    if (typeof event.data === 'string') {
        if (event.data.startsWith('queue:')) {
            if (event.data === 'queue:allowed') {
                overlay = null;
                stopQueueSpinner();
                term.write('\x1b[2J\x1b[H');
            } else if (event.data.startsWith('queue:queued:')) {
                const position = Number.parseInt(event.data.substring('queue:queued:'.length), 10);
                if (Number.isFinite(position) && position >= 1) {
                    overlay = { type: 'queue', position };
                    startQueueSpinner();
                    renderOverlay();
                }
            } else if (event.data.startsWith('queue:rejected:')) {
                showDisconnect(event.data.substring('queue:rejected:'.length));
            }
            return;
        }
        if (event.data.startsWith('session:closed:')) {
            showDisconnect(event.data.substring('session:closed:'.length));
            return;
        }
        if (event.data.startsWith('pong:')) {
            const pingTimestamp = parseFloat(event.data.substring(5));
            const rtt = performance.now() - pingTimestamp;

            const decay = Math.max(0, (smoothedPeakLatency - rtt) * 0.10);
            smoothedPeakLatency = Math.max(rtt, smoothedPeakLatency - decay);

            const targetBufferMs = Math.max(MIN_BUFFER_MS, Math.min(MAX_BUFFER_MS, smoothedPeakLatency * 1.2));
            const deltaMs = targetBufferMs - lastTargetBufferMs;
            lastTargetBufferMs = targetBufferMs;

            if (audioPlayer.workletNode && deltaMs !== 0) {
                audioPlayer.workletNode.port.postMessage({
                    type: 'adjustBufferMs',
                    deltaMs: deltaMs
                });
            }
        }
        return;
    }

    if (!(event.data instanceof ArrayBuffer) || event.data.byteLength < 1) return;

    const data = new Uint8Array(event.data);
    const type = data[data.length - 1];
    const payload = data.subarray(0, data.length - 1);

    if (type === 0x00) {
        try {
            const stream = new ReadableStream({
                start(controller) {
                    controller.enqueue(payload);
                    controller.close();
                }
            });
            const reader = stream.pipeThrough(new DecompressionStream('deflate-raw')).getReader();
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                term.write(value);
            }
        } catch (error) {
            console.error('Decompression error:', error);
            term.write(payload);
        }
    } else if (type === 0x01) {
        audioPlayer.onAudioData(payload);
    }
};

const onSocketError = (error) => {
    console.error('WebSocket error:', error);
    if (overlay?.type === 'disconnect') {
        return;
    }
    showDisconnect('connection_error');
};

const onSocketClose = (event) => {
    stopQueueSpinner();
    const rejectedReason =
        typeof event.reason === 'string' && event.reason.startsWith('rejected:')
            ? event.reason.substring('rejected:'.length)
            : null;
    const closedReason =
        typeof event.reason === 'string' && event.reason.startsWith('closed:')
            ? event.reason.substring('closed:'.length)
            : null;
    if (rejectedReason || closedReason) {
        showDisconnect(rejectedReason ?? closedReason);
    } else if (overlay?.type !== 'disconnect') {
        showDisconnect('connection_closed');
    }
    if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
    }
    audioPlayer.close();
};

try {
    await startWebSocketSession();
} catch (error) {
    console.error('Failed to start WebSocket session:', error);
    if (error instanceof Error && error.message === 'server_shutdown') {
        showDisconnect('server_shutdown');
    } else {
        showDisconnect('connection_error');
    }
}

term.onData((data) => {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(new TextEncoder().encode(data).buffer);
    }
});

term.onBinary((data) => {
    if (socket && socket.readyState === WebSocket.OPEN) {
        const bytes = new Uint8Array(data.length);
        for (let i = 0; i < data.length; i++) {
            bytes[i] = data.charCodeAt(i) & 0xFF;
        }
        socket.send(bytes.buffer);
    }
});

term.onResize((size) => {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(`resize:${size.cols}:${size.rows}`);
    }
    if (overlay !== null) {
        renderOverlay();
    }
});
window.addEventListener('resize', () => fitAddon.fit());

function centeredCol(text) {
    return Math.max(1, Math.floor((term.cols - text.length) / 2) + 1);
}

function renderQueueScreen() {
    if (overlay?.type !== 'queue') {
        return;
    }
    renderCenteredScreen([
        styledTitle,
        `${spinnerFrames[spinnerFrame]} Loading...`,
        `Position in queue: ${overlay.position}`
    ]);
}

function renderDisconnectScreen(reasonKey) {
    const { heading, detail, hint } = disconnectMessage(reasonKey);
    renderCenteredScreen([
        styledTitle,
        `\x1b[38;2;248;113;113m${heading}\x1b[0m`,
        detail,
        `\x1b[2m${hint}\x1b[0m`
    ], true);
}

function renderOverlay() {
    if (overlay?.type === 'queue') {
        renderQueueScreen();
    } else if (overlay?.type === 'disconnect') {
        renderDisconnectScreen(overlay.reason);
    }
}

function showDisconnect(reason) {
    stopQueueSpinner();
    overlay = { type: 'disconnect', reason };
    renderOverlay();
}

function disconnectMessage(reasonKey) {
    switch (reasonKey) {
        case 'banned_ip':
            return {
                heading: 'Connection blocked',
                detail: 'Connections from your IP address are blocked.',
                hint: 'If this is unexpected, contact the operator.'
            };
        case 'too_many_connections_from_ip':
            return {
                heading: 'Too many sessions',
                detail: 'This IP address already has too many active sessions.',
                hint: 'Close another session or wait a few minutes and try again.'
            };
        case 'server_shutdown':
            return {
                heading: 'Maintenance in progress',
                detail: 'Server is shutting down for maintenance.',
                hint: 'Please try again after maintenance ends.'
            };
        case 'connection_error':
            return {
                heading: 'Connection error',
                detail: 'The WebSocket connection failed.',
                hint: 'Check your network connection and try again.'
            };
        case 'connection_lost':
            return {
                heading: 'Connection lost',
                detail: 'The connection to the server was lost.',
                hint: 'Refresh the page to reconnect.'
            };
        case 'normal_exit':
            return {
                heading: 'Session ended',
                detail: 'Thanks for playing!',
                hint: 'Refresh the page to start a new session.'
            };
        case 'idle_timeout':
            return {
                heading: 'Idle timeout',
                detail: 'You were disconnected due to inactivity.',
                hint: 'Refresh the page to start a new session.'
            };
        case 'cluster_limited':
            return {
                heading: 'Session closed',
                detail: 'This session was closed due to likely bot activity.',
                hint: 'If this is unexpected, contact the operator.'
            };
        case 'connection_closed':
        default:
            return {
                heading: 'Disconnected',
                detail: 'The WebSocket connection closed.',
                hint: 'Refresh the page to reconnect.'
            };
    }
}

function startQueueSpinner() {
    if (spinnerInterval !== null) {
        return;
    }
    spinnerInterval = setInterval(() => {
        spinnerFrame = (spinnerFrame + 1) % spinnerFrames.length;
        renderQueueScreen();
    }, 100);
}

function stopQueueSpinner() {
    if (spinnerInterval !== null) {
        clearInterval(spinnerInterval);
        spinnerInterval = null;
    }
}

async function startWebSocketSession() {
    const proof = await solveProofOfWork();
    const wsQuery = new URLSearchParams(baseQuery);
    wsQuery.set('pow_id', proof.id);
    wsQuery.set('pow_counter', String(proof.counter));
    const wsUrl = `${protocol}//${window.location.host}/ws?${wsQuery.toString()}`;

    socket = new WebSocket(wsUrl);
    socket.binaryType = 'arraybuffer';
    socket.onopen = () => {
        term.write('\x1b[2J\x1b[H');
        socket.send(`resize:${term.cols}:${term.rows}`);
        pingInterval = setInterval(measureLatency, 5000);
        measureLatency();
    };
    socket.onmessage = onSocketMessage;
    socket.onerror = onSocketError;
    socket.onclose = onSocketClose;
}

async function solveProofOfWork() {
    const response = await fetch('/pow/challenge', { cache: 'no-store' });
    if (!response.ok) {
        const detail = (await response.text()).trim();
        if (response.status === 503) {
            throw new Error('server_shutdown');
        }
        throw new Error(detail || 'failed to fetch POW challenge');
    }
    const challenge = await response.json();
    let attempts = 0;
    let frame = 0;
    renderPowScreen(frame, attempts);
    const spinnerTimer = setInterval(() => {
        frame = (frame + 1) % spinnerFrames.length;
        renderPowScreen(frame, attempts);
    }, POW_SPINNER_INTERVAL_MS);

    try {
        const counter = await solvePowCounter(challenge, (delta) => {
            attempts += delta;
        });
        term.write('\x1b[2J\x1b[H');
        return { id: challenge.id, counter };
    } finally {
        clearInterval(spinnerTimer);
    }
}

async function solvePowCounter(challenge, onAttempts) {
    const workerCount = Math.max(1, Math.min(navigator.hardwareConcurrency || 4, 8));
    if (typeof Worker === 'undefined' || workerCount <= 1) {
        return await solvePowSingleThreaded(challenge, onAttempts);
    }
    return await solvePowMultiThreaded(challenge, workerCount, onAttempts);
}

async function solvePowSingleThreaded(challenge, onAttempts) {
    let counter = 0;
    while (true) {
        for (let i = 0; i < 64; i++) {
            if (await validatePowAttempt(challenge.nonce, counter, challenge.difficulty)) {
                return counter;
            }
            counter++;
            onAttempts(1);
        }
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
}

async function solvePowMultiThreaded(challenge, workerCount, onAttempts) {
    const workerScript = `
self.onmessage = async (event) => {
    const { nonce, difficulty, start, step, batch } = event.data;
    let stopped = false;
    self.onmessage = (innerEvent) => {
        if (innerEvent.data && innerEvent.data.type === 'stop') {
            stopped = true;
        }
    };

    const countLeadingZeroBits = (bytes) => {
        let bits = 0;
        for (const byte of bytes) {
            if (byte === 0) {
                bits += 8;
                continue;
            }
            for (let i = 7; i >= 0; i--) {
                if ((byte & (1 << i)) === 0) bits++;
                else return bits;
            }
        }
        return bits;
    };

    const validate = async (counter) => {
        const payload = nonce + ':' + counter;
        const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload));
        return countLeadingZeroBits(new Uint8Array(digest)) >= difficulty;
    };

    let counter = start;
    while (!stopped) {
        for (let i = 0; i < batch; i++) {
            if (await validate(counter)) {
                self.postMessage({ type: 'found', counter });
                return;
            }
            counter += step;
        }
        self.postMessage({ type: 'progress', attempts: batch });
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
};
`;

    const blob = new Blob([workerScript], { type: 'application/javascript' });
    const workerUrl = URL.createObjectURL(blob);
    const workers = [];

    try {
        return await new Promise((resolve, reject) => {
            let done = false;
            const cleanup = () => {
                for (const worker of workers) {
                    worker.postMessage({ type: 'stop' });
                    worker.terminate();
                }
                URL.revokeObjectURL(workerUrl);
            };

            for (let i = 0; i < workerCount; i++) {
                const worker = new Worker(workerUrl);
                workers.push(worker);
                worker.onmessage = (event) => {
                    if (done) {
                        return;
                    }
                    const msg = event.data;
                    if (msg.type === 'progress') {
                        onAttempts(msg.attempts || 0);
                    } else if (msg.type === 'found') {
                        done = true;
                        cleanup();
                        resolve(msg.counter);
                    }
                };
                worker.onerror = (error) => {
                    if (done) {
                        return;
                    }
                    done = true;
                    cleanup();
                    reject(error);
                };
                worker.postMessage({
                    nonce: challenge.nonce,
                    difficulty: challenge.difficulty,
                    start: i,
                    step: workerCount,
                    batch: 8,
                });
            }
        });
    } catch (error) {
        URL.revokeObjectURL(workerUrl);
        return await solvePowSingleThreaded(challenge, onAttempts);
    }
}

async function validatePowAttempt(nonce, counter, difficulty) {
    const payload = `${nonce}:${counter}`;
    const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload));
    const bytes = new Uint8Array(digest);
    return countLeadingZeroBits(bytes) >= difficulty;
}

function countLeadingZeroBits(bytes) {
    let bits = 0;
    for (const byte of bytes) {
        if (byte === 0) {
            bits += 8;
            continue;
        }
        for (let i = 7; i >= 0; i--) {
            if ((byte & (1 << i)) === 0) {
                bits++;
            } else {
                return bits;
            }
        }
    }
    return bits;
}

function renderPowScreen(frame, attempts) {
    renderCenteredScreen([
        styledTitle,
        `${spinnerFrames[frame]} Computing proof of work...`,
        `\x1b[2mHash attempts: ${attempts}\x1b[0m`
    ]);
}

function renderCenteredScreen(lines, restore = false) {
    const startRow = Math.max(1, Math.floor(term.rows / 2) - Math.floor((lines.length - 1) / 2));
    term.write(`${restore ? restoreTerminalState : '\x1b[0m\x1b[?25l'}\x1b[2J`);
    lines.forEach((line, index) => {
        const plain = line.replace(/\x1b\[[0-9;?]*[A-Za-z]/g, '');
        term.write(`\x1b[${startRow + index};${centeredCol(plain)}H${line}`);
    });
    term.write('\x1b[H');
}
