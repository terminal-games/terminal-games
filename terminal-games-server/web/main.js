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
let queuePosition = null;
let spinnerInterval = null;
let spinnerFrame = 0;

const measureLatency = () => {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(`ping:${performance.now()}`);
    }
};

const onSocketMessage = async (event) => {
    if (typeof event.data === 'string') {
        if (event.data.startsWith('queue:')) {
            if (event.data === 'queue:allowed') {
                queuePosition = null;
                stopQueueSpinner();
                term.write('\x1b[2J\x1b[H');
            } else if (event.data.startsWith('queue:queued:')) {
                const position = Number.parseInt(event.data.substring('queue:queued:'.length), 10);
                if (Number.isFinite(position) && position >= 1) {
                    queuePosition = position;
                    startQueueSpinner();
                    renderQueueScreen();
                }
            }
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
    term.write('\r\n\x1b[31mWebSocket connection error\x1b[0m\r\n');
};

const onSocketClose = () => {
    stopQueueSpinner();
    term.write('\r\n\x1b[31mWebSocket connection closed\x1b[0m\r\n');
    if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
    }
    audioPlayer.close();
};

await startWebSocketSession();

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
});

window.addEventListener('resize', () => fitAddon.fit());

function centeredCol(text) {
    return Math.max(1, Math.floor((term.cols - text.length) / 2) + 1);
}

function renderQueueScreen() {
    if (queuePosition === null) {
        return;
    }
    const title = 'Terminal Games';
    const spinnerLine = `${spinnerFrames[spinnerFrame]} Loading...`;
    const queueLine = `Position in queue: ${queuePosition}`;
    const centerRow = Math.max(1, Math.floor(term.rows / 2));
    const titleRow = Math.max(1, centerRow - 1);
    const queueRow = Math.min(term.rows, centerRow + 1);

    term.write('\x1b[?25l\x1b[2J');
    term.write(`\x1b[${titleRow};${centeredCol(title)}H${title}`);
    term.write(`\x1b[${centerRow};${centeredCol(spinnerLine)}H${spinnerLine}`);
    term.write(`\x1b[${queueRow};${centeredCol(queueLine)}H${queueLine}`);
    term.write('\x1b[H');
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
        throw new Error('failed to fetch POW challenge');
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
    const title = 'Terminal Games';
    const spinnerLine = `${spinnerFrames[frame]} Computing proof of work...`;
    const detailLine = `Hash attempts: ${attempts}`;
    const centerRow = Math.max(1, Math.floor(term.rows / 2));
    const titleRow = Math.max(1, centerRow - 1);
    const detailRow = Math.min(term.rows, centerRow + 1);

    term.write('\x1b[?25l\x1b[2J');
    term.write(`\x1b[${titleRow};${centeredCol(title)}H${title}`);
    term.write(`\x1b[${centerRow};${centeredCol(spinnerLine)}H${spinnerLine}`);
    term.write(`\x1b[${detailRow};${centeredCol(detailLine)}H\x1b[2m${detailLine}\x1b[0m`);
    term.write('\x1b[H');
}
