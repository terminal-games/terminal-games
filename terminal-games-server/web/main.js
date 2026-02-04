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
const queryString = window.location.search;
const wsUrl = `${protocol}//${window.location.host}/ws${queryString}`;
const socket = new WebSocket(wsUrl);

socket.binaryType = 'arraybuffer';

let smoothedPeakLatency = 0;
let lastTargetBufferMs = 0;
let pingInterval = null;

const measureLatency = () => {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(`ping:${performance.now()}`);
    }
};

socket.onopen = () => {
    socket.send(`resize:${term.cols}:${term.rows}`);
    pingInterval = setInterval(measureLatency, 5000);
    measureLatency();
};

socket.onmessage = async (event) => {
    if (typeof event.data === 'string') {
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

socket.onerror = (error) => {
    console.error('WebSocket error:', error);
    term.write('\r\n\x1b[31mWebSocket connection error\x1b[0m\r\n');
};

socket.onclose = () => {
    term.write('\r\n\x1b[31mWebSocket connection closed\x1b[0m\r\n');
    if (pingInterval) {
        clearInterval(pingInterval);
        pingInterval = null;
    }
    audioPlayer.close();
};

term.onData((data) => {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(new TextEncoder().encode(data).buffer);
    }
});

term.onBinary((data) => {
    if (socket.readyState === WebSocket.OPEN) {
        const bytes = new Uint8Array(data.length);
        for (let i = 0; i < data.length; i++) {
            bytes[i] = data.charCodeAt(i) & 0xFF;
        }
        socket.send(bytes.buffer);
    }
});

term.onResize((size) => {
    if (socket.readyState === WebSocket.OPEN) {
        socket.send(`resize:${size.cols}:${size.rows}`);
    }
});

window.addEventListener('resize', () => fitAddon.fit());
