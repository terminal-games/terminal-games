import { OpusAudioPlayer } from './opus-audio-player.js';
import { connectAudioSocket } from './audio-socket.js';

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

let audioSocketHandle = null;

const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const queryString = window.location.search;
const wsUrl = `${protocol}//${window.location.host}/ws${queryString}`;
const socket = new WebSocket(wsUrl);

socket.binaryType = 'arraybuffer';

socket.onopen = () => {
    const cols = term.cols;
    const rows = term.rows;
    socket.send(`resize:${cols}:${rows}`);
};

socket.onmessage = async (event) => {
    if (typeof event.data === 'string') {
        if (event.data.startsWith('session:')) {
            const sessionId = event.data.substring(8);
            audioSocketHandle = connectAudioSocket(sessionId, audioPlayer);
        }
        return;
    }
    
    if (event.data instanceof ArrayBuffer) {
        try {
            const decompressionStream = new DecompressionStream('deflate-raw');
            const stream = new ReadableStream({
                start(controller) {
                    controller.enqueue(new Uint8Array(event.data));
                    controller.close();
                }
            });
            
            const decompressedStream = stream.pipeThrough(decompressionStream);
            const reader = decompressedStream.getReader();
            
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                term.write(value);
            }
        } catch (error) {
            console.error('Decompression error:', error);
            const bytes = new Uint8Array(event.data);
            term.write(bytes);
        }
    }
};

socket.onerror = (error) => {
    console.error('WebSocket error:', error);
    term.write('\r\n\x1b[31mWebSocket connection error\x1b[0m\r\n');
};

socket.onclose = () => {
    term.write('\r\n\x1b[31mWebSocket connection closed\x1b[0m\r\n');
    audioPlayer.close();
    if (audioSocketHandle) {
        audioSocketHandle.close();
    }
};

term.onData((data) => {
    if (socket.readyState === WebSocket.OPEN) {
        const encoder = new TextEncoder();
        const bytes = encoder.encode(data);
        socket.send(bytes.buffer);
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

window.addEventListener('resize', () => {
    fitAddon.fit();
});
