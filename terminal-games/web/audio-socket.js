export const MIN_BUFFER_MS = 50;
export const MAX_BUFFER_MS = 1000;

export function connectAudioSocket(sessionId, audioPlayer) {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const audioWsUrl = `${protocol}//${window.location.host}/ws/audio?session=${sessionId}`;
    const audioSocket = new WebSocket(audioWsUrl);
    audioSocket.binaryType = 'arraybuffer';
    
    let smoothedPeakLatency = 0;
    let lastTargetBufferMs = 0;
    
    const measureLatency = () => {
        if (audioSocket.readyState === WebSocket.OPEN) {
            const timestamp = performance.now();
            audioSocket.send(`ping:${timestamp}`);
        }
    };
    
    audioSocket.onmessage = (event) => {
        if (typeof event.data === 'string') {
            if (event.data.startsWith('pong:')) {
                const pingTimestamp = parseFloat(event.data.substring(5));
                const rtt = performance.now() - pingTimestamp;
                
                const decay = Math.max(0, (smoothedPeakLatency - rtt) * 0.10);
                smoothedPeakLatency = Math.max(rtt, smoothedPeakLatency - decay);
                
                const targetBufferMs = Math.max(MIN_BUFFER_MS, Math.min(MAX_BUFFER_MS, smoothedPeakLatency * 1.2));
                const deltaMs = targetBufferMs - lastTargetBufferMs;
                lastTargetBufferMs = targetBufferMs;
                
                console.log("rtt", rtt, "smoothedPeakLatency", smoothedPeakLatency, "deltaMs", deltaMs);
                
                if (audioPlayer.workletNode && deltaMs !== 0) {
                    audioPlayer.workletNode.port.postMessage({
                        type: 'adjustBufferMs',
                        deltaMs: deltaMs
                    });
                }
                
                return;
            }
        }
        
        if (event.data instanceof ArrayBuffer) {
            audioPlayer.onAudioData(event.data);
        }
    };

    let pingInterval = null;

    audioSocket.onopen = () => {
        pingInterval = setInterval(measureLatency, 5000);
        measureLatency();
    };
    
    audioSocket.onerror = (error) => {
        console.error('Audio WebSocket error:', error);
    };
    
    audioSocket.onclose = () => {
        console.log('Audio WebSocket closed');
        if (pingInterval) {
            clearInterval(pingInterval);
            pingInterval = null;
        }
    };

    return {
        close: () => {
            if (pingInterval) {
                clearInterval(pingInterval);
                pingInterval = null;
            }
            if (audioSocket) {
                audioSocket.close();
            }
        }
    };
}
