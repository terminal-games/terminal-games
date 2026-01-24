class JitterBufferProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this.sampleRate = 48000;
        this.buffer = new Float32Array(this.sampleRate);
        this.writePos = 0;
        this.readPos = 0;
        this.bufferedSamples = 0;
        this.targetBufferMs = 0;
        this.started = false;
        
        this.port.onmessage = (e) => {
            if (e.data.type === 'samples') {
                this.pushSamples(e.data.left, e.data.right);
            } else if (e.data.type === 'adjustBufferMs') {
                this.targetBufferMs += e.data.deltaMs;
                console.log("adjustBufferMs message targetBufferMs", this.targetBufferMs)
            }
        };
    }
    
    msToSamples(ms) {
        return Math.floor((ms / 1000) * this.sampleRate * 2);
    }
    
    pushSamples(left, right) {
        const samplesToWrite = left.length * 2;
        
        // Drop excess data if buffer gets too full
        const maxBuffer = this.msToSamples(this.targetBufferMs * 2);
        if (this.bufferedSamples + samplesToWrite > maxBuffer) {
            const targetLevel = this.msToSamples(this.targetBufferMs);
            const toDrop = this.bufferedSamples - targetLevel;
            if (toDrop > 0) {
                this.readPos = (this.readPos + toDrop) % this.buffer.length;
                this.bufferedSamples = targetLevel;
            }
        }
        
        for (let i = 0; i < left.length; i++) {
            this.buffer[this.writePos] = left[i];
            this.writePos = (this.writePos + 1) % this.buffer.length;
            this.buffer[this.writePos] = right[i];
            this.writePos = (this.writePos + 1) % this.buffer.length;
        }
        this.bufferedSamples += samplesToWrite;
    }
    
    process(inputs, outputs, parameters) {
        const output = outputs[0];
        const left = output[0];
        const right = output[1];
        const framesToRead = left.length;
        const samplesToRead = framesToRead * 2;
        
        // Wait for initial buffer fill before starting
        if (!this.started) {
            if (this.bufferedSamples >= this.msToSamples(this.targetBufferMs)) {
                this.started = true;
            } else {
                left.fill(0);
                right.fill(0);
                return true;
            }
        }
        
        if (this.bufferedSamples < samplesToRead) {
            left.fill(0);
            right.fill(0);
            this.started = false;
            this.targetBufferMs += 10;
            console.warn("Underrun, increasing targetBufferMs to", this.targetBufferMs)
            return true;
        }
        
        for (let i = 0; i < framesToRead; i++) {
            left[i] = this.buffer[this.readPos];
            this.readPos = (this.readPos + 1) % this.buffer.length;
            right[i] = this.buffer[this.readPos];
            this.readPos = (this.readPos + 1) % this.buffer.length;
        }
        this.bufferedSamples -= samplesToRead;
        
        return true;
    }
}

registerProcessor('jitter-buffer-processor', JitterBufferProcessor);
