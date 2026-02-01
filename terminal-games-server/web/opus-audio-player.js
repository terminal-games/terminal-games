// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

export class OpusAudioPlayer {
    constructor() {
        this.audioContext = null;
        this.workletNode = null;
        this.decoder = null;
        this.sampleRate = 48000;
        this.channels = 1;
        this.preSkip = 0;
        this.preSkipRemaining = 0;
        this.headersParsed = false;
        this.totalSamplesDecoded = 0;
        this.pendingBuffer = new Uint8Array(0);
    }

    async init() {
        this.audioContext = new AudioContext({ 
            latencyHint: 'interactive', 
            sampleRate: this.sampleRate 
        });
        
        await this.audioContext.audioWorklet.addModule('./jitter-buffer-processor.js');
        
        this.workletNode = new AudioWorkletNode(this.audioContext, 'jitter-buffer-processor', {
            // Always output stereo (mono is duplicated to both channels)
            outputChannelCount: [2]
        });
        this.workletNode.connect(this.audioContext.destination);
        
        if (this.audioContext.state === 'suspended') {
            const resumeAudio = async () => {
                await this.audioContext.resume();
                document.removeEventListener('click', resumeAudio);
                document.removeEventListener('keydown', resumeAudio);
            };
            document.addEventListener('click', resumeAudio);
            document.addEventListener('keydown', resumeAudio);
        }
    }

    initDecoder(opusHead) {
        if (opusHead.length < 19) {
            console.error('Invalid OpusHead');
            return false;
        }
        
        const magic = String.fromCharCode(...opusHead.slice(0, 8));
        if (magic !== 'OpusHead') {
            console.error('Invalid OpusHead magic:', magic);
            return false;
        }
        
        this.channels = opusHead[9];
        this.preSkip = opusHead[10] | (opusHead[11] << 8);
        this.preSkipRemaining = this.preSkip;
        
        // console.log(`Opus: ${this.channels} channels, pre-skip: ${this.preSkip} samples`);
        
        this.decoder = new AudioDecoder({
            output: (audioData) => this.handleDecodedAudio(audioData),
            error: (e) => console.error('Decoder error:', e)
        });
        
        this.decoder.configure({
            codec: 'opus',
            sampleRate: this.sampleRate,
            numberOfChannels: this.channels,
            description: opusHead
        });
        
        return true;
    }

    handleDecodedAudio(audioData) {
        const numFrames = audioData.numberOfFrames;
        const numChannels = audioData.numberOfChannels;
        const format = audioData.format;
        
        let startFrame = 0;
        if (this.preSkipRemaining > 0) {
            startFrame = Math.min(this.preSkipRemaining, numFrames);
            this.preSkipRemaining -= startFrame;
        }
        
        const framesToUse = numFrames - startFrame;
        if (framesToUse <= 0) {
            audioData.close();
            return;
        }
        
        let left, right;
        const isPlanar = format.includes('planar');
        
        if (isPlanar) {
            left = new Float32Array(framesToUse);
            right = new Float32Array(framesToUse);
            audioData.copyTo(left, { planeIndex: 0, frameOffset: startFrame });
            if (numChannels > 1) {
                audioData.copyTo(right, { planeIndex: 1, frameOffset: startFrame });
            } else {
                right.set(left);
            }
        } else {
            const interleaved = new Float32Array(numFrames * numChannels);
            audioData.copyTo(interleaved, { planeIndex: 0 });
            
            left = new Float32Array(framesToUse);
            right = new Float32Array(framesToUse);
            for (let i = 0; i < framesToUse; i++) {
                left[i] = interleaved[(startFrame + i) * numChannels];
                right[i] = numChannels > 1 
                    ? interleaved[(startFrame + i) * numChannels + 1]
                    : left[i];
            }
        }
        
        this.workletNode.port.postMessage({
            type: 'samples',
            left: left,
            right: right
        }, [left.buffer, right.buffer]);
        
        this.totalSamplesDecoded += framesToUse;
        audioData.close();
    }

    parseOggPages(data) {
        const pages = [];
        let offset = 0;
        
        while (offset < data.length - 27) {
            if (data[offset] !== 0x4F || data[offset+1] !== 0x67 || 
                data[offset+2] !== 0x67 || data[offset+3] !== 0x53) {
                offset++;
                continue;
            }
            
            const headerType = data[offset + 5];
            const numSegments = data[offset + 26];
            
            if (offset + 27 + numSegments > data.length) break;
            
            const segmentTable = data.slice(offset + 27, offset + 27 + numSegments);
            let dataSize = 0;
            for (let i = 0; i < numSegments; i++) {
                dataSize += segmentTable[i];
            }
            
            const pageHeaderSize = 27 + numSegments;
            if (offset + pageHeaderSize + dataSize > data.length) break;
            
            const pageData = data.slice(offset + pageHeaderSize, offset + pageHeaderSize + dataSize);
            const packets = this.extractPackets(segmentTable, pageData);
            
            pages.push({ headerType, packets });
            offset += pageHeaderSize + dataSize;
        }
        
        return pages;
    }

    extractPackets(segmentTable, pageData) {
        const packets = [];
        let packetData = [];
        let dataOffset = 0;
        
        for (let i = 0; i < segmentTable.length; i++) {
            const segmentSize = segmentTable[i];
            const segment = pageData.slice(dataOffset, dataOffset + segmentSize);
            packetData.push(segment);
            dataOffset += segmentSize;
            
            if (segmentSize < 255) {
                const totalSize = packetData.reduce((sum, s) => sum + s.length, 0);
                const packet = new Uint8Array(totalSize);
                let off = 0;
                for (const seg of packetData) {
                    packet.set(seg, off);
                    off += seg.length;
                }
                packets.push(packet);
                packetData = [];
            }
        }
        
        return packets;
    }

    async onAudioData(data) {
        if (!this.audioContext) return;
        
        const newData = new Uint8Array(data);
        const combined = new Uint8Array(this.pendingBuffer.length + newData.length);
        combined.set(this.pendingBuffer);
        combined.set(newData, this.pendingBuffer.length);
        this.pendingBuffer = combined;
        
        const pages = this.parseOggPages(this.pendingBuffer);
        
        if (pages.length > 0) {
            this.pendingBuffer = new Uint8Array(0);
        }
        
        for (const page of pages) {
            for (const packet of page.packets) {
                if (!this.headersParsed) {
                    if (!this.decoder) {
                        if (this.initDecoder(packet)) continue;
                    }
                    const magic = String.fromCharCode(...packet.slice(0, 8));
                    if (magic === 'OpusTags') {
                        this.headersParsed = true;
                        continue;
                    }
                }
                
                if (this.decoder && this.decoder.state === 'configured') {
                    try {
                        const chunk = new EncodedAudioChunk({
                            type: 'key',
                            timestamp: this.totalSamplesDecoded * 1000000 / this.sampleRate,
                            data: packet
                        });
                        this.decoder.decode(chunk);
                    } catch (e) {
                        console.warn('Failed to decode chunk:', e);
                    }
                }
            }
        }
    }

    close() {
        if (this.decoder) {
            this.decoder.close();
            this.decoder = null;
        }
        if (this.workletNode) {
            this.workletNode.disconnect();
            this.workletNode = null;
        }
        if (this.audioContext) {
            this.audioContext.close();
            this.audioContext = null;
        }
    }
}
