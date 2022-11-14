
interface Chunker {
    chunk: (buffer: Uint8Array) => Uint32Array
}

const chunkerFactory = (targetSize: number, compute_chunks_provider: (source: Uint8Array, min_size: number, avg_size: number, max_size: number) => Uint32Array): Chunker => {
    const sizeRange = (avg: number): { minSize: number, avgSize: number, maxSize: number } => {
        return {
            minSize: Math.floor(avg / 2),
            avgSize: avg,
            maxSize: avg * 2,
        };
    }
    const chunk = (buffer: Uint8Array): Uint32Array => {
        const { minSize, avgSize, maxSize } = sizeRange(targetSize)
        return compute_chunks_provider(buffer, minSize, avgSize, maxSize).subarray(1)
    }
    return { chunk }
}

export { chunkerFactory }