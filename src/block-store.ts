
interface BlockStore {
    put: (block: { cid: any, bytes: Uint8Array }) => Promise<void>
    get: (cid: any) => Promise<Uint8Array>
}

interface MemoryBlockStore extends BlockStore {
    countReads: () => number
    resetReads: () => void
    size: () => number
}

const memoryBlockStoreFactory = (): MemoryBlockStore => {
    const blocks = {}
    let readCounter = 0
    const put = async (block: { cid: any, bytes: Uint8Array }): Promise<void> => {
        //console.log(`Storing block ${block.cid.toString()}`)
        blocks[block.cid.toString()] = block.bytes
    }
    const get = async (cid: any): Promise<Uint8Array> => {
        //console.log(`Getting block ${cid.toString()}`)
        const bytes = blocks[cid.toString()]
        if (!bytes === undefined) throw new Error('Block Not found for ' + cid.toString())
        readCounter++
        return bytes
    }

    const countReads = () => readCounter

    const resetReads = () => readCounter = 0

    const size = () => Object.keys(blocks).length

    return { get, put, countReads, resetReads, size }
}

export { BlockStore, MemoryBlockStore, memoryBlockStoreFactory }