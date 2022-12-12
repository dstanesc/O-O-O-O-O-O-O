interface BlockStore {
    put: (block: { cid: any; bytes: Uint8Array }) => Promise<void>
    get: (cid: any) => Promise<Uint8Array>
}

interface MemoryBlockStore extends BlockStore {
    push: (otherStore: BlockStore) => Promise<void>
    countReads: () => number
    resetReads: () => void
    size: () => number
}

const memoryBlockStoreFactory = (): MemoryBlockStore => {
    const blocks = {}
    let readCounter = 0
    const put = async (block: {
        cid: any
        bytes: Uint8Array
    }): Promise<void> => {
        blocks[block.cid.toString()] = block.bytes
    }
    const get = async (cid: any): Promise<Uint8Array> => {
        const bytes = blocks[cid.toString()]
        if (!bytes === undefined)
            throw new Error('Block Not found for ' + cid.toString())
        readCounter++
        return bytes
    }

    const push = async (otherStore: BlockStore): Promise<void> => {
        const cids = Object.keys(blocks)
        for (const cid of cids) {
            const bytes = blocks[cid]
            await otherStore.put({ cid, bytes })
        }
    }

    const countReads = () => readCounter

    const resetReads = () => (readCounter = 0)

    const size = () => Object.keys(blocks).length

    return { get, put, push, countReads, resetReads, size }
}

export { BlockStore, MemoryBlockStore, memoryBlockStoreFactory }
