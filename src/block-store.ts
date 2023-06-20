interface BlockStore {
    put: (block: { cid: any; bytes: Uint8Array }) => Promise<void>
    get: (cid: any) => Promise<Uint8Array>
}

interface MemoryBlockStore extends BlockStore {
    content: () => Set<string>
    diff: (otherStore: BlockStore) => {
        missingLocal: Set<string>
        missingOther: Set<string>
        intersection: Set<string>
    }
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

    const diff = (
        otherStore: MemoryBlockStore
    ): {
        missingLocal: Set<string>
        missingOther: Set<string>
        intersection: Set<string>
    } => {
        const missingLocal = new Set<string>()
        const missingOther = new Set<string>()
        const intersection = new Set<string>()
        const localCids = content()
        const otherCids = otherStore.content()
        for (const cid of localCids) {
            if (otherCids.has(cid)) {
                intersection.add(cid)
            } else {
                missingOther.add(cid)
            }
        }
        for (const cid of otherCids) {
            if (!localCids.has(cid)) {
                missingLocal.add(cid)
            }
        }
        return { missingLocal, missingOther, intersection }
    }

    const content = (): Set<string> => {
        const out = new Set<string>()
        for (const cid of Object.keys(blocks)) {
            out.add(cid.toString())
        }
        return out
    }

    const countReads = () => readCounter

    const resetReads = () => (readCounter = 0)

    const size = () => Object.keys(blocks).length

    return { get, put, push, countReads, resetReads, size, diff, content }
}

export { BlockStore, MemoryBlockStore, memoryBlockStoreFactory }
