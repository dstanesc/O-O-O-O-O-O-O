import { create, load } from 'prolly-trees/map'
import * as Block from 'multiformats/block'
import * as codec from '@ipld/dag-cbor'
import { sha256 as hasher } from 'multiformats/hashes/sha2'
import { bf, simpleCompare as compare } from 'prolly-trees/utils'
import { nocache, global as globalCache } from 'prolly-trees/cache'

const chunker = bf(12)
const cache = globalCache
const opts = { cache, chunker, codec, hasher }

const indexStoreFactory = (blockStore) => {
    const { put, get } = blockStore
    const indexCreate = async (values) => {
        const list = values.map((elem) => ({
            key: elem.value,
            value: elem.ref,
        }))
        let root
        for await (const node of create({ get, compare, list, ...opts })) {
            const address = await node.address
            const block = await node.block
            await put(block)
            root = address
        }
        return root
    }

    const getDecoded = async (link) => {
        const bytes = await get(link)
        return await Block.decode({ bytes, codec, hasher })
    }

    const indexSearch = async (link, value) => {
        const indexRoot = await load({
            cid: link,
            get: getDecoded,
            compare,
            ...opts,
        })
        const { result } = await indexRoot.get(value)
        return { value, ref: result }
    }
    return { indexCreate, indexSearch }
}

export { indexStoreFactory }
