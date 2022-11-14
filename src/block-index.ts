import { RootDecoder } from "./serde"
import { Link, RootIndex } from "./types"
import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { LinkCodec } from "./codecs"
import { BlockStore } from "./block-store"


interface BlockIndexFactory {
    buildRootIndex: (root: Link) => Promise<{ root: Link, index: RootIndex }>,
    buildChunkyIndex: (root: Link) => Promise<{ indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }>
}


const blockIndexFactory = ({ linkCodec, blockStore }: {
    linkCodec: LinkCodec,
    blockStore: BlockStore
}): BlockIndexFactory => {

    const { encode: linkEncode, decode: linkDecode }: LinkCodec = linkCodec
    const { put: blockPut, get: blockGet } = blockStore
    const { readIndex } = chunkyStore()
    
    const buildChunkyIndex = async (root: Link): Promise<{ indexStruct: { startOffsets: Map<number, any>, indexSize: number, byteArraySize: number }, indexBuffer: Uint8Array }> => {
        return await readIndex(root, blockGet, linkDecode)
    }

    const buildRootIndex = async (root: Link): Promise<{ root: Link, index: RootIndex }> => {
        const bytes = await blockGet(root)
        const rootStruct = new RootDecoder(bytes, linkDecode).read()
        const vertexIndex = await buildChunkyIndex(rootStruct.vertexRoot)
        const edgeIndex = await buildChunkyIndex(rootStruct.edgeRoot)
        const propIndex = await buildChunkyIndex(rootStruct.propRoot)
        const indexIndex = await buildChunkyIndex(rootStruct.indexRoot)
        const index = Object.assign({ vertexIndex, edgeIndex, propIndex, indexIndex }, rootStruct)
        return { root, index }
    }

    return { buildRootIndex, buildChunkyIndex }
}

export { blockIndexFactory, BlockIndexFactory }