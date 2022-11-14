import { LinkCodec, BlockCodec } from "../codecs"
import { graphStore } from "../graph-store"
import { BlockStore } from "../block-store"
import { RootStore } from "../root-store"
import { ProtoGremlin } from './proto-gremlin'
import { Graph } from "../graph"
import { IndexStore } from "../index-store"

interface ProtoGremlinFactory {
    g: () => ProtoGremlin
}


const protoGremlinFactory = ({ chunk, linkCodec, blockCodec, blockStore, rootStore, indexStore }: {
    chunk: (buffer: Uint8Array) => Uint32Array,
    linkCodec: LinkCodec,
    blockCodec: BlockCodec,
    blockStore: BlockStore,
    rootStore: RootStore,
    indexStore?: IndexStore
}): ProtoGremlinFactory => {

    const g = (): ProtoGremlin => {
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })
        const graph = new Graph(rootStore, store, indexStore)
        return new ProtoGremlin(graph)
    }
    return { g }
}


export { ProtoGremlinFactory, protoGremlinFactory }