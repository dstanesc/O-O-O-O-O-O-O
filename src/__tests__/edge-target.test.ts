import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import {
    LinkCodec,
    linkCodecFactory,
    BlockCodec,
    blockCodecFactory,
    multiBlockCodecFactory,
} from '../codecs'
import { Graph } from '../graph'
import { graphStore } from '../graph-store'

const { chunk } = chunkerFactory(1024, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const blockCodec: BlockCodec = blockCodecFactory()
const multiBlockCodec: BlockCodec = multiBlockCodecFactory(chunk)
import * as assert from 'assert'
import { Edge, EdgeRef } from '../types'
import { VersionStore, versionStoreFactory } from '../version-store'

describe('Edge, fields computed on commit', function () {
    test('targetNext and targetPrev are properly set', async () => {
        enum ObjectTypes {
            TWEET = 1,
        }
        enum RlshpTypes {
            COMMENT_TO = 1,
        }

        enum PropTypes {
            COMMENT = 1,
            WEIGHT = 2,
        }

        enum KeyTypes {
            JSON = 1,
            TEXT = 3,
            VALUE = 33,
        }

        const blockStore: BlockStore = memoryBlockStoreFactory()
        const story: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })
        const graph = new Graph(story, store)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex(ObjectTypes.TWEET)

        // second level
        const v2 = tx.addVertex(ObjectTypes.TWEET)
        const v3 = tx.addVertex(ObjectTypes.TWEET)

        //third level
        const v4 = tx.addVertex(ObjectTypes.TWEET)
        const v5 = tx.addVertex(ObjectTypes.TWEET)
        const v6 = tx.addVertex(ObjectTypes.TWEET)
        const v7 = tx.addVertex(ObjectTypes.TWEET)

        // fourth level
        const v8 = tx.addVertex(ObjectTypes.TWEET)
        const v9 = tx.addVertex(ObjectTypes.TWEET)
        const v10 = tx.addVertex(ObjectTypes.TWEET)
        const v11 = tx.addVertex(ObjectTypes.TWEET)
        const v12 = tx.addVertex(ObjectTypes.TWEET)
        const v13 = tx.addVertex(ObjectTypes.TWEET)
        const v14 = tx.addVertex(ObjectTypes.TWEET)
        const v15 = tx.addVertex(ObjectTypes.TWEET)

        const e1 = await tx.addEdge(v1, v2, RlshpTypes.COMMENT_TO)
        const e2 = await tx.addEdge(v1, v3, RlshpTypes.COMMENT_TO)

        const e3 = await tx.addEdge(v2, v4, RlshpTypes.COMMENT_TO)
        const e4 = await tx.addEdge(v2, v5, RlshpTypes.COMMENT_TO)

        const e5 = await tx.addEdge(v3, v6, RlshpTypes.COMMENT_TO)
        const e6 = await tx.addEdge(v3, v7, RlshpTypes.COMMENT_TO)

        const e7 = await tx.addEdge(v4, v8, RlshpTypes.COMMENT_TO)
        const e8 = await tx.addEdge(v4, v9, RlshpTypes.COMMENT_TO)

        await tx.addEdge(v5, v10, RlshpTypes.COMMENT_TO)
        await tx.addEdge(v5, v11, RlshpTypes.COMMENT_TO)

        const { root, index } = await tx.commit({})

        console.log(`e7.targetPrev=${e7.targetPrev}`)
        console.log(`e8.targetPrev=${e8.targetPrev}`)
        console.log(`e3.targetNext=${e3.targetNext}`)

        console.log(`e3.targetPrev=${e3.targetPrev}`)
        console.log(`e1.targetNext=${e1.targetNext}`)

        assert.strictEqual(e7.targetPrev, e3.offset)
        assert.strictEqual(e3.targetNext, e7.offset)

        assert.strictEqual(e3.targetPrev, e1.offset)
        assert.strictEqual(e1.targetNext, e3.offset)

        assert.strictEqual(e1.targetPrev, undefined)

        // all roads lead to Rome, navigating targetPrev from any edge should lead to root

        const topEdge1 = await toRoot(graph, e7)
        const rootVertex1 = await graph.getVertex(topEdge1.source)
        console.log(rootVertex1)

        const topEdge2 = await toRoot(graph, e8)
        const rootVertex2 = await graph.getVertex(topEdge2.source)
        console.log(rootVertex2)

        const topEdge3 = await toRoot(graph, e4)
        const rootVertex3 = await graph.getVertex(topEdge3.source)

        const topEdge4 = await toRoot(graph, e5)
        const rootVertex4 = await graph.getVertex(topEdge4.source)

        assert.strictEqual(rootVertex1.offset, rootVertex2.offset)
        assert.strictEqual(rootVertex2.offset, rootVertex3.offset)
        assert.strictEqual(rootVertex3.offset, rootVertex4.offset)
    })
})

async function toRoot(graph: Graph, edge: Edge): Promise<Edge> {
    if (edge.targetPrev !== undefined) {
        const targetPrev = await graph.getEdge(edge.targetPrev)
        return toRoot(graph, targetPrev)
    } else return edge
}
