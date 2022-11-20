import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { RootStore, emptyRootStore } from '../root-store'
import { graphStore } from '../graph-store'
import { Graph } from '../graph'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import {
    BlockCodec,
    blockCodecFactory,
    LinkCodec,
    linkCodecFactory,
} from '../codecs'
import { Edge, Prop, Status, Vertex } from '../types'
import { deltaFactory } from '../delta'

const { chunk } = chunkerFactory(512, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const blockCodec: BlockCodec = blockCodecFactory()
const blockStore: BlockStore = memoryBlockStoreFactory()

import * as assert from 'assert'

describe('Multi tx', function () {
    test('w/ baseline changes, single graph shares state', async () => {
        const story: RootStore = emptyRootStore()
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })

        // baseline
        const graph = new Graph(story, store)
        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex()
        const v2 = tx.addVertex()
        const v3 = tx.addVertex()

        await tx.addEdge(v1, v2)
        await tx.addEdge(v1, v3)

        await tx.addVertexProp(v2, 1, { hello: 'v2' })
        await tx.addVertexProp(v2, 1, { hello: 'v3' })

        const { root: baseRoot, index: baseIndex } = await tx.commit()

        // next
        const tx2 = graph.tx()

        await tx2.start()

        const v4 = tx2.addVertex()

        await tx2.addEdge(v1, v4)

        const { root: currentRoot, index: currentIndex } = await tx2.commit()

        const { baselineDelta } = deltaFactory({ linkCodec, blockCodec })

        const { vertices, edges } = await baselineDelta({
            baseRoot,
            baseIndex,
            baseStore: blockStore,
            currentRoot,
            currentIndex,
            currentStore: blockStore,
        })

        assert.strictEqual(vertices.added.size, 1)
        assert.strictEqual(vertices.updated.size, 0)
        assert.strictEqual(vertices.added.get(75).status, 1)

        assert.strictEqual(edges.added.size, 1)
        assert.strictEqual(edges.updated.size, 1)
        assert.strictEqual((edges.added.get(90) as Edge).sourcePrev, 45)
        assert.strictEqual((edges.updated.get(45) as Edge).sourcePrev, 0)
        assert.strictEqual((edges.updated.get(45) as Edge).sourceNext, 90)
    })

    test('w/ baseline changes, no state shared, second graph reads initial changes via cid', async () => {
        const story: RootStore = emptyRootStore()
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })

        // baseline
        const graph = new Graph(story, store)
        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex()
        const v2 = tx.addVertex()
        const v3 = tx.addVertex()

        await tx.addEdge(v1, v2)
        await tx.addEdge(v1, v3)

        await tx.addVertexProp(v2, 1, { hello: 'v2' })
        await tx.addVertexProp(v2, 1, { hello: 'v3' })

        const { root: baseRoot, index: baseIndex } = await tx.commit()

        // next
        const graph2 = new Graph(story, store)
        const tx2 = graph2.tx()
        await tx2.start()
        const v4 = tx2.addVertex()
        const v1p = await tx2.getVertex(v1.offset)
        await tx2.addEdge(v1p, v4)
        const { root: currentRoot, index: currentIndex } = await tx2.commit()
        const { baselineDelta } = deltaFactory({ linkCodec, blockCodec })
        const { vertices, edges } = await baselineDelta({
            baseRoot,
            baseIndex,
            baseStore: blockStore,
            currentRoot,
            currentIndex,
            currentStore: blockStore,
        })

        assert.strictEqual(vertices.added.size, 1)
        assert.strictEqual(vertices.updated.size, 0)
        assert.strictEqual(vertices.added.get(75).status, 1)

        assert.strictEqual(edges.added.size, 1)
        assert.strictEqual(edges.updated.size, 1)
        assert.strictEqual((edges.added.get(90) as Edge).sourcePrev, 45)
        assert.strictEqual((edges.updated.get(45) as Edge).sourcePrev, 0)
        assert.strictEqual((edges.updated.get(45) as Edge).sourceNext, 90)
    })
})
