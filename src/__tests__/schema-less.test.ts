import { linkCodecFactory, blockCodecFactory, BlockCodec, LinkCodec } from "../codecs"
import { graphStore } from "../graph-store"
import { compute_chunks } from "@dstanesc/wasm-chunking-fastcdc-node"
import { chunkerFactory } from "../chunking"
import { Graph } from "../graph"
import { BlockStore, memoryBlockStoreFactory } from "../block-store"
import { RootStore, emptyRootStore } from "../root-store"

import { protoGremlinFactory } from '../api/proto-gremlin-factories'
import { ProtoGremlin } from '../api/proto-gremlin'

import * as assert from 'assert'
import { navigateVertices, PathElemType, RequestBuilder } from "../navigate"
import { Status } from "../types"


describe('Minimal, schema-less creation and navigation deterministic root', function () {

    test("internal api creation", async () => {

        const { chunk } = chunkerFactory(1024, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const story: RootStore = emptyRootStore()
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })
        const graph = new Graph(story, store)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex()
        const v2 = tx.addVertex()
        const v3 = tx.addVertex()

        await tx.addEdge(v1, v2)
        await tx.addEdge(v1, v3)

        await tx.addVertexProp(v2, 1, { hello: "v2" })
        await tx.addVertexProp(v2, 1, { hello: "v3" })

        const { root } = await tx.commit()

        assert.equal('bafkreia6mi6w4iukhulm7qldemwiiep4gyp3pkpdngkkqvmdbvlcvd35ra', root.toString())
    })

    test("proto-gremlin api creation", async () => {

        const { chunk } = chunkerFactory(1024, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const rootStore: RootStore = emptyRootStore()

        const g: ProtoGremlin = protoGremlinFactory({ chunk, linkCodec, blockCodec, blockStore, rootStore }).g()

        const tx = await g.tx()

        const v1 = await tx.addV().next()
        const v2 = await tx.addV().property(1, { hello: "v2" }).property(1, { hello: "v3" }).next()
        const v3 = await tx.addV().next()

        const e1 = await tx.addE().from(v1).to(v2).next()
        const e2 = await tx.addE().from(v1).to(v3).next()

        const { root } = await tx.commit()

        assert.equal('bafkreia6mi6w4iukhulm7qldemwiiep4gyp3pkpdngkkqvmdbvlcvd35ra', root.toString())
    })

    test("internal api navigate", async () => {

        const { chunk } = chunkerFactory(1024, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const story: RootStore = emptyRootStore()
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })
        const graph = new Graph(story, store)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex()
        const v2 = tx.addVertex()
        const v3 = tx.addVertex()

        const e1 = await tx.addEdge(v1, v2)
        const e2 =await tx.addEdge(v1, v3)

        const p1 = await tx.addVertexProp(v2, 1, { hello: "v2" })
        const p2 = await tx.addVertexProp(v2, 1, { hello: "v3" })

        await tx.commit()

        const path = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .add(PathElemType.EDGE)
            .add(PathElemType.VERTEX)
            .maxResults(10)
            .get()

        const vr = []
        for await (const result of navigateVertices(graph, [v1.offset], path)) {
            vr.push(result)
        }

        assert.equal(vr.length, 2)
        assert.equal(vr[0].offset, 25)
        assert.equal(vr[1].offset, 50)
        assert.equal(vr[0].status, Status.CREATED)
        assert.equal(vr[1].status, Status.CREATED)

        assert.equal(p1.status, Status.CREATED)
        assert.equal(p2.status, Status.CREATED)
        assert.equal(e1.status, Status.CREATED)
        assert.equal(e2.status, Status.CREATED)
    })

    test("proto-gremlin api navigate", async () => {

        const { chunk } = chunkerFactory(1024, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const rootStore: RootStore = emptyRootStore()

        const g: ProtoGremlin = protoGremlinFactory({ chunk, linkCodec, blockCodec, blockStore, rootStore }).g()

        const tx = await g.tx()

        const v1 = await tx.addV().next()
        const v2 = await tx.addV().property(1, { hello: "v2" }).property(1, { hello: "v3" }).next()
        const v3 = await tx.addV().next()

        const e1 = await tx.addE().from(v1).to(v2).next()
        const e2 = await tx.addE().from(v1).to(v3).next()

        await tx.commit()

        const vr = []
        for await (const result of g.V([v1.offset]).out().exec()) {
            vr.push(result)
        }

        assert.equal(vr.length, 2)
        assert.equal(vr[0].offset, 25)
        assert.equal(vr[1].offset, 50)
        assert.equal(vr[0].status, Status.CREATED)
        assert.equal(vr[1].status, Status.CREATED)
    })

})
