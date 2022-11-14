import { linkCodecFactory, blockCodecFactory, BlockCodec, LinkCodec } from "../codecs"
import { graphStore } from "../graph-store"
import { compute_chunks } from "@dstanesc/wasm-chunking-fastcdc-node"
import { chunkerFactory } from "../chunking"
import { Graph } from "../graph"
import { BlockStore, memoryBlockStoreFactory } from "../block-store"
import { RootStore, emptyRootStore } from "../root-store"
import { OFFSET_INCREMENTS } from "../serde"

import { ProtoGremlinFactory, protoGremlinFactory } from '../api/proto-gremlin-factories'
import { ProtoGremlin, ProtoGremlinTransaction, NavigateVertexWrapper, NavigateEdgeWrapper } from '../api/proto-gremlin'

import * as assert from 'assert'
import { EdgePathElem, navigateVertices, PathElemType, VertexPathElem, RequestBuilder } from "../navigate"
import { Vertex, Prop, Edge } from "../types"
import { eq } from "../ops"

describe('Filter data', function () {

    test("by property, internal api", async () => {

        enum ObjectTypes {
            TWEET = 1
        }

        enum RlshpTypes {
            COMMENT_TO = 1
        }

        enum PropTypes {
            COMMENT = 1,
            WEIGHT = 2
        }

        enum KeyTypes {
            TEXT = 3,
            VALUE = 333
        }

        const { chunk } = chunkerFactory(1024, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const story: RootStore = emptyRootStore()
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })

        const graph = new Graph(story, store)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex(ObjectTypes.TWEET)
        const v2 = tx.addVertex(ObjectTypes.TWEET)
        const v3 = tx.addVertex(ObjectTypes.TWEET)

        const e1 = await tx.addEdge(v1, v2, RlshpTypes.COMMENT_TO)
        const e2 = await tx.addEdge(v1, v3, RlshpTypes.COMMENT_TO)

        const p1 = await tx.addEdgeProp(e1, KeyTypes.VALUE, 55, PropTypes.WEIGHT)
        const p2 = await tx.addEdgeProp(e2, KeyTypes.VALUE, 33, PropTypes.WEIGHT)

        await tx.addVertexProp(v2, KeyTypes.TEXT, "hello world from v2", PropTypes.COMMENT)
        await tx.addVertexProp(v3, KeyTypes.TEXT, "hello world from v3", PropTypes.COMMENT)

        const { root } = await tx.commit()

        const request = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .add(PathElemType.EDGE)
            .add(PathElemType.VERTEX)
            .propPred(KeyTypes.TEXT, eq("hello world from v3"))
            .maxResults(10)
            .get()

        const vr = []
        for await (const result of navigateVertices(graph, [v1.offset], request)) {
            vr.push(result)
        }

        assert.equal(vr.length, 1)
        assert.equal(vr[0].offset, 50)
    })

    test("by property, proto-gremlin api, json object property", async () => {

        enum ObjectTypes {
            TWEET = 1
        }

        enum RlshpTypes {
            COMMENT_TO = 1
        }

        enum PropTypes {
            COMMENT = 1,
            WEIGHT = 2
        }

        enum KeyTypes {
            TEXT = 3,
            VALUE = 333
        }

        const { chunk } = chunkerFactory(1024, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const rootStore: RootStore = emptyRootStore()

        const g: ProtoGremlin = protoGremlinFactory({ chunk, linkCodec, blockCodec, blockStore, rootStore }).g()

        const tx = await g.tx()

        const v1 = await tx.addV(ObjectTypes.TWEET).next()
        const v2 = await tx.addV(ObjectTypes.TWEET).property(KeyTypes.VALUE, { hello: "v2" }, PropTypes.COMMENT).next()
        const v3 = await tx.addV(ObjectTypes.TWEET).property(KeyTypes.VALUE, { hello: "v3" }, PropTypes.COMMENT).next()

        const e1 = await tx.addE(RlshpTypes.COMMENT_TO).from(v1).to(v2).next()
        const e2 = await tx.addE(RlshpTypes.COMMENT_TO).from(v1).to(v3).next()

        await tx.commit()


        const vr = []
        for await (const result of g.V([v1.offset]).out().has(ObjectTypes.TWEET, { keyType: KeyTypes.VALUE, operation: eq("v3", v => v.hello) }).exec()) {
            vr.push(result)
        }
        vr.forEach(r => console.log(r))

        assert.equal(vr.length, 1)
        assert.equal(vr[0].offset, 50)
    })

    test("by property, proto-gremlin api, text property", async () => {

        enum ObjectTypes {
            TWEET = 1
        }

        enum RlshpTypes {
            COMMENT_TO = 1
        }

        enum PropTypes {
            COMMENT = 1,
            WEIGHT = 2
        }

        enum KeyTypes {
            TEXT = 3,
            VALUE = 333
        }

        const { chunk } = chunkerFactory(1024, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const rootStore: RootStore = emptyRootStore()

        const g: ProtoGremlin = protoGremlinFactory({ chunk, linkCodec, blockCodec, blockStore, rootStore }).g()

        const tx = await g.tx()

        const v1 = await tx.addV(ObjectTypes.TWEET).next()
        const v2 = await tx.addV(ObjectTypes.TWEET).property(KeyTypes.VALUE, "hello world from v2", PropTypes.COMMENT).next()
        const v3 = await tx.addV(ObjectTypes.TWEET).property(KeyTypes.VALUE, "hello world from v3", PropTypes.COMMENT).next()

        const e1 = await tx.addE(RlshpTypes.COMMENT_TO).from(v1).to(v2).next()
        const e2 = await tx.addE(RlshpTypes.COMMENT_TO).from(v1).to(v3).next()

        await tx.commit()

        const vr = []
        for await (const result of g.V([v1.offset]).out().has(ObjectTypes.TWEET, { keyType: KeyTypes.VALUE, operation: eq("hello world from v3") }).exec()) {
            vr.push(result)
        }

        assert.equal(vr.length, 1)
        assert.equal(vr[0].offset, 50)
    })


})