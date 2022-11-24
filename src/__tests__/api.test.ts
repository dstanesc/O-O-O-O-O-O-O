import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import {
    BlockCodec,
    LinkCodec,
    linkCodecFactory,
    blockCodecFactory,
    multiBlockCodecFactory,
} from '../codecs'
import { graphStore } from '../graph-store'
import { chunkerFactory } from '../chunking'
import { Graph } from '../graph'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import { RequestBuilder, navigateVertices, PathElemType } from '../navigate'
import { OFFSET_INCREMENTS } from '../serde'
import * as assert from 'assert'

import {
    protoGremlinFactory,
    ProtoGremlin,
    ProtoGremlinFactory,
} from '../api/proto-gremlin'

import { incl, isTrue } from '../ops'
import { VersionStore, versionStoreFactory } from '../version-store'

const { chunk } = chunkerFactory(1024, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const blockCodec: BlockCodec = blockCodecFactory()
const multiBlockCodec: BlockCodec = multiBlockCodecFactory(chunk)

describe('Api', function () {
    test('internal, minimal creation, proto-schema aware, deterministic root and offsets', async () => {
        enum ObjectTypes {
            TWEET = 1,
        }

        enum RlshpTypes {
            COMMENT_TO = 1,
        }

        enum PropTypes {
            COMMENT = 1,
        }

        enum KeyTypes {
            JSON = 1,
        }

        const blockStore: BlockStore = memoryBlockStoreFactory()

        const story: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })

        const store = graphStore({
            chunk,
            linkCodec,
            blockCodec: multiBlockCodec,
            blockStore,
        })

        const graph = new Graph(story, store)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex(ObjectTypes.TWEET)
        const v2 = tx.addVertex(ObjectTypes.TWEET)
        const v3 = tx.addVertex(ObjectTypes.TWEET)

        await tx.addEdge(v1, v2, RlshpTypes.COMMENT_TO)
        await tx.addEdge(v1, v3, RlshpTypes.COMMENT_TO)

        await tx.addVertexProp(
            v2,
            KeyTypes.JSON,
            { hello: 'v2' },
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v2,
            KeyTypes.JSON,
            { hello: 'v3' },
            PropTypes.COMMENT
        )

        const { root, index, blocks } = await tx.commit({})

        const { vertexOffset, edgeOffset, propOffset } = index

        console.log(root.toString())

        assert.equal(OFFSET_INCREMENTS.VERTEX_INCREMENT * 3, vertexOffset)
        assert.equal(OFFSET_INCREMENTS.EDGE_INCREMENT * 2, edgeOffset)
        assert.equal(OFFSET_INCREMENTS.PROP_INCREMENT * 2, propOffset)

        assert.equal(
            'bafkreiguifcsbkfb7jlxr7inlp5fqukmve7mv2po234jj73pp7nryvbcxu',
            root.toString()
        )
    })

    test('equivalent proto-gremlin, minimal creation, proto-schema aware, deterministic root and offsets', async () => {
        enum ObjectTypes {
            TWEET = 1,
        }

        enum RlshpTypes {
            COMMENT_TO = 1,
        }

        enum PropTypes {
            COMMENT = 1,
        }

        enum KeyTypes {
            JSON = 1,
        }

        const blockStore: BlockStore = memoryBlockStoreFactory()

        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })

        const gf: ProtoGremlinFactory = protoGremlinFactory({
            chunk,
            linkCodec,
            blockCodec: multiBlockCodec,
            blockStore,
            versionStore,
        })

        const g: ProtoGremlin = gf.g()

        const tx = await g.tx()

        const v1 = await tx.addV(ObjectTypes.TWEET).next()
        const v2 = await tx
            .addV(ObjectTypes.TWEET)
            .property(KeyTypes.JSON, { hello: 'v2' }, PropTypes.COMMENT)
            .property(KeyTypes.JSON, { hello: 'v3' }, PropTypes.COMMENT)
            .next()
        const v3 = await tx.addV(ObjectTypes.TWEET).next()

        const e1 = await tx.addE(RlshpTypes.COMMENT_TO).from(v1).to(v2).next()
        const e2 = await tx.addE(RlshpTypes.COMMENT_TO).from(v1).to(v3).next()

        const { root, index, blocks } = await tx.commit({})
        const { vertexOffset, edgeOffset, propOffset } = index

        assert.equal(OFFSET_INCREMENTS.VERTEX_INCREMENT * 3, vertexOffset)
        assert.equal(OFFSET_INCREMENTS.EDGE_INCREMENT * 2, edgeOffset)
        assert.equal(OFFSET_INCREMENTS.PROP_INCREMENT * 2, propOffset)

        console.log(root.toString())

        assert.equal(
            'bafkreiguifcsbkfb7jlxr7inlp5fqukmve7mv2po234jj73pp7nryvbcxu',
            root.toString()
        )
    })

    test('internal minimal creation, proto-schema aware w/ additional edge properties', async () => {
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
        const store = graphStore({
            chunk,
            linkCodec,
            blockCodec: multiBlockCodec,
            blockStore,
        })
        const graph = new Graph(story, store)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex(ObjectTypes.TWEET)
        const v2 = tx.addVertex(ObjectTypes.TWEET)
        const v3 = tx.addVertex(ObjectTypes.TWEET)

        const e1 = await tx.addEdge(v1, v2, RlshpTypes.COMMENT_TO)
        const e2 = await tx.addEdge(v1, v3, RlshpTypes.COMMENT_TO)

        const p1 = await tx.addEdgeProp(
            e1,
            KeyTypes.VALUE,
            55,
            PropTypes.WEIGHT
        )
        const p2 = await tx.addEdgeProp(
            e2,
            KeyTypes.VALUE,
            33,
            PropTypes.WEIGHT
        )

        await tx.addVertexProp(
            v2,
            KeyTypes.TEXT,
            'hello world from v2',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v3,
            KeyTypes.TEXT,
            'hello world from v3',
            PropTypes.COMMENT
        )

        const { root, index } = await tx.commit({})

        const {
            vertexRoot,
            vertexOffset,
            vertexIndex,
            edgeRoot,
            edgeOffset,
            edgeIndex,
            propRoot,
            propOffset,
            propIndex,
        } = index

        console.log(root.toString())

        assert.equal(OFFSET_INCREMENTS.VERTEX_INCREMENT * 3, vertexOffset)
        assert.equal(OFFSET_INCREMENTS.EDGE_INCREMENT * 2, edgeOffset)
        assert.equal(OFFSET_INCREMENTS.PROP_INCREMENT * 4, propOffset)

        assert.equal(
            'bafkreicklvs2aaeqfvs6f2pgcki2gont35chka2loq7mlah7yu4tj6bsvy',
            root.toString()
        )
    })

    test('proto-schema aware creation, w/ navigation to edge ', async () => {
        const { v1, graph } = await createSchemaAwareGraph()

        const request = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .type(1)
            .add(PathElemType.EDGE)
            .type(1)
            .maxResults(10)
            .get()

        const edgeResults = []
        for await (const result of navigateVertices(
            graph,
            [v1.offset],
            request
        )) {
            edgeResults.push(result)
        }
        edgeResults.forEach((r) => console.log(r))

        // first edge
        assert.equal(0, edgeResults[0].offset)
        assert.equal(0, edgeResults[0].source)
        assert.equal(25, edgeResults[0].target)
        assert.equal(45, edgeResults[0].sourceNext)

        // second edge
        assert.equal(45, edgeResults[1].offset)
        assert.equal(0, edgeResults[1].source)
        assert.equal(50, edgeResults[1].target)
        assert.equal(0, edgeResults[1].sourcePrev)
        assert.equal(56, edgeResults[1].nextProp)
    })

    test('proto-schema aware creation, w/ navigation to vertex ', async () => {
        const { v1, graph } = await createSchemaAwareGraph()

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

        const request = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .type(ObjectTypes.TWEET)
            .add(PathElemType.EDGE)
            .type(RlshpTypes.COMMENT_TO)
            .propPred(KeyTypes.VALUE, incl([55, 33]))
            .add(PathElemType.VERTEX)
            .type(ObjectTypes.TWEET)
            .propPred(KeyTypes.TEXT, isTrue())
            .maxResults(10)
            .get()

        const vertexResults = []
        for await (const result of navigateVertices(
            graph,
            [v1.offset],
            request
        )) {
            vertexResults.push(result)
        }
        vertexResults.forEach((r) => console.log(r))

        // first vertex
        assert.equal(25, vertexResults[0].offset)
        assert.equal(112, vertexResults[0].nextProp)

        // second vertex
        assert.equal(50, vertexResults[1].offset)
        assert.equal(280, vertexResults[1].nextProp)
    })

    test('proto-schema aware creation, w/ extraction of properties ', async () => {
        const { v1, graph } = await createSchemaAwareGraph()

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

        const request = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .type(1)
            .add(PathElemType.EDGE)
            .type(1)
            .propPred(KeyTypes.VALUE, isTrue())
            .add(PathElemType.VERTEX)
            .type(1)
            .propPred(KeyTypes.TEXT, isTrue())
            .extract(3)
            .maxResults(10)
            .get()

        const propResults = []
        for await (const result of navigateVertices(
            graph,
            [v1.offset],
            request
        )) {
            propResults.push(result)
        }
        propResults.forEach((r) => console.log(r))

        assert.equal(4, propResults.length)

        assert.equal('comment 1 from v2', propResults[0].value)
        assert.equal('comment 2 from v2', propResults[1].value)
        assert.equal('comment 3 from v2', propResults[2].value)
        assert.equal('comment 1 from v3', propResults[3].value)
    })

    test('proto-schema aware creation, w/ reduction on properties ', async () => {
        enum ObjectTypes {
            TWEET = 1,
        }
        enum RlshpTypes {
            COMMENT_TO = 1,
        }

        enum PropTypes {
            KPI = 1,
            WEIGHT = 2,
        }

        enum KeyTypes {
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
        const v2 = tx.addVertex(ObjectTypes.TWEET)
        const v3 = tx.addVertex(ObjectTypes.TWEET)

        const e1 = await tx.addEdge(v1, v2, RlshpTypes.COMMENT_TO)
        const e2 = await tx.addEdge(v1, v3, RlshpTypes.COMMENT_TO)

        const p1 = await tx.addEdgeProp(
            e1,
            KeyTypes.VALUE,
            55,
            PropTypes.WEIGHT
        )
        const p2 = await tx.addEdgeProp(
            e2,
            KeyTypes.VALUE,
            33,
            PropTypes.WEIGHT
        )

        await tx.addVertexProp(v2, KeyTypes.VALUE, 100, PropTypes.KPI)
        await tx.addVertexProp(v2, KeyTypes.VALUE, 200, PropTypes.KPI)
        await tx.addVertexProp(v2, KeyTypes.VALUE, 300, PropTypes.KPI)
        await tx.addVertexProp(v3, KeyTypes.VALUE, 700, PropTypes.KPI)
        await tx.addVertexProp(v3, KeyTypes.VALUE, 50, PropTypes.KPI)

        const { root, index } = await tx.commit({})

        const request = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .type(1)
            .add(PathElemType.EDGE)
            .type(1)
            .propPred(KeyTypes.VALUE, isTrue())
            .add(PathElemType.VERTEX)
            .type(1)
            .propPred(KeyTypes.VALUE, isTrue())
            .extract(KeyTypes.VALUE)
            .reduce((previous, current) => {
                if (previous === undefined) previous = 0
                previous += current
                return previous
            })
            .maxResults(10)
            .get()

        const propResults = []
        for await (const result of navigateVertices(
            graph,
            [v1.offset],
            request
        )) {
            propResults.push(result)
        }
        propResults.forEach((r) => console.log(r))

        assert.equal(2, propResults.length)

        assert.equal(v2.offset, propResults[0].context.offset)
        assert.equal(v3.offset, propResults[1].context.offset)

        assert.equal(600, propResults[0].value)
        assert.equal(750, propResults[1].value)
    })
})

async function createSchemaAwareGraph() {
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
    const v2 = tx.addVertex(ObjectTypes.TWEET)
    const v3 = tx.addVertex(ObjectTypes.TWEET)

    const e1 = await tx.addEdge(v1, v2, RlshpTypes.COMMENT_TO)
    const e2 = await tx.addEdge(v1, v3, RlshpTypes.COMMENT_TO)

    const p1 = await tx.addEdgeProp(e1, KeyTypes.VALUE, 55, PropTypes.WEIGHT)
    const p2 = await tx.addEdgeProp(e2, KeyTypes.VALUE, 33, PropTypes.WEIGHT)

    await tx.addVertexProp(
        v2,
        KeyTypes.TEXT,
        'comment 1 from v2',
        PropTypes.COMMENT
    )
    await tx.addVertexProp(
        v2,
        KeyTypes.TEXT,
        'comment 2 from v2',
        PropTypes.COMMENT
    )
    await tx.addVertexProp(
        v2,
        KeyTypes.TEXT,
        'comment 3 from v2',
        PropTypes.COMMENT
    )
    await tx.addVertexProp(
        v3,
        KeyTypes.TEXT,
        'comment 1 from v3',
        PropTypes.COMMENT
    )

    const { root, index } = await tx.commit({})

    const {
        vertexRoot,
        vertexOffset,
        vertexIndex,
        edgeRoot,
        edgeOffset,
        edgeIndex,
        propRoot,
        propOffset,
        propIndex,
    } = index

    assert.equal(OFFSET_INCREMENTS.VERTEX_INCREMENT * 3, vertexOffset)
    assert.equal(OFFSET_INCREMENTS.EDGE_INCREMENT * 2, edgeOffset)
    assert.equal(OFFSET_INCREMENTS.PROP_INCREMENT * 6, propOffset)

    return { v1, graph }
}
