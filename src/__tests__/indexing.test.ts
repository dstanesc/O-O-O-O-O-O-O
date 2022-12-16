import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from '../block-store'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import {
    LinkCodec,
    linkCodecFactory,
    BlockCodec,
    blockCodecFactory,
    multiBlockCodecFactory,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { Graph } from '../graph'
import { graphStore } from '../graph-store'
import * as assert from 'assert'
import { Edge, EdgeRef, Index, IndexedValue, Prop } from '../types'
import { indexStoreFactory } from '../index-store-factory'
import { navigateVertices, PathElemType, RequestBuilder } from '../navigate'
import { eq } from '../ops'
import { VersionStore, versionStoreFactory } from '../version-store'

const { chunk } = chunkerFactory(1024, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const valueCodec: ValueCodec = valueCodecFactory()
const blockCodec: BlockCodec = blockCodecFactory()
const multiBlockCodec: BlockCodec = multiBlockCodecFactory(chunk)

describe('Indexing', function () {
    test('index store api', async () => {
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const indexStore = indexStoreFactory(blockStore)
        const root = await indexStore.indexCreate([{ value: 'aaa', ref: 111 }])
        const result = await indexStore.indexSearch(root, 'aaa')
        assert.strictEqual(result.ref, 111)
        assert.strictEqual(result.value, 'aaa')
    })

    test('create index', async () => {
        enum ObjectTypes {
            TWEET = 1,
        }
        enum RlshpTypes {
            COMMENT_TO = 1,
        }

        enum PropTypes {
            NAME = 2,
            COMMENT = 3,
        }

        enum KeyTypes {
            NAME = 33,
            TEXT = 11,
        }

        enum IndexTypes {
            L2_NAME = 33,
        }

        const blockStore: MemoryBlockStore = memoryBlockStoreFactory()
        const story: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const store = graphStore({ chunk, linkCodec, valueCodec, blockStore })
        const indexStore = indexStoreFactory(blockStore)

        const graph = new Graph(story, store, indexStore)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex(ObjectTypes.TWEET)

        // second level
        const v2 = tx.addVertex(ObjectTypes.TWEET)
        const v3 = tx.addVertex(ObjectTypes.TWEET)
        const v4 = tx.addVertex(ObjectTypes.TWEET)
        const v5 = tx.addVertex(ObjectTypes.TWEET)
        const v6 = tx.addVertex(ObjectTypes.TWEET)
        const v7 = tx.addVertex(ObjectTypes.TWEET)
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
        const e3 = await tx.addEdge(v1, v4, RlshpTypes.COMMENT_TO)
        const e4 = await tx.addEdge(v1, v5, RlshpTypes.COMMENT_TO)
        const e5 = await tx.addEdge(v1, v6, RlshpTypes.COMMENT_TO)
        const e6 = await tx.addEdge(v1, v7, RlshpTypes.COMMENT_TO)
        const e7 = await tx.addEdge(v1, v8, RlshpTypes.COMMENT_TO)
        const e8 = await tx.addEdge(v1, v9, RlshpTypes.COMMENT_TO)
        const e9 = await tx.addEdge(v1, v10, RlshpTypes.COMMENT_TO)
        const e10 = await tx.addEdge(v1, v11, RlshpTypes.COMMENT_TO)
        const e11 = await tx.addEdge(v1, v12, RlshpTypes.COMMENT_TO)
        const e12 = await tx.addEdge(v1, v13, RlshpTypes.COMMENT_TO)
        const e13 = await tx.addEdge(v1, v14, RlshpTypes.COMMENT_TO)
        const e14 = await tx.addEdge(v1, v15, RlshpTypes.COMMENT_TO)

        await tx.addVertexProp(v2, KeyTypes.NAME, 'v2', PropTypes.NAME)
        await tx.addVertexProp(v3, KeyTypes.NAME, 'v3', PropTypes.NAME)
        await tx.addVertexProp(v4, KeyTypes.NAME, 'v4', PropTypes.NAME)
        await tx.addVertexProp(v5, KeyTypes.NAME, 'v5', PropTypes.NAME)
        await tx.addVertexProp(v6, KeyTypes.NAME, 'v6', PropTypes.NAME)
        await tx.addVertexProp(v7, KeyTypes.NAME, 'v7', PropTypes.NAME)
        await tx.addVertexProp(v8, KeyTypes.NAME, 'v8', PropTypes.NAME)
        await tx.addVertexProp(v9, KeyTypes.NAME, 'v9', PropTypes.NAME)
        await tx.addVertexProp(v10, KeyTypes.NAME, 'v10', PropTypes.NAME)
        await tx.addVertexProp(v11, KeyTypes.NAME, 'v11', PropTypes.NAME)
        await tx.addVertexProp(v12, KeyTypes.NAME, 'v12', PropTypes.NAME)
        await tx.addVertexProp(v13, KeyTypes.NAME, 'v13', PropTypes.NAME)
        await tx.addVertexProp(v14, KeyTypes.NAME, 'v14', PropTypes.NAME)
        await tx.addVertexProp(v15, KeyTypes.NAME, 'v15', PropTypes.NAME)

        await tx.addVertexProp(
            v2,
            KeyTypes.TEXT,
            'Comment v2',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v3,
            KeyTypes.TEXT,
            'Comment v3',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v4,
            KeyTypes.TEXT,
            'Comment v4',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v5,
            KeyTypes.TEXT,
            'Comment v5',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v6,
            KeyTypes.TEXT,
            'Comment v6',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v7,
            KeyTypes.TEXT,
            'Comment v7',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v8,
            KeyTypes.TEXT,
            'Comment v8',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v9,
            KeyTypes.TEXT,
            'Comment v9',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v10,
            KeyTypes.TEXT,
            'Comment v10',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v11,
            KeyTypes.TEXT,
            'Comment v11',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v12,
            KeyTypes.TEXT,
            'Comment v12',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v13,
            KeyTypes.TEXT,
            'Comment v13',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v14,
            KeyTypes.TEXT,
            'Comment v14',
            PropTypes.COMMENT
        )
        await tx.addVertexProp(
            v15,
            KeyTypes.TEXT,
            'Comment v15',
            PropTypes.COMMENT
        )

        const index: Index = await tx.uniqueIndex(
            v1,
            KeyTypes.NAME,
            IndexTypes.L2_NAME
        )

        const { root } = await tx.commit({})

        const request = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .add(PathElemType.EDGE)
            .add(PathElemType.VERTEX)
            .propPred(KeyTypes.NAME, eq('v10'))
            .extract(KeyTypes.TEXT)
            .maxResults(10)
            .get()

        const g = new Graph(story, store, indexStore)
        blockStore.resetReads()
        const results = []
        for await (const result of navigateVertices(g, [v1.offset], request)) {
            results.push(result)
        }

        assert.strictEqual(results.length, 1)
        assert.strictEqual(results[0].offset, 704)
        assert.strictEqual(results[0].type, PropTypes.COMMENT)
        assert.strictEqual(results[0].key, KeyTypes.TEXT)
        assert.strictEqual(results[0].value, 'Comment v10')

        const blocksLoaded = blockStore.countReads()
        assert.strictEqual(blocksLoaded, 10)
    })
})
