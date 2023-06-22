import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { GraphStore, graphStoreFactory } from '../graph-store'
import { Graph } from '../graph'
import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from '../block-store'
import {
    LinkCodec,
    linkCodecFactory,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import * as assert from 'assert'
import { navigateVertices, PathElemType, RequestBuilder } from '../navigate'
import { eq } from '../ops'
import {
    Link,
    Offset,
    Part,
    Prop,
    Comment,
    Tag,
    Block,
    Version,
} from '../types'
import { merge, MergePolicyEnum } from '../merge'
import { VersionStore, versionStoreFactory } from '../version-store'
import { link } from 'fs'
import { graphPackerFactory } from '../graph-packer'
import { version } from 'uuid'

/**
 * Some proto-schema
 */

enum ObjectTypes {
    FOLDER = 1,
    FILE = 2,
}

enum RlshpTypes {
    CONTAINS = 1,
}

enum PropTypes {
    META = 1,
    DATA = 2,
}

enum KeyTypes {
    NAME = 1,
    CONTENT = 2,
}

const { chunk } = chunkerFactory(512, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const valueCodec: ValueCodec = valueCodecFactory()
const {
    packVersionStore,
    restoreSingleIndex: restoreVersionStore,
    packGraphVersion,
    restoreGraphVersion,
} = graphPackerFactory(linkCodec)
const blockStore1: MemoryBlockStore = memoryBlockStoreFactory()

describe('Version pack and restore', function () {
    test('edit, bundle, restore, edit, bundle, restore, query', async () => {
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })
        const graphStore: GraphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })
        const g = new Graph(versionStore, graphStore)
        const tx = g.tx()
        await tx.start()
        const v1 = tx.addVertex(ObjectTypes.FOLDER)
        const v2 = tx.addVertex(ObjectTypes.FOLDER)
        const v3 = tx.addVertex(ObjectTypes.FILE)
        const e1 = await tx.addEdge(v1, v2, RlshpTypes.CONTAINS)
        const e2 = await tx.addEdge(v1, v3, RlshpTypes.CONTAINS)
        await tx.addVertexProp(v1, KeyTypes.NAME, 'root-folder', PropTypes.META)
        await tx.addVertexProp(
            v2,
            KeyTypes.NAME,
            'nested-folder',
            PropTypes.META
        )
        await tx.addVertexProp(v3, KeyTypes.NAME, 'nested-file', PropTypes.META)
        await tx.addVertexProp(
            v2,
            KeyTypes.CONTENT,
            'hello world from v2',
            PropTypes.DATA
        )
        await tx.addVertexProp(
            v3,
            KeyTypes.CONTENT,
            'hello world from v3',
            PropTypes.DATA
        )
        const { root: original } = await tx.commit({})

        /**
         * Pack version store
         */
        const versionStoreBundle: Block = await packVersionStore(
            versionStore.versionStoreRoot(),
            blockStore1,
            chunk,
            valueCodec
        )
        /**
         * Pack last version
         */
        const graphStoreBundle: Block = await packGraphVersion(
            original,
            blockStore1
        )
        /**
         * Empty block store
         */
        const memoryStore: BlockStore = memoryBlockStoreFactory()

        /**
         * Restore version store into the empty block store
         */
        const { root: storeRoot } = await restoreVersionStore(
            versionStoreBundle.bytes,
            memoryStore
        )
        /**
         * Restore last version into the empty block store
         */
        const { root: versionRoot } = await restoreGraphVersion(
            graphStoreBundle.bytes,
            memoryStore
        )

        /**
         * Edit restored version
         */
        const versionStore1: VersionStore = await versionStoreFactory({
            storeRoot,
            versionRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memoryStore,
        })
        const graphStore1 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memoryStore,
        })
        const g1 = new Graph(versionStore1, graphStore1)
        const tx1 = g1.tx()
        await tx1.start()
        const v10 = await tx1.getVertex(0)
        const v11 = tx1.addVertex(ObjectTypes.FILE)
        const e11 = await tx1.addEdge(v10, v11, RlshpTypes.CONTAINS)
        await tx1.addVertexProp(
            v11,
            KeyTypes.NAME,
            'nested-file-user-1',
            PropTypes.META
        )
        await tx1.addVertexProp(
            v11,
            KeyTypes.CONTENT,
            'hello world from v11',
            PropTypes.DATA
        )
        const { root: first } = await tx1.commit({})

        /**
         * Pack newest version store
         */
        const versionStoreBundle1: Block = await packVersionStore(
            versionStore1.versionStoreRoot(),
            memoryStore,
            chunk,
            valueCodec
        )
        /**
         * Pack newest version
         */
        const graphStoreBundle1: Block = await packGraphVersion(
            first,
            memoryStore
        )

        /**
         * Empty block store
         */
        const memoryStore1: BlockStore = memoryBlockStoreFactory()

        /**
         * Restore newest version store into the empty block store
         */
        const { root: storeRoot1 } = await restoreVersionStore(
            versionStoreBundle1.bytes,
            memoryStore1
        )
        /**
         * Restore newest graph version into the empty block store
         */
        const { root: versionRoot1 } = await restoreGraphVersion(
            graphStoreBundle1.bytes,
            memoryStore1
        )

        const versionStore2: VersionStore = await versionStoreFactory({
            storeRoot: storeRoot1,
            versionRoot: versionRoot1,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memoryStore1,
        })
        const graphStore2 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memoryStore1,
        })
        const g2 = new Graph(versionStore2, graphStore2)

        const vr = await query(g2)

        assert.strictEqual(vr.length, 3)
        assert.strictEqual(vr[0].value, 'nested-folder')
        assert.strictEqual(vr[1].value, 'nested-file')
        assert.strictEqual(vr[2].value, 'nested-file-user-1')
    })
})

const query = async (graph: Graph): Promise<Prop[]> => {
    const request = new RequestBuilder()
        .add(PathElemType.VERTEX)
        .add(PathElemType.EDGE)
        .add(PathElemType.VERTEX)
        .extract(KeyTypes.NAME)
        .maxResults(100)
        .get()

    const vr: Prop[] = []
    for await (const result of navigateVertices(graph, [0], request)) {
        vr.push(result as Prop)
    }
    return vr
}
