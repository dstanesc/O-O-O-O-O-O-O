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

describe('Version store merge', function () {
    test('Revise graphs independently, bundle differences, merge results', async () => {
        const blockStore1: MemoryBlockStore = memoryBlockStoreFactory()
        const blockStore2: MemoryBlockStore = memoryBlockStoreFactory()
        /**
         * Build original data set
         */
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

        const graph = new Graph(versionStore, graphStore)

        const tx = graph.tx()

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

        // transfer all blocks from initial commit
        await blockStore1.push(blockStore2)

        const originalVersionStoreId = versionStore.id()

        /**
         * Revise original, first user
         */
        const versionStore1: VersionStore = await versionStoreFactory({
            versionRoot: original,
            storeRoot: versionStore.versionStoreRoot(),
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })

        const graphStore1 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
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

        assert.equal(versionStore1.id(), originalVersionStoreId)

        const log1 = versionStore1.log()

        /**
         * Revise original, second user
         */
        const versionStore2: VersionStore = await versionStoreFactory({
            versionRoot: original,
            storeRoot: versionStore.versionStoreRoot(),
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore2,
        })

        const graphStore2 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore2,
        })
        const g2 = new Graph(versionStore2, graphStore2)

        const tx2 = g2.tx()
        await tx2.start()
        const v20 = await tx2.getVertex(0)
        const v21 = tx2.addVertex(ObjectTypes.FILE)
        const e21 = await tx2.addEdge(v20, v21, RlshpTypes.CONTAINS)
        await tx2.addVertexProp(
            v21,
            KeyTypes.NAME,
            'nested-file-user-2',
            PropTypes.META
        )
        await tx2.addVertexProp(
            v21,
            KeyTypes.CONTENT,
            'hello world from v21',
            PropTypes.DATA
        )

        const { root: second, blocks: secondBlocks } = await tx2.commit({})

        assert.equal(versionStore2.id(), originalVersionStoreId)

        const log2 = versionStore2.log()

        /**
         * Create a bundle w/ blocks present in versionStore2/blockStore2 and missing in versionStore1/blocksStore1
         */
        const bundle: Block = await versionStore1.packMissingBlocks(
            versionStore2,
            blockStore2
        )

        const { restoreRandomBlocks } = graphPackerFactory(linkCodec)

        /**
         * Restore blocks from the bundle into the blockStore1
         */
        await restoreRandomBlocks(bundle.bytes, blockStore1)

        /**
         * Merge versionStore2 into versionStore1
         */
        const {
            root: mergedRoot,
            index: mergedIndex,
            blocks: mergedBlocks,
        } = await versionStore1.mergeVersions(versionStore2)

        assert.equal(versionStore1.id(), originalVersionStoreId)
        assert.equal(
            mergedRoot.toString(),
            'bafkreigs5zkhwksdnqersuu2nu7jueivtg3w3dzmysv6tmgx5jhi3o53ae'
        )

        const log3 = versionStore1.log()

        assert.equal(log1.length, 2)
        assert.equal(log2.length, 2)
        assert.equal(log3.length, 3)

        const diff1 = log3.filter((x) => !log1.includes(x))

        assert.equal(diff1.length, 1)

        assert.equal(
            diff1[0].root.toString(),
            'bafkreigs5zkhwksdnqersuu2nu7jueivtg3w3dzmysv6tmgx5jhi3o53ae'
        )

        assert.equal(
            log1[0].root.toString(),
            'bafkreia7homtnphv3je3iwlfl6y3gmdrqrakswsl5sqpcw3gfz25wyfm4q'
        )

        assert.equal(
            log1[0].parent.toString(),
            'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
        )

        assert.equal(
            log2[0].root.toString(),
            'bafkreibgksdipkjqzcuoslti5st6fkrbmu5toju63qvedjuseztdxr64ba'
        )

        assert.equal(
            log2[0].parent.toString(),
            'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
        )

        assert.equal(
            log3[0].root.toString(),
            'bafkreigs5zkhwksdnqersuu2nu7jueivtg3w3dzmysv6tmgx5jhi3o53ae'
        )
        assert.equal(
            log3[0].parent.toString(),
            'bafkreia7homtnphv3je3iwlfl6y3gmdrqrakswsl5sqpcw3gfz25wyfm4q'
        )
        assert.equal(
            log3[0].mergeParent.toString(),
            'bafkreibgksdipkjqzcuoslti5st6fkrbmu5toju63qvedjuseztdxr64ba'
        )

        const g3 = new Graph(versionStore1, graphStore1)

        const request = new RequestBuilder()
            .add(PathElemType.VERTEX)
            .add(PathElemType.EDGE)
            .add(PathElemType.VERTEX)
            .extract(KeyTypes.NAME)
            .maxResults(100)
            .get()

        const results: Prop[] = []
        for await (const result of navigateVertices(g3, [0], request)) {
            results.push(result as Prop)
        }

        assert.equal(results[3].value, 'nested-file-user-1')
        assert.equal(results[2].value, 'nested-file-user-2')
        assert.equal(results[1].value, 'nested-file')
        assert.equal(results[0].value, 'nested-folder')
    })

    test('Revise remote graph, merge fast forward', async () => {
        const blockStore1: MemoryBlockStore = memoryBlockStoreFactory()
        const blockStore2: MemoryBlockStore = memoryBlockStoreFactory()
        /**
         * Build original data set
         */
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

        const graph = new Graph(versionStore, graphStore)

        const tx = graph.tx()

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

        // transfer all blocks from initial commit
        await blockStore1.push(blockStore2)

        const originalVersionStoreId = versionStore.id()

        /**
         * Revise original, second user
         */
        const versionStore2: VersionStore = await versionStoreFactory({
            versionRoot: original,
            storeRoot: versionStore.versionStoreRoot(),
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore2,
        })

        const graphStore2 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore2,
        })
        const g2 = new Graph(versionStore2, graphStore2)

        const tx2 = g2.tx()
        await tx2.start()
        const v20 = await tx2.getVertex(0)
        const v21 = tx2.addVertex(ObjectTypes.FILE)
        const e21 = await tx2.addEdge(v20, v21, RlshpTypes.CONTAINS)
        await tx2.addVertexProp(
            v21,
            KeyTypes.NAME,
            'nested-file-user-2',
            PropTypes.META
        )
        await tx2.addVertexProp(
            v21,
            KeyTypes.CONTENT,
            'hello world from v21',
            PropTypes.DATA
        )

        const { root: second, blocks: secondBlocks } = await tx2.commit({})

        assert.equal(versionStore2.id(), originalVersionStoreId)

        /**
         * Create a bundle w/ blocks present in versionStore2/blockStore2 and missing in versionStore/blocksStore
         */
        const bundle: Block = await versionStore.packMissingBlocks(
            versionStore2,
            blockStore2
        )

        const { restoreRandomBlocks } = graphPackerFactory(linkCodec)

        /**
         * Restore blocks from the bundle into the blockStore
         */
        await restoreRandomBlocks(bundle.bytes, blockStore1)

        /**
         * Merge versionStore2 into versionStore, fast forward
         */
        const {
            root: mergedRoot,
            index: mergedIndex,
            blocks: mergedBlocks,
        } = await versionStore.mergeVersions(versionStore2)

        assert.equal(versionStore.id(), originalVersionStoreId)
        assert.equal(
            mergedRoot.toString(),
            'bafkreibgksdipkjqzcuoslti5st6fkrbmu5toju63qvedjuseztdxr64ba'
        )

        const version: Version = versionStore.log()[0]
        assert.equal(version.root.toString(), mergedRoot.toString())
        assert.equal(version.parent.toString(), original.toString())
        const { extractVersionBlocks } = graphPackerFactory(linkCodec)
        const expectedBlocks = await extractVersionBlocks(
            { root: second },
            blockStore2
        )
        assert.deepEqual(mergedBlocks, expectedBlocks)
    })

    test('zero change version merge', async () => {
        const blockStore1: MemoryBlockStore = memoryBlockStoreFactory()
        /**
         * Build original data set
         */
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

        const graph = new Graph(versionStore, graphStore)

        const tx = graph.tx()

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

        const versionStore1: VersionStore = await versionStoreFactory({
            versionRoot: original,
            storeRoot: versionStore.versionStoreRoot(),
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })

        const versionStore2: VersionStore = await versionStoreFactory({
            versionRoot: original,
            storeRoot: versionStore.versionStoreRoot(),
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore1,
        })

        const {
            root: mergedRoot,
            index: mergedIndex,
            blocks: mergedBlocks,
        } = await versionStore1.mergeVersions(versionStore2)

        assert.equal(mergedRoot.toString(), original.toString())
    })
})
