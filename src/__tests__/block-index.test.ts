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
import {
    Link,
    Offset,
    Part,
    Prop,
    Comment,
    Tag,
    Block,
    Version,
    ContentDiff,
    RootIndex,
} from '../types'
import { VersionStore, versionStoreFactory } from '../version-store'
import { graphPackerFactory } from '../graph-packer'
import { blockIndexFactory } from '../block-index'

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
    FILL = 3,
}

const { chunk } = chunkerFactory(512, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const valueCodec: ValueCodec = valueCodecFactory()
const blockStore1: MemoryBlockStore = memoryBlockStoreFactory()
const {
    packVersionStore,
    restoreSingleIndex: restoreVersionStore,
    packGraphVersion,
    restoreGraphVersion,
    packRootIndex,
    restoreRootIndex,
    packRandomBlocks,
    restoreRandomBlocks,
} = graphPackerFactory(linkCodec)

describe('Block index', function () {
    test('diffRootIndex returns all relevant blocks for incremental bundles', async () => {
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
        // force generating blocks
        for (let i = 0; i < 100; i++) {
            await tx.addVertexProp(
                v3,
                KeyTypes.FILL,
                new Uint8Array(1024),
                PropTypes.DATA
            )
        }

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
         * Pack original root index
         */

        const rootIndexBundle: Block = await packRootIndex(
            original,
            blockStore1
        )

        /**
         * Empty block store
         */
        const memoryStore: MemoryBlockStore = memoryBlockStoreFactory()

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
         * Pack newest root index
         */
        const rootIndexBundle1: Block = await packRootIndex(first, memoryStore)

        /**
         * Empty block store
         */
        const memoryStore1: MemoryBlockStore = memoryBlockStoreFactory()

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

        const diffStore: MemoryBlockStore = memoryBlockStoreFactory()

        /**
         * Restore original root index into the empty block store
         */
        const { root: originalRootLink } = await restoreRootIndex(
            rootIndexBundle.bytes,
            diffStore
        )

        assert.strictEqual(
            originalRootLink.toString(),
            original.toString(),
            'original root link properly unpacked'
        )

        /**
         * Restore newest root index into the empty block store
         */
        const { root: newestRootLink } = await restoreRootIndex(
            rootIndexBundle1.bytes,
            diffStore
        )

        assert.strictEqual(
            newestRootLink.toString(),
            first.toString(),
            'newest root link properly unpacked'
        )

        const blockIndexBuilder = blockIndexFactory({
            linkCodec,
            blockStore: diffStore,
        })

        // Compute content diff based on the restored root indices
        const contentDiff: ContentDiff = await blockIndexBuilder.diffRootIndex({
            currentRoot: originalRootLink,
            otherRoot: newestRootLink,
        })

        assert.equal(contentDiff.added.length, 5)

        assert.strictEqual(
            linkCodec.encodeString(contentDiff.added[0]),
            'bafkreihtrcfbwpb2jytsj7lfk7mf4pu64peipthclvtgaayfqzuwoqhnk4'
        )

        assert.strictEqual(
            linkCodec.encodeString(contentDiff.added[1]),
            'bafkreic7bnjlkbyiieuuewdfhhleehcye2fhycg3bcuwwnbphdtvlpmyoa'
        )

        assert.strictEqual(
            linkCodec.encodeString(contentDiff.added[2]),
            'bafkreiftybfklunci2s7z6z73jyn2mwig7za32owsvlssxmwcitbbzt63e'
        )

        assert.strictEqual(
            linkCodec.encodeString(contentDiff.added[3]),
            'bafkreif4bd6365zrkbecbib6prfthbs4lpxayteyjv56rsrax7nyhtxgze'
        )

        assert.strictEqual(
            linkCodec.encodeString(contentDiff.added[4]),
            'bafkreiht6mcqgekpczcylrp5f4uv6hruz6b5cdto5iqiremkjyoxlkwhpa'
        )

        const blocks: Block[] = []
        for (const cid of contentDiff.added) {
            const bytes = await memoryStore1.get(cid)
            const block: Block = { cid, bytes }
            blocks.push(block)
        }

        const diffBundle = await packRandomBlocks(blocks)

        // create a fresh block store
        const incrementalStore: MemoryBlockStore = memoryBlockStoreFactory()
        // containing original blocks
        await memoryStore.push(incrementalStore)
        // and the new version store
        await restoreVersionStore(versionStoreBundle1.bytes, incrementalStore)
        // and just the missing/diff blocks
        await restoreRandomBlocks(diffBundle.bytes, incrementalStore)

        // the blocks from the incremental store should be sufficient for resolving the query
        const versionStore3: VersionStore = await versionStoreFactory({
            storeRoot: storeRoot1,
            versionRoot: versionRoot1,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: incrementalStore,
        })
        const graphStore3 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: incrementalStore,
        })
        const g3 = new Graph(versionStore3, graphStore3)
        const incr = await query(g3)
        assert.strictEqual(incr.length, 3)
        assert.strictEqual(incr[0].value, 'nested-folder')
        assert.strictEqual(incr[1].value, 'nested-file')
        assert.strictEqual(incr[2].value, 'nested-file-user-1')

        // incremental store should contain all the blocks associated with the root index
        const { index: lastIndex } = await blockIndexBuilder.buildRootIndex(
            newestRootLink
        )
        const {
            vertexIndex: lastVertexIndex,
            edgeIndex: lastEdgeIndex,
            propIndex: lastPropIndex,
            valueIndex: lastValueIndex,
            indexIndex: lastIndexIndex,
        } = lastIndex

        const vertexLinks = Array.from(
            lastVertexIndex.indexStruct.startOffsets.values()
        )
        const edgeLinks = Array.from(
            lastEdgeIndex.indexStruct.startOffsets.values()
        )
        const propLinks = Array.from(
            lastPropIndex.indexStruct.startOffsets.values()
        )
        const valueLinks = Array.from(
            lastValueIndex.indexStruct.startOffsets.values()
        )
        const indexLinks = Array.from(
            lastIndexIndex.indexStruct.startOffsets.values()
        )

        const allLinks = [
            ...vertexLinks,
            ...edgeLinks,
            ...propLinks,
            ...valueLinks,
            ...indexLinks,
        ]

        assert.strictEqual(allLinks.length, 113)

        for (const link of allLinks) {
            const bytes = await incrementalStore.get(link)
            assert.ok(bytes !== undefined, 'block exists in incremental store')
        }
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
