import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { graphStore } from '../graph-store'
import { Graph } from '../graph'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import {
    LinkCodec,
    linkCodecFactory,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import * as assert from 'assert'
import { navigateVertices, PathElemType, RequestBuilder } from '../navigate'
import { eq } from '../ops'
import { Link, Offset, Part, Prop, Comment, Tag } from '../types'
import { merge, MergePolicyEnum } from '../merge'
import { VersionStore, versionStoreFactory } from '../version-store'

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
const blockStore: BlockStore = memoryBlockStoreFactory()

describe('Merge graphs', function () {
    test('internal api, simple merge', async () => {
        /**
         * Build original data set
         */
        const story: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const store = graphStore({ chunk, linkCodec, valueCodec, blockStore })

        const graph = new Graph(story, store)

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

        /**
         * Revise original, first user
         */

        const store1 = graphStore({ chunk, linkCodec, valueCodec, blockStore })
        const g1 = new Graph(story, store1)

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
         * Revise original, second user
         */
        story.checkout(original)

        const store2 = graphStore({ chunk, linkCodec, valueCodec, blockStore })
        const g2 = new Graph(story, store2)

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

        const { root: second } = await tx2.commit({})

        /**
         * Merge MultiValueRegistry
         */

        const {
            root: mergeRootMvr,
            index: mergeIndexMvr,
            blocks: mergeBlocksMvr,
        } = await merge(
            {
                baseRoot: original,
                baseStore: blockStore,
                currentRoot: first,
                currentStore: blockStore,
                otherRoot: second,
                otherStore: blockStore,
            },
            MergePolicyEnum.MultiValueRegistry,
            chunk,
            linkCodec,
            valueCodec
        )

        const mergedFilesMvr = await query(mergeRootMvr)

        //mergedFilesMvr.forEach(r => console.log(r))

        assert.strictEqual(mergedFilesMvr.length, 4)
        assert.strictEqual(mergedFilesMvr[0].value, 'nested-folder')
        assert.strictEqual(mergedFilesMvr[1].value, 'nested-file')
        assert.strictEqual(mergedFilesMvr[2].value, 'nested-file-user-2')
        assert.strictEqual(mergedFilesMvr[3].value, 'nested-file-user-1')

        /**
         * Merge LastWriterWins
         */

        const {
            root: mergeRootLww,
            index: mergeIndexLww,
            blocks: mergeBlocksLww,
        } = await merge(
            {
                baseRoot: original,
                baseStore: blockStore,
                currentRoot: first,
                currentStore: blockStore,
                otherRoot: second,
                otherStore: blockStore,
            },
            MergePolicyEnum.LastWriterWins,
            chunk,
            linkCodec,
            valueCodec
        )

        const mergedFilesLww = await query(mergeRootLww)

        assert.strictEqual(mergedFilesLww.length, 3)
        assert.strictEqual(mergedFilesLww[0].value, 'nested-folder')
        assert.strictEqual(mergedFilesLww[1].value, 'nested-file')
        assert.strictEqual(mergedFilesLww[2].value, 'nested-file-user-1')
    })
})



const query = async (versionRoot: Link): Promise<Prop[]> => {
    const versionStore: VersionStore = await versionStoreFactory({
        versionRoot,
        chunk,
        linkCodec,
        valueCodec,
        blockStore,
    })
    const store = graphStore({ chunk, linkCodec, valueCodec, blockStore })
    const graph = new Graph(versionStore, store)
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
