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
import { Link, Offset, Part, Prop, Comment, Tag } from '../types'
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

describe('Version management', function () {
    test('incremental graph versions, single graph, multiple transactions, log changes', async () => {
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

        const { root: original } = await tx.commit({
            comment: 'First draft',
            tags: ['v0.0.1'],
        })

        /**
         * Revise original, first change
         */

        const tx1 = graph.tx()
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

        const { root: first } = await tx1.commit({
            comment: 'Second draft',
            tags: ['v0.0.2'],
        })

        /**
         * Revise original, second change
         */

        const tx2 = graph.tx()
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

        const { root: second } = await tx2.commit({
            comment: 'First release',
            tags: ['v0.1.0'],
        })

        const versions = story.log()

        assert.strictEqual(versions.length, 3)

        // reversed order

        assert.strictEqual(
            linkCodec.encodeString(versions[0].root),
            'bafkreicwdw7eufain2py7aj6a7qmgy2ii5qq5nrhilwjocckrtnj3ljbqm'
        )
        assert.strictEqual(
            linkCodec.encodeString(versions[0].parent),
            'bafkreia7homtnphv3je3iwlfl6y3gmdrqrakswsl5sqpcw3gfz25wyfm4q'
        )
        assert.strictEqual(versions[0].details.comment, 'First release')
        assert.strictEqual(versions[0].details.tags.length, 1)
        assert.strictEqual(versions[0].details.tags[0], 'v0.1.0')

        assert.strictEqual(
            linkCodec.encodeString(versions[1].root),
            'bafkreia7homtnphv3je3iwlfl6y3gmdrqrakswsl5sqpcw3gfz25wyfm4q'
        )
        assert.strictEqual(
            linkCodec.encodeString(versions[1].parent),
            'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
        )
        assert.strictEqual(versions[1].details.comment, 'Second draft')
        assert.strictEqual(versions[1].details.tags.length, 1)
        assert.strictEqual(versions[1].details.tags[0], 'v0.0.2')

        assert.strictEqual(
            linkCodec.encodeString(versions[2].root),
            'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
        )
        assert.strictEqual(versions[2].parent, undefined)
        assert.strictEqual(versions[2].details.comment, 'First draft')
        assert.strictEqual(versions[2].details.tags.length, 1)
        assert.strictEqual(versions[2].details.tags[0], 'v0.0.1')

        const files = await query(second)

        assert.strictEqual(files.length, 4)
        assert.strictEqual(files[0].value, 'nested-folder')
        assert.strictEqual(files[1].value, 'nested-file')
        assert.strictEqual(files[2].value, 'nested-file-user-1')
        assert.strictEqual(files[3].value, 'nested-file-user-2')
    })

    test('incremental graph versions, version checkout', async () => {
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

        const { root: original } = await tx.commit({
            comment: 'First draft',
            tags: ['v0.0.1'],
        })

        /**
         * Revise original, first change
         */

        const tx1 = graph.tx()
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

        const { root: first } = await tx1.commit({
            comment: 'Second draft',
            tags: ['v0.0.2'],
        })

        /**
         * Revise, second change
         */

        const tx2 = graph.tx()
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

        const { root: second } = await tx2.commit({
            comment: 'First release',
            tags: ['v0.1.0'],
        })

        const files = await query(second)

        assert.strictEqual(files.length, 4)
        assert.strictEqual(files[0].value, 'nested-folder')
        assert.strictEqual(files[1].value, 'nested-file')
        assert.strictEqual(files[2].value, 'nested-file-user-1')
        assert.strictEqual(files[3].value, 'nested-file-user-2')

        /**
         * Revise original checkout
         */

        story.checkout(original)

        // ensure fresh graph object
        const g3 = new Graph(story, store)

        const tx3 = g3.tx()
        await tx3.start()
        const v30 = await tx3.getVertex(0)
        const v31 = tx3.addVertex(ObjectTypes.FILE)
        const e31 = await tx3.addEdge(v30, v31, RlshpTypes.CONTAINS)
        await tx3.addVertexProp(
            v31,
            KeyTypes.NAME,
            'nested-file-user-3',
            PropTypes.META
        )
        await tx3.addVertexProp(
            v31,
            KeyTypes.CONTENT,
            'hello world from v31',
            PropTypes.DATA
        )

        const { root: third } = await tx3.commit({
            comment: 'Second release',
            tags: ['v0.2.0'],
        })

        const files3 = await query(third)

        assert.strictEqual(files3.length, 3)
        assert.strictEqual(files3[0].value, 'nested-folder')
        assert.strictEqual(files3[1].value, 'nested-file')
        assert.strictEqual(files3[2].value, 'nested-file-user-3')

        const versions = story.log()

        assert.strictEqual(versions.length, 4)

        /**
         * Current version is parented by the original version
         */

        assert.strictEqual(
            linkCodec.encodeString(versions[0].root),
            'bafkreigx63iqmw743rxhlxpkz3ge5zdid2qnpxl7vujgyu2eftmhoasnqa'
        )
        assert.strictEqual(
            linkCodec.encodeString(versions[0].parent),
            'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
        )
        assert.strictEqual(versions[0].details.comment, 'Second release')
        assert.strictEqual(versions[0].details.tags.length, 1)
        assert.strictEqual(versions[0].details.tags[0], 'v0.2.0')

        assert.strictEqual(
            linkCodec.encodeString(versions[3].root),
            'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
        )
        assert.strictEqual(versions[3].parent, undefined)
        assert.strictEqual(versions[3].details.comment, 'First draft')
        assert.strictEqual(versions[3].details.tags.length, 1)
        assert.strictEqual(versions[3].details.tags[0], 'v0.0.1')
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
