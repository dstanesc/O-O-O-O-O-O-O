import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { graphStore } from '../graph-store'
import { Graph } from '../graph'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import {
    BlockCodec,
    blockCodecFactory,
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

describe('Extract graph data fragments', function () {
    test('internal & proto-gremlin api, simple navigation incl. data template', async () => {
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
        const v4 = tx.addVertex(ObjectTypes.FILE)
        const v5 = tx.addVertex(ObjectTypes.FILE)

        const e1 = await tx.addEdge(v1, v2, RlshpTypes.CONTAINS)
        const e2 = await tx.addEdge(v1, v3, RlshpTypes.CONTAINS)
        const e3 = await tx.addEdge(v1, v4, RlshpTypes.CONTAINS)
        const e4 = await tx.addEdge(v2, v5, RlshpTypes.CONTAINS)

        await tx.addVertexProp(v1, KeyTypes.NAME, 'root-folder', PropTypes.META)

        await tx.addVertexProp(
            v2,
            KeyTypes.NAME,
            'nested-folder-v2',
            PropTypes.META
        )

        await tx.addVertexProp(
            v3,
            KeyTypes.NAME,
            'nested-file-v3',
            PropTypes.META
        )

        await tx.addVertexProp(
            v3,
            KeyTypes.CONTENT,
            'hello world from v3',
            PropTypes.DATA
        )

        await tx.addVertexProp(
            v4,
            KeyTypes.NAME,
            'nested-file-v4',
            PropTypes.META
        )
        await tx.addVertexProp(
            v4,
            KeyTypes.CONTENT,
            'hello world from v4',
            PropTypes.DATA
        )

        await tx.addVertexProp(
            v5,
            KeyTypes.NAME,
            'nested-file-v5',
            PropTypes.META
        )
        await tx.addVertexProp(
            v5,
            KeyTypes.CONTENT,
            'hello world from v5',
            PropTypes.DATA
        )

        const { root: original } = await tx.commit({})

        const roots = await zeroLevelList(graph)

        assert.strictEqual(roots.length, 1)
        assert.strictEqual(roots[0], 'root-folder')

        const names = await firstLevelList(graph)

        assert.strictEqual(names.length, 3)
        assert.strictEqual(names[0], 'nested-folder-v2')
        assert.strictEqual(names[1], 'nested-file-v3')
        assert.strictEqual(names[2], 'nested-file-v4')

        const results1 = await firstLevelTemplate(graph, DATA_TEMPLATE_1)

        assert.strictEqual(results1.length, 3)
        assert.strictEqual(results1[0].name, 'nested-folder-v2')
        assert.strictEqual(results1[0].content, undefined)
        assert.deepStrictEqual(results1[0].includes, [
            {
                name: 'nested-file-v5',
                content: 'hello world from v5',
            },
        ])
        assert.strictEqual(results1[1].name, 'nested-file-v3')
        assert.strictEqual(results1[1].content, 'hello world from v3')

        assert.strictEqual(results1[2].name, 'nested-file-v4')
        assert.strictEqual(results1[2].content, 'hello world from v4')

        const results2 = await firstLevelTemplate(graph, DATA_TEMPLATE_2)

        for (const content of results2) {
            console.log(content)
        }
    })
})

const zeroLevelList = async (graph: Graph): Promise<Prop[]> => {
    const request = new RequestBuilder()
        .add(PathElemType.VERTEX)
        .extract(KeyTypes.NAME)
        .maxResults(100)
        .get()

    const vr: Prop[] = []
    for await (const result of navigateVertices(graph, [0], request)) {
        vr.push((result as Prop).value)
    }
    return vr
}

const firstLevelList = async (graph: Graph): Promise<Prop[]> => {
    const request = new RequestBuilder()
        .add(PathElemType.VERTEX)
        .add(PathElemType.EDGE)
        .add(PathElemType.VERTEX)
        .extract(KeyTypes.NAME)
        .maxResults(100)
        .get()

    const vr: Prop[] = []
    for await (const result of navigateVertices(graph, [0], request)) {
        vr.push((result as Prop).value)
    }
    return vr
}

const DATA_TEMPLATE_1 = {
    name: {
        $elemType: PathElemType.EXTRACT,
        $type: KeyTypes.NAME,
    },
    content: {
        $elemType: PathElemType.EXTRACT,
        $type: KeyTypes.CONTENT,
    },
    includes: {
        $elemType: PathElemType.EDGE,
        $type: RlshpTypes.CONTAINS,
        name: {
            $elemType: PathElemType.EXTRACT,
            $type: KeyTypes.NAME,
        },
        content: {
            $elemType: PathElemType.EXTRACT,
            $type: KeyTypes.CONTENT,
        },
    },
}

const DATA_TEMPLATE_2 = {
    fileName: {
        $elemType: PathElemType.EXTRACT,
        $type: KeyTypes.NAME,
    },
    includes: {
        $elemType: PathElemType.EDGE,
        $type: RlshpTypes.CONTAINS,
        $value: {
            $elemType: PathElemType.EXTRACT,
            $type: KeyTypes.NAME,
        },
    },
}

const firstLevelTemplate = async (
    graph: Graph,
    template: any
): Promise<any[]> => {
    const request = new RequestBuilder()
        .add(PathElemType.VERTEX)
        .add(PathElemType.EDGE)
        .add(PathElemType.VERTEX)
        .template(template)
        .maxResults(100)
        .get()

    const vr: any[] = []
    for await (const result of navigateVertices(graph, [0], request)) {
        vr.push(result)
    }
    return vr
}
