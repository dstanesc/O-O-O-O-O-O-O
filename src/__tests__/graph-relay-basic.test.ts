import {
    Block,
    BlockStore,
    Graph,
    GraphStore,
    Link,
    LinkCodec,
    MemoryBlockStore,
    PathElemType,
    Prop,
    RequestBuilder,
    ValueCodec,
    VersionStore,
    chunkerFactory,
    graphPackerFactory,
    graphStoreFactory,
    linkCodecFactory,
    memoryBlockStoreFactory,
    navigateVertices,
    valueCodecFactory,
    versionStoreFactory,
} from '../index'

import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'

import {
    GraphRelay,
    LinkResolver,
    memoryBlockResolverFactory,
} from '@dstanesc/o-o-o-o-o-o-o-r'
import {
    BasicPushResponse,
    RelayClientBasic,
    relayClientBasicFactory,
} from '../relay-client'

const chunkSize = 512
const { chunk } = chunkerFactory(chunkSize, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const valueCodec: ValueCodec = valueCodecFactory()
const {
    packVersionStore,
    restoreSingleIndex: restoreVersionStore,
    packGraphVersion,
    restoreGraphVersion,
} = graphPackerFactory(linkCodec)

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

describe('Basic client tests', () => {
    let relayBlockStore: BlockStore
    let blockStore: MemoryBlockStore
    let linkResolver: LinkResolver
    let server: any
    let graphRelay: GraphRelay
    let relayClient: RelayClientBasic
    beforeAll((done) => {
        blockStore = memoryBlockStoreFactory()
        relayBlockStore = memoryBlockStoreFactory()
        linkResolver = memoryBlockResolverFactory()
        graphRelay = new GraphRelay(relayBlockStore, linkResolver)
        server = graphRelay.start(3000, done) // Start the server
        relayClient = relayClientBasicFactory(
            {
                chunk,
                chunkSize,
                linkCodec,
                valueCodec,
                blockStore,
            },
            {
                baseURL: 'http://localhost:3000',
            }
        )
    })

    afterAll((done) => {
        graphRelay.stop(done) // Stop the server
    })

    describe('the relay client', () => {
        let versionStoreId: string
        let originalStoreRoot: Link
        it('should push initial graph and history', async () => {
            const versionStore: VersionStore = await versionStoreFactory({
                chunk,
                linkCodec,
                valueCodec,
                blockStore,
            })
            const graphStore: GraphStore = graphStoreFactory({
                chunk,
                linkCodec,
                valueCodec,
                blockStore,
            })
            const graph = new Graph(versionStore, graphStore)
            const tx = graph.tx()
            await tx.start()
            const v1 = tx.addVertex(ObjectTypes.FOLDER)
            const v2 = tx.addVertex(ObjectTypes.FOLDER)
            const v3 = tx.addVertex(ObjectTypes.FILE)
            const e1 = await tx.addEdge(v1, v2, RlshpTypes.CONTAINS)
            const e2 = await tx.addEdge(v1, v3, RlshpTypes.CONTAINS)
            await tx.addVertexProp(
                v1,
                KeyTypes.NAME,
                'root-folder',
                PropTypes.META
            )
            await tx.addVertexProp(
                v2,
                KeyTypes.NAME,
                'nested-folder',
                PropTypes.META
            )
            await tx.addVertexProp(
                v3,
                KeyTypes.NAME,
                'nested-file',
                PropTypes.META
            )
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

            versionStoreId = versionStore.id()

            originalStoreRoot = versionStore.versionStoreRoot()

            const response: BasicPushResponse = await relayClient.push(
                versionStore.versionStoreRoot()
            )

            expect(original.toString()).toEqual(
                'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
            )
            expect(response.storeRoot.toString()).toEqual(
                versionStore.versionStoreRoot().toString()
            )
            expect(response.versionRoot.toString()).toEqual(
                'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
            )
            expect(response.versionsPushed.length).toEqual(1)

            expect(response.versionsPushed[0].versionRoot.toString()).toEqual(
                'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
            )
        })

        it('should pull graph and history', async () => {
            const { versionStore, graphStore, graph } = await relayClient.pull(
                versionStoreId
            )

            expect(versionStore.id()).toEqual(versionStoreId)

            expect(versionStore.versionStoreRoot().toString()).toEqual(
                originalStoreRoot.toString()
            )

            const vr = await query(graph)

            expect(vr.length).toEqual(2)
            expect(vr[0].value).toEqual('nested-folder')
            expect(vr[1].value).toEqual('nested-file')
        })

        it('should update existing and pushed result should reflect changes', async () => {
            const { versionStore, graphStore, graph } = await relayClient.pull(
                versionStoreId
            )
            const tx = graph.tx()
            await tx.start()
            const v10 = await tx.getVertex(0)
            const v11 = tx.addVertex(ObjectTypes.FILE)
            const e11 = await tx.addEdge(v10, v11, RlshpTypes.CONTAINS)
            await tx.addVertexProp(
                v11,
                KeyTypes.NAME,
                'nested-file-user-1',
                PropTypes.META
            )
            await tx.addVertexProp(
                v11,
                KeyTypes.CONTENT,
                'hello world from v11',
                PropTypes.DATA
            )
            const { root: first } = await tx.commit({})

            const response: BasicPushResponse = await relayClient.push(
                versionStore.versionStoreRoot()
            )
            expect(versionStore.id()).toEqual(versionStoreId)

            expect(first.toString()).toEqual(
                'bafkreia7homtnphv3je3iwlfl6y3gmdrqrakswsl5sqpcw3gfz25wyfm4q'
            )

            expect(response.storeRoot.toString()).toEqual(
                versionStore.versionStoreRoot().toString()
            )

            expect(response.versionsPushed.length).toEqual(2)

            expect(response.versionsPushed[0].versionRoot.toString()).toEqual(
                'bafkreia7homtnphv3je3iwlfl6y3gmdrqrakswsl5sqpcw3gfz25wyfm4q'
            )
            expect(response.versionsPushed[1].versionRoot.toString()).toEqual(
                'bafkreiflyrpgzvjjg3ve36ecgv24k5zfjc6hdz7yttko36ho7hy3yhgrue'
            )

            const { versionStore: versionStore2, graph: graph2 } =
                await relayClient.pull(versionStoreId)

            expect(versionStore.versionStoreRoot().toString()).toEqual(
                versionStore2.versionStoreRoot().toString()
            )

            const vr = await query(graph2)

            expect(vr.length).toEqual(3)
            expect(vr[0].value).toEqual('nested-folder')
            expect(vr[1].value).toEqual('nested-file')
            expect(vr[2].value).toEqual('nested-file-user-1')
        })
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
