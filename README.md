# O-O-O-O-O-O-O

![](./img/OOOOOOO-W100.png) modular persistence for web3 applications. Neo4j inspired index-free adjacency for navigation efficiency. Vertex, edge and property data are logical byte arrays created, modified and accessed using the [store-chunky-bytes](https://www.npmjs.com/package/@dstanesc/store-chunky-bytes) library. The current choice for data partitioning stability on change is content defined chunking (ie. FastCDC algorithm employed via [wasm-chunking-fastcdc](https://www.npmjs.com/package/@dstanesc/wasm-chunking-fastcdc-node) library). The data blocks, which are the atomic units of persistence, are effectively immutable and identified w/ cryptographic hashes. Depending on graph topology, edge indexing can minimize the number of block reads, hence accelerate navigation. Indexes are provided by the [prolly trees](https://www.npmjs.com/package/prolly-trees) library.

_WIP_

## Example

Create

```ts
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
const blockCodec: BlockCodec = blockCodecFactory()
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

const v1 = tx.addVertex(ObjectTypes.FOLDER)
const v2 = tx.addVertex(ObjectTypes.FOLDER)
const v3 = tx.addVertex(ObjectTypes.FILE)

const e1 = await tx.addEdge(v1, v2, RlshpTypes.CONTAINS)
const e2 = await tx.addEdge(v1, v3, RlshpTypes.CONTAINS)

await tx.addVertexProp(v1, KeyTypes.NAME, 'root-folder', PropTypes.META)
await tx.addVertexProp(v2, KeyTypes.NAME, 'nested-folder', PropTypes.META)
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

const { root, index, blocks } = await tx.commit({
    comment: 'First draft',
    tags: ['v0.0.1'],
})
```

Navigate the graph, filter and extract data

```ts
const query = async (versionRoot: Link): Promise<Prop[]> => {
    const versionStore: VersionStore = await versionStoreFactory({
        versionRoot,
        chunk,
        linkCodec,
        blockCodec,
        blockStore,
    })
    const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })
    const graph = new Graph(versionStore, store)
    const request = new RequestBuilder()
        .add(PathElemType.VERTEX)
        .add(PathElemType.EDGE)
        .add(PathElemType.VERTEX)
        .propPred(KeyTypes.CONTENT, eq('hello world from v3'))
        .extract(KeyTypes.NAME)
        .maxResults(100)
        .get()

    const vr: Prop[] = []
    for await (const result of navigateVertices(graph, [0], request)) {
        vr.push(result as Prop)
    }
    return vr
}
```


## Multiple stores

-   [IndexedDB](https://www.npmjs.com/package/@dstanesc/idb-block-store) for browser local
-   [Azure](https://www.npmjs.com/package/@dstanesc/az-block-store)
-   [S3](https://www.npmjs.com/package/@dstanesc/s3-block-store)
-   [IPFS](https://www.npmjs.com/package/@dstanesc/ipfs-block-store)
-   [IPFS over HTTP](https://www.npmjs.com/package/@dstanesc/http-block-store)
-   [Lucy](https://www.npmjs.com/package/@dstanesc/lucy-block-store) to store blocks everywhere

or provide your own

```ts
interface BlockStore {
    put: (block: { cid: any; bytes: Uint8Array }) => Promise<void>
    get: (cid: any) => Promise<Uint8Array>
}
```

## Multiple APIs

-   Native
-   Proto-gremlin
-   ...

## Build

```sh
npm run clean
npm install
npm run build
npm run test
```

## Licenses

Licensed under either [Apache 2.0](http://opensource.org/licenses/MIT) or [MIT](http://opensource.org/licenses/MIT) at your option.
