# O-O-O-O-O-O-O

![](./img/OOOOOOO-W100.png) is the next level persistence. There is cloud continuum and there is local store. Locality first, non local next.

_WIP_

## Examples

Schema-less creation and navigation, proto-gremlin API

```ts
const { chunk } = chunkerFactory(1024, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const blockCodec: BlockCodec = blockCodecFactory()
const blockStore: BlockStore = memoryBlockStoreFactory()
const rootStore: RootStore = emptyRootStore()

const g: ProtoGremlin = protoGremlinFactory({
    chunk,
    linkCodec,
    blockCodec,
    blockStore,
    rootStore,
}).g()

const tx = await g.tx()
const v1 = await tx.addV().next()
const v2 = await tx
    .addV()
    .property(1, { hello: 'v2' })
    .property(1, { hello: 'v3' })
    .next()
const v3 = await tx.addV().next()
const e1 = await tx.addE().from(v1).to(v2).next()
const e2 = await tx.addE().from(v1).to(v3).next()
await tx.commit()

const vr = []
for await (const result of g.V([v1.offset]).out().exec()) {
    vr.push(result)
}
```

Proto-schema based navigation and retrieval based on proto-gremlin API

```ts
enum ObjectTypes {
    ROOT = 1,
    BOOK = 2,
    CHAPTER = 3,
    VERSE = 4,
}
enum RlshpTypes {
    book = 1,
    chapter = 2,
    verse = 3,
}
enum KeyTypes {
    ID = 1,
    NAME = 2,
    TEXT = 3,
}

const cid = CID.parse(
    'bafkreibbirr5na66us6jjkpycr3qnt4ukbzmkjq4ic5jo7tmp2ngrbd7d4'
)
const cache = {}
const ipfs = ipfsApi({ url: process.env.IPFS_API })
const { chunk } = chunkerFactory(1024 * 16, compute_chunks)
const linkCodec: LinkCodec = linkCodecFactory()
const blockCodec: BlockCodec = blockCodecFactory()
const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
const { buildRootIndex } = blockIndexFactory({ linkCodec, blockStore })
const rootStore: RootStore = initRootStore(await buildRootIndex(cid))
const g: ProtoGremlin = protoGremlinFactory({
    chunk,
    linkCodec,
    blockCodec,
    blockStore,
    rootStore,
}).g()

// quick scan loads 15 blocks from 31549 total
const r1 = await queryVerse(g, 0, 'Gen', 1, 1)

// full scan loads 594 blocks (401 blocks if indexed) from 31549 total
const r2 = await queryVerse(g, 0, 'Rev', 22, 21)

async function queryVerse(
    g: ProtoGremlin,
    rootOffset: VertexRef,
    book: string,
    chapter: number,
    verse: number
): Promise<{ result: string; time: number }> {
    const vr = []
    for await (const result of g
        .V([rootOffset])
        .out(RlshpTypes.book)
        .has(ObjectTypes.BOOK, { keyType: KeyTypes.ID, operation: eq(book) })
        .out(RlshpTypes.chapter)
        .has(ObjectTypes.CHAPTER, {
            keyType: KeyTypes.ID,
            operation: eq(chapter),
        })
        .out(RlshpTypes.verse)
        .has(ObjectTypes.VERSE, { keyType: KeyTypes.ID, operation: eq(verse) })
        .values(KeyTypes.TEXT)
        .maxResults(1)
        .exec()) {
        vr.push(result)
    }
    return vr[0]
}
```

## Plugable Storage

-   [IndexedDB](https://www.npmjs.com/package/@dstanesc/idb-block-store) for browser local
-   [Azure](https://www.npmjs.com/package/@dstanesc/az-block-store)
-   [S3](https://www.npmjs.com/package/@dstanesc/s3-block-store)
-   [IPFS](https://www.npmjs.com/package/@dstanesc/ipfs-block-store)
-   [IPFS over HTTP](https://www.npmjs.com/package/@dstanesc/http-block-store)
-   [Lucy](https://www.npmjs.com/package/@dstanesc/lucy-block-store) to store blocks everywhere

## Plugable APIs

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
