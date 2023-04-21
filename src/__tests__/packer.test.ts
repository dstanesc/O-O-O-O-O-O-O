import { protoGremlinFactory, ProtoGremlin } from '../api/proto-gremlin'
import { Block, Link, RootIndex, Vertex, VertexRef } from '../types'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from '../block-store'
import { create as ipfsApi } from 'ipfs-http-client'
import { blockStore as ipfsBlockStore } from '@dstanesc/ipfs-block-store'
import {
    LinkCodec,
    linkCodecFactory,
    valueCodecFactory,
    ValueCodec,
} from '../codecs'
import { eq } from '../ops'
import * as assert from 'assert'
import bent from 'bent'
import { VersionStore, versionStoreFactory } from '../version-store'
import { graphPackerFactory } from '../graph-packer'

const getStream = bent('https://raw.githubusercontent.com')

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

enum PropTypes {
    ANY = 1,
}

async function queryVerse(
    g: ProtoGremlin,
    rootOffset: VertexRef,
    book: string,
    chapter: number,
    verse: number
): Promise<{ result: string; time: number }> {
    const vr = []
    const startTime = new Date().getTime()
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
    const endTime = new Date().getTime()
    const time = endTime - startTime
    console.log(`Query Time ${time} ms`)

    return { result: vr[0].value, time }
}

async function quickVerse(
    g: ProtoGremlin,
    verseOffset: VertexRef
): Promise<{ result: string; time: number }> {
    const vr = []
    const startTime = new Date().getTime()
    for await (const result of g
        .V([verseOffset])
        .values(KeyTypes.TEXT)
        .maxResults(1)
        .exec()) {
        vr.push(result)
    }
    const endTime = new Date().getTime()
    const time = endTime - startTime
    console.log(`Query Time ${time} ms`)
    return { result: vr[0].value, time }
}

describe('Graph packer', function () {
    test('pack and restore full graph', async () => {
        const cache = {}
        const ipfs = ipfsApi({ url: process.env.IPFS_API })
        const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
        const versionRoot = linkCodec.parseString(
            'bafkreiegljjns2rqb3z5mtdyvq2u6u2cvsahyez6bqsdjibo6737vrqhbi'
        )
        const versionStore: VersionStore = await versionStoreFactory({
            versionRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        /*
         * Packing and restoring
         */
        const { packGraph, restore } = graphPackerFactory(linkCodec)

        const bundle: Block = await packGraph(versionRoot, blockStore)

        const memStore: BlockStore = memoryBlockStoreFactory()

        const { root, index, blocks } = await restore(bundle.bytes, memStore)

        console.log(
            `Unpacked graph root: ${linkCodec.encodeString(
                root
            )}, block count: ${blocks.length}`
        )

        /**
         * Verifying
         */
        const g2: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memStore,
            versionStore,
        }).g()

        const { result, time } = await queryVerse(g2, 0, 'Rev', 22, 21)

        assert.strictEqual(
            result,
            'The grace of our Lord Jesus Christ be with you all. Amen.'
        )
        assert.ok(time < 3000) // 3sec

        /**
         * More concise packing
         */
        const g: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
            versionStore,
        }).g()

        const bundle2 = await g.pack(versionRoot)

        assert.deepStrictEqual(bundle, bundle2)
    })

    test('pack and restore single commit', async () => {
        const stream = await getStream(
            '/bibleapi/bibleapi-bibles-json/master/kjv.json'
        )
        const str = (await stream.text()).trim()
        const lines = str.split(/\r?\n/g)

        const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: MemoryBlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const g: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
            versionStore,
        }).g()

        const tx = await g.tx()

        const bible = await tx.addV(ObjectTypes.ROOT).next()

        let book: any, book_id: string
        let chapter: any, chapter_id: number
        let verse: any, verse_id: number

        for (const line of lines) {
            const entry = JSON.parse(line)
            if (book === undefined || book_id !== entry.book_id) {
                book = await tx
                    .addV(ObjectTypes.BOOK)
                    .property(KeyTypes.ID, entry.book_id, PropTypes.ANY)
                    .property(KeyTypes.NAME, entry.book_name, PropTypes.ANY)
                    .next()
                book_id = entry.book_id
                await tx.addE(RlshpTypes.book).from(bible).to(book).next()
            }
            if (chapter === undefined || chapter_id !== entry.chapter) {
                chapter = await tx
                    .addV(ObjectTypes.CHAPTER)
                    .property(KeyTypes.ID, entry.chapter, PropTypes.ANY)
                    .next()
                chapter_id = entry.chapter
                await tx.addE(RlshpTypes.chapter).from(book).to(chapter).next()
            }
            if (verse === undefined || verse_id !== entry.verse) {
                verse = await tx
                    .addV(ObjectTypes.VERSE)
                    .property(KeyTypes.ID, entry.verse, PropTypes.ANY)
                    .property(KeyTypes.TEXT, entry.text, PropTypes.ANY)
                    .next()
                verse_id = entry.verse
                await tx.addE(RlshpTypes.verse).from(chapter).to(verse).next()
            }
        }

        const commit = await tx.commit({})

        const { packCommit, restore } = graphPackerFactory(linkCodec)

        const bundle: Block = await packCommit(commit)

        const emptyStore: BlockStore = memoryBlockStoreFactory()

        const { root, index, blocks } = await restore(bundle.bytes, emptyStore)

        console.log(
            `Unpacked commit root: ${linkCodec.encodeString(
                root
            )}, block count: ${blocks.length}`
        )

        const g2: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: emptyStore,
            versionStore,
        }).g()

        {
            const { result, time } = await queryVerse(g2, 0, 'Rev', 22, 21)

            assert.strictEqual(
                result,
                'The grace of our Lord Jesus Christ be with you all. Amen.'
            )
            assert.ok(time < 3000) // 3sec
        }
    })

    test('pack and restore computed range', async () => {
        const cache = {}
        const ipfs = ipfsApi({ url: process.env.IPFS_API })
        const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
        const versionRoot = linkCodec.parseString(
            'bafkreiegljjns2rqb3z5mtdyvq2u6u2cvsahyez6bqsdjibo6737vrqhbi'
        )
        const versionStore: VersionStore = await versionStoreFactory({
            versionRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const { packComputed, unpack, restore } = graphPackerFactory(linkCodec)

        const bundle: Block = await packComputed(
            versionRoot,
            808775, // first vertex ref
            1, // vertex count
            blockStore,
            chunk,
            valueCodec
        )

        const emptyStore: MemoryBlockStore = memoryBlockStoreFactory()
        {
            const { root, index, blocks } = await restore(
                bundle.bytes,
                emptyStore
            )

            assert.strictEqual(
                root.toString(),
                'bafkreiegljjns2rqb3z5mtdyvq2u6u2cvsahyez6bqsdjibo6737vrqhbi'
            )
            assert.strictEqual(blocks.length, 9)

            const g2: ProtoGremlin = protoGremlinFactory({
                chunk,
                linkCodec,
                valueCodec,
                blockStore: emptyStore,
                versionStore,
            }).g()

            const { result, time } = await quickVerse(g2, 808800)

            assert.strictEqual(
                result,
                'The grace of our Lord Jesus Christ be with you all. Amen.'
            )
            assert.ok(time < 3) // 3ms
        }

        {
            const { root, index, blocks } = await unpack(bundle.bytes)

            assert.strictEqual(
                root.toString(),
                'bafkreiegljjns2rqb3z5mtdyvq2u6u2cvsahyez6bqsdjibo6737vrqhbi'
            )

            assert.strictEqual(blocks.length, 9)

            const otherStore: MemoryBlockStore = memoryBlockStoreFactory()

            for (const block of blocks) {
                await otherStore.put(block)
            }

            assert.deepStrictEqual(emptyStore.size(), otherStore.size())
        }

        {
            /**
             * More concise fragment packing
             */
            const g: ProtoGremlin = protoGremlinFactory({
                chunk,
                linkCodec,
                valueCodec,
                blockStore,
                versionStore,
            }).g()

            const bundle2 = await g.packFragment(808775, 1, versionRoot)

            assert.deepStrictEqual(bundle, bundle2)
        }
    })
})