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

async function queryVerseFromBook(
    g: ProtoGremlin,
    bookOffset: VertexRef,
    chapter: number,
    verse: number
): Promise<{ result: string; time: number }> {
    const vr = []
    const startTime = new Date().getTime()
    for await (const result of g
        .V([bookOffset])
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
    test('pack and restore random blocks', async () => {
        const cache = {}
        const ipfs = ipfsApi({ url: process.env.IPFS_API })
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
        const bytes1 = await blockStore.get(
            'bafkreigu3fodriojosw3irmjz6v2znxli243n3xstcg575taxxjmctwu64'
        )
        const bytes2 = await blockStore.get(
            'bafkreihxekffa3coj4gpdmx2x5vteix5t5exrxfd7t4erqibj3mvw4rzm4'
        )
        const bytes3 = await blockStore.get(
            'bafkreib55oovsq4oxgiszoi43bsavb4raonyydlcyk6wx75hmb2ehkkikm'
        )
        const block1 = {
            bytes: bytes1,
            cid: linkCodec.parseString(
                'bafkreigu3fodriojosw3irmjz6v2znxli243n3xstcg575taxxjmctwu64'
            ),
        }
        const block2 = {
            bytes: bytes2,
            cid: linkCodec.parseString(
                'bafkreihxekffa3coj4gpdmx2x5vteix5t5exrxfd7t4erqibj3mvw4rzm4'
            ),
        }
        const block3 = {
            bytes: bytes3,
            cid: linkCodec.parseString(
                'bafkreib55oovsq4oxgiszoi43bsavb4raonyydlcyk6wx75hmb2ehkkikm'
            ),
        }
        const { packRandomBlocks, restoreRandomBlocks } =
            graphPackerFactory(linkCodec)
        const bundle: Block = await packRandomBlocks([block1, block2, block3])
        const memStore: BlockStore = memoryBlockStoreFactory()
        const restored: Block[] = await restoreRandomBlocks(
            bundle.bytes,
            memStore
        )
        expect(restored.length).toBe(3)
        expect(restored[0].cid.toString()).toBe(
            'bafkreigu3fodriojosw3irmjz6v2znxli243n3xstcg575taxxjmctwu64'
        )
        expect(restored[1].cid.toString()).toBe(
            'bafkreihxekffa3coj4gpdmx2x5vteix5t5exrxfd7t4erqibj3mvw4rzm4'
        )
        expect(restored[2].cid.toString()).toBe(
            'bafkreib55oovsq4oxgiszoi43bsavb4raonyydlcyk6wx75hmb2ehkkikm'
        )
    })

    test('pack and restore version store', async () => {
        const cache = {}
        const ipfs = ipfsApi({ url: process.env.IPFS_API })
        const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
        const versionStoreRoot = linkCodec.parseString(
            'bafkreidlnlkwfgzxw4rvwmqpg73snceph7gf3blui64ozwzxtjhyjkjh54'
        )

        /*
         * Packing and restoring
         */
        const { packVersionStore, restoreSingleIndex: restoreVersionStore } =
            graphPackerFactory(linkCodec)

        const bundle: Block = await packVersionStore(
            versionStoreRoot,
            blockStore,
            chunk,
            valueCodec
        )

        const memStore: BlockStore = memoryBlockStoreFactory()

        await restoreVersionStore(bundle.bytes, memStore)

        const versionStore: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memStore,
        })

        assert.equal(
            'bafkreiep4fey4tsqkt3ewglsifoxbdhc74xechba272sl5iwqgmcykezvi',
            versionStore.id()
        )
        assert.equal(
            'bafkreidlnlkwfgzxw4rvwmqpg73snceph7gf3blui64ozwzxtjhyjkjh54',
            versionStore.versionStoreRoot()
        )
        assert.equal(1, versionStore.log().length)
        assert.equal(
            'bafkreiegljjns2rqb3z5mtdyvq2u6u2cvsahyez6bqsdjibo6737vrqhbi',
            versionStore.currentRoot().toString()
        )
    })

    test('pack and restore full graph graphx', async () => {
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

        for (const block of blocks) {
            console.log(`Block: ${block.cid.toString()}`)
        }

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
            console.log(
                `Book:${entry.book_name}: ${book.offset} Chapter: ${entry.chapter}: ${chapter.offset} Verse: ${entry.verse}: ${verse.offset}`
            )
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

    test('pack and restore computed range, graph depth 1', async () => {
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
            1, // graph depth
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
            assert.ok(time < 10) // 10ms
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

            const bundle2 = await g.packFragment(808775, 1, 1, versionRoot)

            assert.deepStrictEqual(bundle, bundle2)
        }
    })

    test('pack and restore computed range, graph depth 3', async () => {
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

        const g: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
            versionStore,
        }).g()

        // Packing single (Revelation) book, w/ depth 3 (book, chapters, verses) - vertex offset is 798175
        const bundle = await g.packFragment(798175, 1, 3, versionRoot)

        const emptyStore: MemoryBlockStore = memoryBlockStoreFactory()

        const { restore } = graphPackerFactory(linkCodec)

        await restore(bundle.bytes, emptyStore)

        console.log(`Unpacked block count: ${emptyStore.size()}`)

        const g2: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: emptyStore,
            versionStore,
        }).g()

        const { result: r1, time } = await queryVerseFromBook(
            g2,
            798175,
            22,
            21
        )

        assert.strictEqual(
            r1,
            'The grace of our Lord Jesus Christ be with you all. Amen.'
        )

        const { result: r2 } = await queryVerseFromBook(g2, 798175, 22, 11)

        assert.strictEqual(
            r2,
            'He that is unjust, let him be unjust still: and he which is filthy, let him be filthy still: and he that is righteous, let him be righteous still: and he that is holy, let him be holy still.'
        )
    })
})
