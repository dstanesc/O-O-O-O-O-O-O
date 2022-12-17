import { protoGremlinFactory, ProtoGremlin } from '../api/proto-gremlin'
import * as assert from 'assert'
import bent from 'bent'
import { Prop, VertexRef } from '../types'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from '../block-store'
import {
    LinkCodec,
    linkCodecFactory,
    BlockCodec,
    blockCodecFactory,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { indexStoreFactory } from '../index-store-factory'
import { eq } from '../ops'
import { VersionStore, versionStoreFactory } from '../version-store'

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

enum PropTypes {
    ANY = 1,
}

enum KeyTypes {
    ID = 1,
    NAME = 2,
    TEXT = 3,
}

enum IndexTypes {
    BOOK_ID = 2,
    CHAPTER_ID = 3,
    VERSE_ID = 4,
}

describe('e2e ', function () {
    test('full bible, 7MB json, no index, load and navigate', async () => {
        const stream = await getStream(
            '/bibleapi/bibleapi-bibles-json/master/kjv.json'
        )
        const str = (await stream.text()).trim()
        const lines = str.split(/\r?\n/g)

        const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: MemoryBlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
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

        const { root, index, blocks } = await tx.commit({})

        const g2: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
            versionStore,
        }).g()

        blockStore.resetReads()

        const first = await queryVerse(g2, bible.offset, 'Gen', 1, 1)

        const readsFirst = blockStore.countReads()

        console.log(`Quick scan reads ${readsFirst}`)

        assert.equal(
            first,
            'In the beginning God created the heaven and the earth.'
        )

        blockStore.resetReads()

        const last = await queryVerse(g2, bible.offset, 'Rev', 22, 21)

        const readsLast = blockStore.countReads()

        console.log(`Full scan reads ${readsLast}`)

        assert.equal(
            last,
            'The grace of our Lord Jesus Christ be with you all. Amen.'
        )

        console.log(`BlockStore total size = ${blockStore.size()}`)

        console.log(
            `Root ${root.toString()} storeRoot=${versionStore
                .versionStoreRoot()
                .toString()}`
        )

        /**
         * Validate push
         */
        const blockStore2: MemoryBlockStore = memoryBlockStoreFactory()

        await blockStore.push(blockStore2)

        const versionStore2: VersionStore = await versionStoreFactory({
            readOnly: true,
            storeRoot: versionStore.versionStoreRoot(),
            versionRoot: undefined, // HEAD, can be omitted
            chunk,
            linkCodec,
            blockCodec,
            blockStore: blockStore2,
        })

        const g3: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: blockStore2,
            versionStore: versionStore2,
        }).g()

        const first2 = await queryVerse(g3, bible.offset, 'Gen', 1, 1)

        assert.equal(
            first2,
            'In the beginning God created the heaven and the earth.'
        )

        const last2 = await queryVerse(g3, bible.offset, 'Rev', 22, 21)

        assert.equal(
            last2,
            'The grace of our Lord Jesus Christ be with you all. Amen.'
        )

        console.log(`BlockStore2 total size = ${blockStore2.size()}`)

        assert.strictEqual(blockStore.size(), blockStore2.size())

        const books = await queryBooks(g3, bible.offset)

        console.log('Books', books)
    })

    test('full bible, 7MB json, no index, query first level', async () => {
        const stream = await getStream(
            '/bibleapi/bibleapi-bibles-json/master/kjv.json'
        )
        const str = (await stream.text()).trim()
        const lines = str.split(/\r?\n/g)

        const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: MemoryBlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
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

        const { root, index, blocks } = await tx.commit({})

        const books = await queryBooks(g, bible.offset)

        const expected = [
            'Gen',   'Exod',   'Lev',    'Num',   'Deut',
            'Josh',  'Judg',   'Ruth',   '1Sam',  '2Sam',
            '1Kgs',  '2Kgs',   '1Chr',   '2Chr',  'Ezra',
            'Neh',   'Esth',   'Job',    'Ps',    'Prov',
            'Eccl',  'Song',   'Isa',    'Jer',   'Lam',
            'Ezek',  'Dan',    'Hos',    'Joel',  'Amos',
            'Obad',  'Jona',   'Mic',    'Nah',   'Hab',
            'Zeph',  'Hag',    'Zech',   'Mal',   'Matt',
            'Mark',  'Luke',   'John',   'Acts',  'Rom',
            '1Cor',  '2Cor',   'Gal',    'Eph',   'Phil',
            'Col',   '1Thess', '2Thess', '1Tim',  '2Tim',
            'Titus', 'Phlm',   'Heb',    'Jas',   '1Pet',
            '2Pet',  '1John',  '2John',  '3John', 'Jude',
            'Rev'
          ]

        console.log('Books', books)

        assert.deepStrictEqual(books, expected)
    })

    test('full bible, 7MB json, KeyTypes.ID indexed, load and navigate', async () => {
        const stream = await getStream(
            '/bibleapi/bibleapi-bibles-json/master/kjv.json'
        )
        const str = (await stream.text()).trim()
        const lines = str.split(/\r?\n/g)

        const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const blockStore: MemoryBlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const indexStore = indexStoreFactory(blockStore)
        const g: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
            versionStore,
            indexStore,
        }).g()

        const tx = await g.tx()

        const bible = await tx.addV(ObjectTypes.ROOT).next()

        let book: any, book_id: string
        let chapter: any, chapter_id: number
        let verse: any, verse_id: number

        const books = []
        const chapters = []
        const verses = []

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
                books.push(book)
            }
            if (chapter === undefined || chapter_id !== entry.chapter) {
                chapter = await tx
                    .addV(ObjectTypes.CHAPTER)
                    .property(KeyTypes.ID, entry.chapter, PropTypes.ANY)
                    .next()
                chapter_id = entry.chapter
                await tx.addE(RlshpTypes.chapter).from(book).to(chapter).next()
                chapters.push(chapter)
            }
            if (verse === undefined || verse_id !== entry.verse) {
                verse = await tx
                    .addV(ObjectTypes.VERSE)
                    .property(KeyTypes.ID, entry.verse, PropTypes.ANY)
                    .property(KeyTypes.TEXT, entry.text, PropTypes.ANY)
                    .next()
                verse_id = entry.verse
                await tx.addE(RlshpTypes.verse).from(chapter).to(verse).next()
                verses.push(verse)
            }
        }

        for (const book of books) {
            await book.uniqueIndex(KeyTypes.ID)
        }
        for (const chapter of chapters) {
            await chapter.uniqueIndex(KeyTypes.ID)
        }
        for (const verse of verses) {
            await verse.uniqueIndex(KeyTypes.ID)
        }

        const { root, index, blocks } = await tx.commit({})

        const g2: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
            versionStore,
            indexStore,
        }).g()

        blockStore.resetReads()

        const first = await queryVerse(g2, bible.offset, 'Gen', 1, 1)

        const readsFirst = blockStore.countReads()

        console.log(`Indexed reads first ${readsFirst}`)

        assert.equal(
            first,
            'In the beginning God created the heaven and the earth.'
        )

        blockStore.resetReads()

        const last = await queryVerse(g2, bible.offset, 'Rev', 22, 21)

        const readsLast = blockStore.countReads()

        console.log(`Indexed reads last ${readsLast}`)

        assert.equal(
            last,
            'The grace of our Lord Jesus Christ be with you all. Amen.'
        )

        console.log(`BlockStore total size = ${blockStore.size()}`)

        console.log(
            `Root ${root.toString()} storeRoot=${versionStore
                .versionStoreRoot()
                .toString()}`
        )
    })
})

async function queryVerse(
    g: ProtoGremlin,
    rootOffset: VertexRef,
    book: string,
    chapter: number,
    verse: number
): Promise<string> {
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
    console.log(`Query Time ${endTime - startTime} ms`)

    return vr[0].value
}

async function queryBooks(g: ProtoGremlin, rootOffset: VertexRef) {
    const books = []
    for await (const result of g
        .V([rootOffset])
        .out(RlshpTypes.book)
        .hasType(ObjectTypes.BOOK)
        .values(KeyTypes.ID)
        .exec()) {
        books.push((result as Prop).value)
    }
    return books
}
