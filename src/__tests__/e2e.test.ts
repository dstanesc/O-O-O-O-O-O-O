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
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { indexStoreFactory } from '../index-store-factory'
import { eq } from '../ops'
import { VersionStore, versionStoreFactory } from '../version-store'
import { navigateVertices, PathElemType, RequestBuilder } from '../navigate'
import { Graph } from '../graph'

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
            '/dstanesc/bibleapi-bibles-json/legacy/kjv.json'
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
            valueCodec,
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

        assert.strictEqual(blockStore.size(), blockStore2.size())

        const books = await queryBooks(g3, bible.offset)
    })

    test('full bible, 7MB json, no index, query first level', async () => {
        const stream = await getStream(
            '/dstanesc/bibleapi-bibles-json/legacy/kjv.json'
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

        const { root, index, blocks } = await tx.commit({})

        const books = await queryBooks(g, bible.offset)

        const expected = [
            'Gen',
            'Exod',
            'Lev',
            'Num',
            'Deut',
            'Josh',
            'Judg',
            'Ruth',
            '1Sam',
            '2Sam',
            '1Kgs',
            '2Kgs',
            '1Chr',
            '2Chr',
            'Ezra',
            'Neh',
            'Esth',
            'Job',
            'Ps',
            'Prov',
            'Eccl',
            'Song',
            'Isa',
            'Jer',
            'Lam',
            'Ezek',
            'Dan',
            'Hos',
            'Joel',
            'Amos',
            'Obad',
            'Jona',
            'Mic',
            'Nah',
            'Hab',
            'Zeph',
            'Hag',
            'Zech',
            'Mal',
            'Matt',
            'Mark',
            'Luke',
            'John',
            'Acts',
            'Rom',
            '1Cor',
            '2Cor',
            'Gal',
            'Eph',
            'Phil',
            'Col',
            '1Thess',
            '2Thess',
            '1Tim',
            '2Tim',
            'Titus',
            'Phlm',
            'Heb',
            'Jas',
            '1Pet',
            '2Pet',
            '1John',
            '2John',
            '3John',
            'Jude',
            'Rev',
        ]
        assert.deepStrictEqual(books, expected)
    })

    test('full bible, 7MB json, KeyTypes.ID indexed, load and navigate', async () => {
        const stream = await getStream(
            '/dstanesc/bibleapi-bibles-json/legacy/kjv.json'
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

    test('full bible, 7MB json, no index, template based retrieval', async () => {
        const stream = await getStream(
            '/dstanesc/bibleapi-bibles-json/legacy/kjv.json'
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

        const { root, index, blocks } = await tx.commit({})

        const books = await queryBooksTemplateGremlin(
            g,
            bible.offset,
            BOOKS_TEMPLATE_1
        )

        assert.strictEqual(books.length, 66)
        assert.strictEqual(books[0].id, 'Gen')
        assert.strictEqual(books[0].name, 'Genesis')
        assert.deepStrictEqual(books[0].chapters, [
            { id: 1 },
            { id: 2 },
            { id: 3 },
            { id: 4 },
            { id: 5 },
            { id: 6 },
            { id: 7 },
            { id: 8 },
            { id: 9 },
            { id: 10 },
            { id: 11 },
            { id: 12 },
            { id: 13 },
            { id: 14 },
            { id: 15 },
            { id: 16 },
            { id: 17 },
            { id: 18 },
            { id: 19 },
            { id: 20 },
            { id: 21 },
            { id: 22 },
            { id: 23 },
            { id: 24 },
            { id: 25 },
            { id: 26 },
            { id: 27 },
            { id: 28 },
            { id: 29 },
            { id: 30 },
            { id: 31 },
            { id: 32 },
            { id: 33 },
            { id: 34 },
            { id: 35 },
            { id: 36 },
            { id: 37 },
            { id: 38 },
            { id: 39 },
            { id: 40 },
            { id: 41 },
            { id: 42 },
            { id: 43 },
            { id: 44 },
            { id: 45 },
            { id: 46 },
            { id: 47 },
            { id: 48 },
            { id: 49 },
            { id: 50 },
        ])

        assert.strictEqual(books[1].id, 'Exod')
        assert.strictEqual(books[1].name, 'Exodus')
        assert.deepStrictEqual(books[1].chapters, [
            { id: 1 },
            { id: 2 },
            { id: 3 },
            { id: 4 },
            { id: 5 },
            { id: 6 },
            { id: 7 },
            { id: 8 },
            { id: 9 },
            { id: 10 },
            { id: 11 },
            { id: 12 },
            { id: 13 },
            { id: 14 },
            { id: 15 },
            { id: 16 },
            { id: 17 },
            { id: 18 },
            { id: 19 },
            { id: 20 },
            { id: 21 },
            { id: 22 },
            { id: 23 },
            { id: 24 },
            { id: 25 },
            { id: 26 },
            { id: 27 },
            { id: 28 },
            { id: 29 },
            { id: 30 },
            { id: 31 },
            { id: 32 },
            { id: 33 },
            { id: 34 },
            { id: 35 },
            { id: 36 },
            { id: 37 },
            { id: 38 },
            { id: 39 },
            { id: 40 },
        ])

        const books2 = await queryBooksTemplateGremlin(
            g,
            bible.offset,
            BOOKS_TEMPLATE_2
        )

        assert.strictEqual(books2.length, 66)

        assert.strictEqual(books2[0].id, 'Gen')
        assert.strictEqual(books2[0].name, 'Genesis')
        assert.deepStrictEqual(
            books2[0].chapters,
            [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
                35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
            ]
        )

        assert.deepStrictEqual(books2[65], {
            id: 'Rev',
            name: 'Revelation',
            chapters: [
                2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 21, 22,
            ],
        })

        const books3 = await queryBooksTemplateNative(
            g.graph,
            bible.offset,
            BOOKS_TEMPLATE_2
        )

        assert.strictEqual(books3.length, 66)

        assert.deepStrictEqual(books3[0], {
            id: 'Gen',
            name: 'Genesis',
            chapters: [
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
                19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
                35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
            ],
        })

        assert.deepStrictEqual(books3[65], {
            id: 'Rev',
            name: 'Revelation',
            chapters: [
                2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                20, 21, 22,
            ],
        })
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

const BOOKS_TEMPLATE_1 = {
    id: {
        $elemType: PathElemType.EXTRACT,
        $type: KeyTypes.ID,
    },
    name: {
        $elemType: PathElemType.EXTRACT,
        $type: KeyTypes.NAME,
    },
    chapters: {
        $elemType: PathElemType.EDGE,
        $type: RlshpTypes.chapter,
        id: {
            $elemType: PathElemType.EXTRACT,
            $type: KeyTypes.ID,
        },
    },
}

const BOOKS_TEMPLATE_2 = {
    id: {
        $elemType: PathElemType.EXTRACT,
        $type: KeyTypes.ID,
    },
    name: {
        $elemType: PathElemType.EXTRACT,
        $type: KeyTypes.NAME,
    },
    chapters: {
        $elemType: PathElemType.EDGE,
        $type: RlshpTypes.chapter,
        $value: {
            $elemType: PathElemType.EXTRACT,
            $type: KeyTypes.ID,
        },
    },
}

async function queryBooksTemplateGremlin(
    g: ProtoGremlin,
    rootOffset: VertexRef,
    template: any
): Promise<any[]> {
    const vr = []
    const startTime = new Date().getTime()

    for await (const result of g
        .V([rootOffset])
        .out(RlshpTypes.book)
        .hasType(ObjectTypes.BOOK)
        .template(template)
        .maxResults(100)
        .exec()) {
        vr.push(result)
    }
    const endTime = new Date().getTime()
    return vr
}

const queryBooksTemplateNative = async (
    graph: Graph,
    rootOffset: VertexRef,
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
    for await (const result of navigateVertices(graph, [rootOffset], request)) {
        vr.push(result)
    }
    return vr
}
