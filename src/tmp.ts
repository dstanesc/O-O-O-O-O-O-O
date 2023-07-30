import { protoGremlinFactory, ProtoGremlin } from './api/proto-gremlin'
import bent from 'bent'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from './chunking'
import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from './block-store'
import {
    LinkCodec,
    linkCodecFactory,
    ValueCodec,
    valueCodecFactory,
} from './codecs'

import { create as ipfsApi } from 'ipfs-http-client'
import { blockStore as ipfsBlockStore } from '@dstanesc/ipfs-block-store'
import {
    blockStore as httpBlockStore,
    resolvers,
} from '@dstanesc/http-block-store'

import { eq } from './ops'
import { Link, VertexRef } from './types'
import { VersionStore, versionStoreFactory } from './version-store'

import AWS, { S3 } from 'aws-sdk'
import { blockStore as awsBlockStore } from '@dstanesc/s3-block-store'

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

const stream = await getStream( '/dstanesc/bibleapi-bibles-json/legacy/kjv.json')
const str = (await stream.text()).trim()
const lines = str.split(/\r?\n/g)

async function publish() {
    const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
    const linkCodec: LinkCodec = linkCodecFactory()
    const valueCodec: ValueCodec = valueCodecFactory()
    const cache = {}
    const ipfs = ipfsApi({ url: process.env.IPFS_API }) // eg. /ip4/192.168.1.231/tcp/5001
    const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
    // const awsRegion = process.env.AWS_REGION
    // const awsBucket = process.env.AWS_BUCKET_NAME
    // AWS.config.update({region: awsRegion})
    // const s3: S3 = new AWS.S3()
    // const blockStore: BlockStore = awsBlockStore({ cache, s3, bucket: awsBucket })
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

    console.log(
        `Bible written root=${root.toString()}, storeRoot=${versionStore
            .versionStoreRoot()
            .toString()}`
    )

    console.log('Version store identity', versionStore.id())
    console.log('Version store log', versionStore.log())
}

async function query() {
    const cache = {}
    const { chunk } = chunkerFactory(1024 * 48, compute_chunks)
    const linkCodec: LinkCodec = linkCodecFactory()
    const valueCodec: ValueCodec = valueCodecFactory()
    // const resolver = (cid: any) =>
    //     `http://192.168.1.205:8080/ipfs/${cid.toString()}`
    // const blockStore = httpBlockStore({ cache, resolver })
    // const awsRegion = process.env.AWS_REGION
    // const awsBucket = process.env.AWS_BUCKET_NAME
    // AWS.config.update({region: awsRegion})
    // const s3: S3 = new AWS.S3()
    // const blockStore: BlockStore = awsBlockStore({ cache, s3, bucket: awsBucket })
    const ipfs = ipfsApi({ url: process.env.IPFS_API }) // eg. /ip4/192.168.1.231/tcp/5001
    const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
    const versionRoot = linkCodec.parseString(
        'bafkreiegljjns2rqb3z5mtdyvq2u6u2cvsahyez6bqsdjibo6737vrqhbi'
    )
    const versionStore: VersionStore = await versionStoreFactory({
        readOnly: true,
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

    const { result: r1, time: t1 } = await queryVerse(g, 0, 'Rev', 22, 21)

    console.log(r1, t1, 'ms')

    const { result: r2, time: t2 } = await queryVerse(g, 0, 'Gen', 1, 1)

    console.log(r2, t2, 'ms')

    const { result: r3, time: t3 } = await queryVerse(g, 0, '1John', 5, 5)

    console.log(r3, t3, 'ms')
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
    return { result: vr[0].value, time }
}

await publish()
// await query()
