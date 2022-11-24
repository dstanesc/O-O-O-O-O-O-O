import { protoGremlinFactory, ProtoGremlin } from '../api/proto-gremlin'
import { VertexRef } from '../types'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { BlockStore } from '../block-store'
import { create as ipfsApi } from 'ipfs-http-client'
import { blockStore as ipfsBlockStore } from '@dstanesc/ipfs-block-store'
import {
    LinkCodec,
    linkCodecFactory,
    BlockCodec,
    blockCodecFactory,
} from '../codecs'
import { eq } from '../ops'
import * as assert from 'assert'
import { VersionStore, versionStoreFactory } from '../version-store'

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

describe('IPFS block-store', function () {
    describe('Query ', function () {
        test('bible quick scan, retrieve verse - Gen 1 1', async () => {
            const cache = {}
            const ipfs = ipfsApi({ url: process.env.IPFS_API }) // eg. /ip4/192.168.1.231/tcp/5001
            const { chunk } = chunkerFactory(1024 * 16, compute_chunks)
            const linkCodec: LinkCodec = linkCodecFactory()
            const blockCodec: BlockCodec = blockCodecFactory()
            const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
            const versionRoot = linkCodec.parseString(
                'bafkreidhv2kilqj6eydivvatngsrtbcbifiij33tnq6zww7u34kit536q4'
            )
            const versionStore: VersionStore = await versionStoreFactory({
                versionRoot,
                chunk,
                linkCodec,
                blockCodec,
                blockStore,
            })

            const g: ProtoGremlin = protoGremlinFactory({
                chunk,
                linkCodec,
                blockCodec,
                blockStore,
                versionStore,
            }).g()

            const { result, time } = await queryVerse(g, 0, 'Gen', 1, 1)

            assert.strictEqual(
                result,
                'In the beginning God created the heaven and the earth.'
            )
            assert.ok(time < 300) // 300 ms
        })

        test('bible full scan, retrieve verse - Rev 22 21', async () => {
            const cache = {}
            const ipfs = ipfsApi({ url: process.env.IPFS_API }) // eg. /ip4/192.168.1.231/tcp/5001
            const { chunk } = chunkerFactory(1024 * 16, compute_chunks)
            const linkCodec: LinkCodec = linkCodecFactory()
            const blockCodec: BlockCodec = blockCodecFactory()
            const blockStore: BlockStore = ipfsBlockStore({ cache, ipfs })
            const versionRoot = linkCodec.parseString(
                'bafkreidhv2kilqj6eydivvatngsrtbcbifiij33tnq6zww7u34kit536q4'
            )
            const versionStore: VersionStore = await versionStoreFactory({
                versionRoot,
                chunk,
                linkCodec,
                blockCodec,
                blockStore,
            })

            const g: ProtoGremlin = protoGremlinFactory({
                chunk,
                linkCodec,
                blockCodec,
                blockStore,
                versionStore,
            }).g()

            const { result, time } = await queryVerse(g, 0, 'Rev', 22, 21)

            assert.strictEqual(
                result,
                'The grace of our Lord Jesus Christ be with you all. Amen.'
            )
            assert.ok(time < 3000) // 3sec
        })
    })
})
