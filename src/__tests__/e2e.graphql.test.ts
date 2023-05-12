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
import { create as ipfsApi } from 'ipfs-http-client'
import { blockStore as ipfsBlockStore } from '@dstanesc/ipfs-block-store'
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
import {
    graphql,
    GraphQLObjectType,
    GraphQLString,
    GraphQLSchema,
    GraphQLInt,
} from 'graphql'

const BibleQuery = new GraphQLObjectType({
    name: 'BibleQuery',
    fields: {
        anyVerse: {
            type: GraphQLString,
            args: {
                book: { type: GraphQLString },
                chapter: { type: GraphQLInt },
                verse: { type: GraphQLInt },
            },
            resolve(parent, args, context) {
                const { book, chapter, verse } = args
                return queryVerse(
                    context.g2,
                    context.rootOffset,
                    book,
                    chapter,
                    verse
                )
            },
        },
    },
})

const schema = new GraphQLSchema({
    query: BibleQuery,
})

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

describe('e2e ', function () {
    test('simple graphql query wrapper', async () => {
        const cache = {}
        const ipfs = ipfsApi({ url: process.env.IPFS_API }) // eg. /ip4/192.168.1.231/tcp/5001
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

        const g2: ProtoGremlin = protoGremlinFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
            versionStore,
        }).g()

        const query = graphqlQuery('Gen', 1, 1)
        const firstResult = await graphql({
            schema,
            source: query,
            contextValue: { g2, rootOffset: 0 },
        })
        const first = firstResult.data.anyVerse

        assert.equal(
            first,
            'In the beginning God created the heaven and the earth.'
        )
    })
})
function graphqlQuery(book: string, chapter: number, verse: number) {
    return `
            query {
                anyVerse(book: "${book}", chapter: ${chapter}, verse: ${verse})
            }
          `
}
