import {
    linkCodecFactory,
    blockCodecFactory,
    LinkCodec,
    BlockCodec,
    valueCodecFactory,
    ValueCodec,
} from '../codecs'
import { memoryBlockStoreFactory } from '../block-store'
import { Version, Edge, Index, Prop, Status, Vertex, ValueRef } from '../types'
import {
    EdgeDecoder,
    EdgeEncoder,
    VersionDecoder,
    VersionEncoder,
    IndexDecoder,
    IndexEncoder,
    PropDecoder,
    PropEncoder,
    VertexDecoder,
    VertexEncoder,
    PropValueEncoder,
    PropValueDecoder,
} from '../serde'
import * as assert from 'assert'
import { CID } from 'multiformats/cid'

const { encode: linkEncode, decode: linkDecode }: LinkCodec = linkCodecFactory()
const { encode: blockEncode, decode: blockDecode }: BlockCodec =
    blockCodecFactory()
const { encode: valueEncode, decode: valueDecode }: ValueCodec =
    valueCodecFactory()
const { put: blockPut, get: blockGet } = memoryBlockStoreFactory()

describe('Serde validation with', function () {
    test('basic write and read, vertices', async () => {
        const v1: Vertex = { status: Status.CREATED, offset: 0 }
        const v2: Vertex = {
            status: Status.UPDATED,
            offset: 25,
            type: 77,
            nextProp: 92,
            nextEdge: 90,
        }
        const v3: Vertex = { status: Status.DELETED, offset: 50 }
        const bytes = new VertexEncoder([v1, v2, v3]).write()
        const vertices = new VertexDecoder(bytes).read()
        const v3p: Vertex = { status: Status.DELETED, offset: 50 }
        assert.deepEqual([v1, v2, v3p], vertices)
    })

    test('basic write and read, edges', async () => {
        const e1: Edge = {
            status: Status.CREATED,
            offset: 0,
            source: 0,
            target: 20,
            sourceNext: 10,
            sourcePrev: 25,
            targetNext: 99,
            targetPrev: 77,
            nextProp: 25,
        }
        const e2: Edge = {
            status: Status.CREATED,
            offset: 45,
            type: 22,
            source: 0,
            target: 20,
            targetNext: 99,
            targetPrev: 77,
            nextProp: 25,
        }
        const e3: Edge = {
            status: Status.CREATED,
            offset: 90,
            type: 22,
            source: 0,
            target: 2000,
        }
        const bytes = new EdgeEncoder([e1, e2, e3]).write()
        const e3p: Edge = {
            status: Status.CREATED,
            offset: 90,
            type: 22,
            source: 0,
            target: 2000,
        }
        const edges = new EdgeDecoder(bytes).read()
        assert.deepEqual([e1, e2, e3p], edges)
    })

    test('basic write and read, props', async () => {
        const p1: Prop = {
            status: Status.CREATED,
            offset: 0,
            key: 1,
            value: { some: 'text' },
            nextProp: 111,
        }
        const p2: Prop = {
            status: Status.CREATED,
            offset: 56,
            type: 121,
            key: 1,
            value: 222,
        }
        const p3: Prop = {
            status: Status.CREATED,
            offset: 112,
            type: 128,
            key: 7,
            value: 'Hello World',
            nextProp: 888,
        }
        const { refs, buf } = new PropValueEncoder(
            0,
            [p1, p2, p3],
            valueEncode
        ).write()

        const bytes = await new PropEncoder([p1, p2, p3], refs).write()
        const props = await new PropDecoder(bytes, (valueRef: ValueRef) => {
            const decoder = new PropValueDecoder(buf, valueDecode)
            decoder.skipBytes(valueRef.ref)
            return decoder.readValue(valueRef)
        }).read()

        assert.deepEqual([p1, p2, p3], props)
    })

    test('basic write and read, indices', async () => {
        const i1: Index = {
            status: Status.CREATED,
            offset: 0,
            key: 101,
            value: await linkEncode(new TextEncoder().encode('Some text')),
            nextIndex: 111,
        }
        const i2: Index = {
            status: Status.CREATED,
            offset: 56,
            type: 121,
            key: 0,
            value: await linkEncode(
                new TextEncoder().encode('Some other text')
            ),
        }
        const i3: Index = {
            status: Status.CREATED,
            offset: 112,
            type: 128,
            key: 1,
            value: await linkEncode(new TextEncoder().encode('Some more text')),
            nextIndex: 888,
        }
        const bytes = await new IndexEncoder([i1, i2, i3]).write()
        const indices = await new IndexDecoder(bytes, linkDecode).read()
        assert.deepEqual([i1, i2, i3], indices)
    })

    test('basic write and read, versions', async () => {
        const v1: Version = {
            root: CID.parse(
                'bafkreidhv2kilqj6eydivvatngsrtbcbifiij33tnq6zww7u34kit536q4'
            ),
            comment: 'First commit',
            tags: ['tag0', 'tag1'],
        }

        const v2: Version = {
            root: CID.parse(
                'bafkreicklvs2aaeqfvs6f2pgcki2gont35chka2loq7mlah7yu4tj6bsvy'
            ),
            parent: CID.parse(
                'bafkreidhv2kilqj6eydivvatngsrtbcbifiij33tnq6zww7u34kit536q4'
            ),
            comment: 'Second commit',
            tags: ['tag2'],
        }

        const v3: Version = {
            root: CID.parse(
                'bafkreiguifcsbkfb7jlxr7inlp5fqukmve7mv2po234jj73pp7nryvbcxu'
            ),
            parent: CID.parse(
                'bafkreicklvs2aaeqfvs6f2pgcki2gont35chka2loq7mlah7yu4tj6bsvy'
            ),
            comment: 'Third commit',
            tags: ['tag3'],
        }

        const bytes = await new VersionEncoder(
            CID.parse(
                'bafkreibygummtcvcgmld7re3s4kjfoaf4z3zgxsdsqdmh3baom4suvgnem'
            ),
            [v1, v2, v3],
            blockEncode,
            blockPut
        ).write()

        const { id, versions } = await new VersionDecoder(
            bytes,
            linkDecode,
            blockDecode,
            blockGet
        ).read()
        assert.deepEqual([v1, v2, v3], versions)
    })
})
