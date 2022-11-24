import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { chunkerFactory } from '../chunking'
import { graphStore } from '../graph-store'
import { Graph } from '../graph'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import {
    BlockCodec,
    blockCodecFactory,
    LinkCodec,
    linkCodecFactory,
} from '../codecs'
import {
    EdgeDecoder,
    EdgeEncoder,
    OFFSET_INCREMENTS,
    PropDecoder,
    PropEncoder,
    VertexDecoder,
    VertexEncoder,
} from '../serde'
import { Edge, Prop, Status, Vertex } from '../types'
import { deltaFactory } from '../delta'

const { create, read, append, update, remove, readIndex } = chunkyStore()
const { chunk } = chunkerFactory(512, compute_chunks)

const linkCodec: LinkCodec = linkCodecFactory()
const blockCodec: BlockCodec = blockCodecFactory()
const blockStore: BlockStore = memoryBlockStoreFactory()

import * as assert from 'assert'
import { VersionStore, versionStoreFactory } from '../version-store'

describe('Delta for', function () {
    test('baseline changes vertices 1', async () => {
        await vertexTest(200, 20, 25, 50)
    })

    test('baseline changes vertices 2', async () => {
        await vertexTest(2000, 20, 25, 50)
    })

    test('baseline changes vertices 3', async () => {
        await vertexTest(1789, 13, 233, 123)
    })

    test('baseline changes vertices 4', async () => {
        await vertexTest(200, 185, 15, 15)
    })

    test('baseline changes vertices 5', async () => {
        await vertexTest(200, 183, 15, 15)
    })

    test('baseline changes vertices 6', async () => {
        await vertexTest(10, 0, 1, 1)
    })

    test('baseline changes vertices 7', async () => {
        await vertexTest(100, 0, 1, 0)
    })

    test('baseline changes vertices 8', async () => {
        await vertexTest(100, 0, 10, 0)
    })

    test('baseline changes edges 1', async () => {
        await edgeTest(200, 0, 15, 15)
    })

    test('baseline changes edges 2', async () => {
        await edgeTest(200, 185, 15, 15)
    })

    test('baseline changes edges 3', async () => {
        await edgeTest(5, 0, 1, 0)
    })

    test('baseline changes props 1', async () => {
        await propTest(200, 10, 10, 10)
    })

    test('baseline changes props 2', async () => {
        await propTest(1, 0, 1, 0)
    })

    test('baseline changes props 3', async () => {
        await propTest(1, 0, 1, 0)
    })
    test('baseline changes props 4', async () => {
        await propTest(200, 11, 1, 20)
    })

    test('baseline changes graph', async () => {
        const story: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const store = graphStore({ chunk, linkCodec, blockCodec, blockStore })

        // baseline

        const graph = new Graph(story, store)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex()
        const v2 = tx.addVertex()
        const v3 = tx.addVertex()

        await tx.addEdge(v1, v2)
        await tx.addEdge(v1, v3)

        await tx.addVertexProp(v2, 1, { hello: 'v2' })
        await tx.addVertexProp(v2, 1, { hello: 'v3' })

        const { root: baseRoot, index: baseIndex } = await tx.commit({})

        // current
        const story2: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const graph2 = new Graph(story2, store)

        const tx2 = graph2.tx()

        await tx2.start()

        const w1 = tx2.addVertex()
        const w2 = tx2.addVertex()
        const w3 = tx2.addVertex()
        const w4 = tx2.addVertex()

        await tx2.addEdge(w1, w2)
        await tx2.addEdge(w1, w3)
        await tx2.addEdge(w1, w4)

        await tx2.addVertexProp(w2, 1, { hello: 'v2' })
        await tx2.addVertexProp(w2, 1, { hello: 'v3' })

        const { root: currentRoot, index: currentIndex } = await tx2.commit({})

        const { baselineDelta } = deltaFactory({ linkCodec, blockCodec })

        const { vertices, edges } = await baselineDelta({
            baseRoot,
            baseIndex,
            baseStore: blockStore,
            currentRoot,
            currentIndex,
            currentStore: blockStore,
        })

        assert.strictEqual(vertices.added.size, 1)
        assert.strictEqual(vertices.updated.size, 0)
        assert.strictEqual(vertices.added.get(75).status, 1)

        assert.strictEqual(edges.added.size, 1)
        assert.strictEqual(edges.updated.size, 1)
        assert.strictEqual((edges.added.get(90) as Edge).sourcePrev, 45)
        assert.strictEqual((edges.updated.get(45) as Edge).sourcePrev, 0)
        assert.strictEqual((edges.updated.get(45) as Edge).sourceNext, 90)
    })
})

const vertexTest = async (
    size: number,
    updatePos: number,
    update: number,
    increment: number
) => {
    const { baseline, current, updated, added } = await vertexCollection(
        size,
        updatePos,
        update,
        increment
    )
    const { root: baseRoot, index: baseIndex, blocks: baseBlocks } = baseline
    const {
        root: currentRoot,
        index: currentIndex,
        blocks: currentBlocks,
    } = current
    const { baselineChangesRecords } = deltaFactory({ linkCodec, blockCodec })
    const recordsDecode = async (bytes: Uint8Array) =>
        new VertexDecoder(bytes).read()
    const { added: addedFound, updated: updatedFound } =
        await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.VERTEX_INCREMENT,
            baseRoot,
            baseIndex,
            baseStore: blockStore,
            currentRoot,
            currentIndex,
            currentStore: blockStore,
            recordsDecode,
        })

    assert.strictEqual(addedFound.size, added.length)
    assert.strictEqual(updatedFound.size, updated.length)

    assert.deepStrictEqual(Array.from(addedFound.values()), added)
    assert.deepStrictEqual(Array.from(updatedFound.values()), updated)
}

const edgeTest = async (
    size: number,
    updatePos: number,
    update: number,
    increment: number
) => {
    const { baseline, current, updated, added } = await edgeCollection(
        size,
        updatePos,
        update,
        increment
    )
    const { root: baseRoot, index: baseIndex, blocks: baseBlocks } = baseline
    const {
        root: currentRoot,
        index: currentIndex,
        blocks: currentBlocks,
    } = current
    const { baselineChangesRecords } = deltaFactory({ linkCodec, blockCodec })
    const recordsDecode = async (bytes: Uint8Array) =>
        new EdgeDecoder(bytes).read()
    const { added: addedFound, updated: updatedFound } =
        await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.EDGE_INCREMENT,
            baseRoot,
            baseIndex,
            baseStore: blockStore,
            currentRoot,
            currentIndex,
            currentStore: blockStore,
            recordsDecode,
        })

    assert.strictEqual(addedFound.size, added.length)
    assert.strictEqual(updatedFound.size, updated.length)

    assert.deepStrictEqual(Array.from(addedFound.values()), added)
    assert.deepStrictEqual(Array.from(updatedFound.values()), updated)
}

const propTest = async (
    size: number,
    updatePos: number,
    update: number,
    increment: number
) => {
    const { baseline, current, updated, added } = await propCollection(
        size,
        updatePos,
        update,
        increment
    )
    const { root: baseRoot, index: baseIndex, blocks: baseBlocks } = baseline
    const {
        root: currentRoot,
        index: currentIndex,
        blocks: currentBlocks,
    } = current
    const { baselineChangesRecords } = deltaFactory({ linkCodec, blockCodec })
    const recordsDecode = async (bytes: Uint8Array) =>
        new PropDecoder(
            bytes,
            linkCodec.decode,
            blockCodec.decode,
            blockStore.get
        ).read()
    const { added: addedFound, updated: updatedFound } =
        await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.PROP_INCREMENT,
            baseRoot,
            baseIndex,
            baseStore: blockStore,
            currentRoot,
            currentIndex,
            currentStore: blockStore,
            recordsDecode,
        })

    assert.strictEqual(addedFound.size, added.length)
    assert.strictEqual(updatedFound.size, updated.length)

    assert.deepStrictEqual(Array.from(addedFound.values()), added)
    assert.deepStrictEqual(Array.from(updatedFound.values()), updated)
}

const vertexCollection = async (
    size: number,
    updatePos: number,
    update: number,
    increment: number
) => {
    const vertices = []
    for (let i = 0; i < size; i++) {
        const vertex: Vertex = {
            status: Status.CREATED,
            offset: i * OFFSET_INCREMENTS.VERTEX_INCREMENT,
            type: 1,
        }
        vertices.push(vertex)
    }
    const baseline = await verticesCreate(vertices)

    const updated = []
    for (let i = 0; i < update; i++) {
        const pos = updatePos //Math.floor(size / 3)
        const vertex: Vertex = {
            status: Status.UPDATED,
            offset:
                pos * OFFSET_INCREMENTS.VERTEX_INCREMENT +
                i * OFFSET_INCREMENTS.VERTEX_INCREMENT,
            type: 999,
        }
        vertices[pos + i] = vertex
        updated.push(vertex)
    }
    const added = []
    for (let i = 0; i < increment; i++) {
        const vertex: Vertex = {
            status: Status.CREATED,
            offset:
                size * OFFSET_INCREMENTS.VERTEX_INCREMENT +
                i * OFFSET_INCREMENTS.VERTEX_INCREMENT,
            type: 777,
        }
        vertices.push(vertex)
        added.push(vertex)
    }
    const current = await verticesCreate(vertices)
    return { baseline, current, updated, added }
}

const edgeCollection = async (
    size: number,
    updatePos: number,
    update: number,
    increment: number
) => {
    const edges = []
    for (let i = 0; i < size; i++) {
        const edge: Edge = {
            status: Status.CREATED,
            offset: i * OFFSET_INCREMENTS.EDGE_INCREMENT,
            type: 1,
            source: 100 + i,
            target: 101 + i,
        }
        edges.push(edge)
    }
    const baseline = await edgesCreate(edges)

    const updated = []
    for (let i = 0; i < update; i++) {
        const pos = updatePos //Math.floor(size / 3)
        const edge: Edge = {
            status: Status.UPDATED,
            offset:
                pos * OFFSET_INCREMENTS.EDGE_INCREMENT +
                i * OFFSET_INCREMENTS.EDGE_INCREMENT,
            type: 999,
            source: 888,
            target: 3333,
        }
        edges[pos + i] = edge
        updated.push(edge)
    }
    const added = []
    for (let i = 0; i < increment; i++) {
        const edge: Edge = {
            status: Status.CREATED,
            offset:
                size * OFFSET_INCREMENTS.EDGE_INCREMENT +
                i * OFFSET_INCREMENTS.EDGE_INCREMENT,
            type: 777,
            source: 888,
            target: 3333,
        }
        edges.push(edge)
        added.push(edge)
    }
    const current = await edgesCreate(edges)

    return { baseline, current, updated, added }
}

const propCollection = async (
    size: number,
    updatePos: number,
    update: number,
    increment: number
) => {
    const props = []
    for (let i = 0; i < size; i++) {
        const prop: Prop = {
            status: Status.CREATED,
            offset: i * OFFSET_INCREMENTS.PROP_INCREMENT,
            type: 1,
            key: 1,
            value: '123',
        }
        props.push(prop)
    }
    const baseline = await propsCreate(props)

    const updated = []
    for (let i = 0; i < update; i++) {
        const pos = updatePos //Math.floor(size / 3)
        const prop: Prop = {
            status: Status.UPDATED,
            offset:
                pos * OFFSET_INCREMENTS.PROP_INCREMENT +
                i * OFFSET_INCREMENTS.PROP_INCREMENT,
            type: 999,
            key: 2,
            value: '123',
        }
        props[pos + i] = prop
        updated.push(prop)
    }
    const added = []
    for (let i = 0; i < increment; i++) {
        const prop: Prop = {
            status: Status.CREATED,
            offset:
                size * OFFSET_INCREMENTS.PROP_INCREMENT +
                i * OFFSET_INCREMENTS.PROP_INCREMENT,
            type: 777,
            key: 2,
            value: '123',
        }
        props.push(prop)
        added.push(prop)
    }
    const current = await propsCreate(props)

    return { baseline, current, updated, added }
}

const verticesCreate = async (array: Vertex[]) => {
    const buf = new VertexEncoder(array).write()
    const { root, index, blocks } = await create({
        buf,
        chunk,
        encode: linkCodec.encode,
    })
    for (const block of blocks) await blockStore.put(block)
    return { root, index, blocks }
}

const edgesCreate = async (array: Edge[]) => {
    const buf = new EdgeEncoder(array).write()
    const { root, index, blocks } = await create({
        buf,
        chunk,
        encode: linkCodec.encode,
    })
    for (const block of blocks) await blockStore.put(block)
    return { root, index, blocks }
}

const propsCreate = async (array: Prop[]) => {
    const buf = await new PropEncoder(
        array,
        blockCodec.encode,
        blockStore.put
    ).write()
    const { root, index, blocks } = await create({
        buf,
        chunk,
        encode: linkCodec.encode,
    })
    for (const block of blocks) await blockStore.put(block)
    return { root, index, blocks }
}
