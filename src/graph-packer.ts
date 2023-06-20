import { blockIndexFactory } from './block-index'
import bounds from 'binary-search-bounds'
import { BlockStore, memoryBlockStoreFactory } from './block-store'
import { LinkCodec, ValueCodec } from './codecs'
import {
    Block,
    Edge,
    Link,
    Prop,
    PropRecord,
    PropRef,
    RootIndex,
    ValueRef,
    Vertex,
} from './types'
import { CID } from 'multiformats/cid'
import { CarReader, CarWriter, CarBufferWriter } from '@ipld/car'
import { Writer } from '@ipld/car/dist/src/buffer-writer'
import { VersionStore, versionStoreFactory } from './version-store'
import { graphStoreFactory } from './graph-store'
import { Graph } from './graph'
import { OFFSET_INCREMENTS, PropRecordDecoder } from './serde'
import { chunkyStore } from '@dstanesc/store-chunky-bytes'

type Slice = {
    startOffset: number
    length: number
}

const { read } = chunkyStore()

/**
 * The GraphPacker is responsible for packing and unpacking complete graph versions or fragments thereof in a single large block.
 */
interface GraphPacker {
    /**
     * Packs a complete graph story into a single block.
     */
    packVersionStore: (
        versionStoreRoot: Link,
        fromStore: BlockStore,
        chunk: (buffer: Uint8Array) => Uint32Array,
        valueCodec: ValueCodec
    ) => Promise<Block>

    /**
     * Packs a complete graph version into a single block.
     */
    packGraph: (versionRoot: Link, fromStore: BlockStore) => Promise<Block>
    /**
     * Packs the commit results into a single block.
     */
    packCommit: ({
        root,
        index,
        blocks,
    }: {
        root: Link
        index: RootIndex
        blocks: Block[]
    }) => Promise<Block>
    /**
     * Packs a vertex data slice together with the derived edge and property data into a single block.
     */
    packComputed: (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number,
        fromStore: BlockStore,
        chunk: (buffer: Uint8Array) => Uint32Array,
        valueCodec: ValueCodec
    ) => Promise<Block>

    /**
     * Packs a vertex data slice together with the derived edge and property data into a single block. Graph API friendly.
     */
    packFragment: (
        graph: Graph,
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number,
        fromStore: BlockStore
    ) => Promise<Block>

    /**
     * Unpacks and restores packed graph data into a block store.
     */
    restore: (
        pack: Uint8Array,
        intoStore: BlockStore
    ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>

    /**
     * Unpacks and restores single index data into a block store.
     */
    restoreSingleIndex: (
        packBytes: Uint8Array,
        intoStore: BlockStore
    ) => Promise<{
        root: Link
        index: {
            startOffsets: Map<number, any>
            indexSize: number
            byteArraySize: number
        }
        blocks: Block[]
    }>

    /**
     * Unpacks graph packed data
     */
    unpack: (
        pack: Uint8Array
    ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>

    packRandomBlocks: (blocks: Block[]) => Promise<Block>

    restoreRandomBlocks: (
        pack: Uint8Array,
        intoStore: BlockStore
    ) => Promise<Block[]>
}

const graphPackerFactory = (linkCodec: LinkCodec): GraphPacker => {
    const computeSize = async (
        cids: Iterable<any>,
        fromStore: BlockStore
    ): Promise<number> => {
        let size = 0
        for (const cid of cids) {
            const bytes = await fromStore.get(cid)
            size += CarBufferWriter.blockLength({ cid, bytes })
        }
        return size
    }

    const writeMany = async (
        cids: Iterable<any>,
        fromStore: BlockStore,
        writer: Writer
    ): Promise<void> => {
        for (const cid of cids) {
            const bytes = await fromStore.get(cid)
            writer.write({ cid, bytes })
        }
    }

    const pushMany = async (
        cids: Iterable<any>,
        fromStore: BlockStore,
        blocks: Set<Block>
    ): Promise<void> => {
        for (const cid of cids) {
            const bytes = await fromStore.get(cid)
            blocks.add({ cid, bytes })
        }
    }

    const recordOffsets = (
        firstOffset: number,
        recordCount: number,
        recordIncrement: number
    ) => {
        const offsets = []
        for (let i = 0; i < recordCount; i++) {
            offsets.push(firstOffset + i * recordIncrement)
        }
        return offsets
    }

    const readPropValueRefs = async (
        propOffsetStart: number,
        propCount: number,
        index: RootIndex,
        fromStore: BlockStore
    ): Promise<ValueRef[]> => {
        const { propRoot, propIndex } = index
        const bytes = await read(
            propOffsetStart,
            propCount * OFFSET_INCREMENTS.PROP_INCREMENT,
            {
                root: propRoot,
                index: propIndex,
                decode: linkCodec.decode,
                get: fromStore.get,
            }
        )
        const propRecords: PropRecord[] = await new PropRecordDecoder(
            bytes
        ).read()
        return propRecords.map((propRecord) => propRecord.valueRef)
    }

    const findSlices = (valueRefs: ValueRef[]): Slice[] => {
        if (valueRefs.length === 0) return []
        let startOffset = valueRefs[0].ref
        let endOffset = startOffset + valueRefs[0].length
        const ranges: number[][] = []
        for (let i = 1; i < valueRefs.length; i++) {
            const valueRef = valueRefs[i]
            if (valueRef.ref === endOffset) {
                endOffset = valueRef.ref + valueRef.length
            } else {
                ranges.push([startOffset, endOffset])
                startOffset = valueRef.ref
                endOffset = startOffset + valueRef.length
            }
        }
        ranges.push([startOffset, endOffset])
        return ranges.map(([start, end]) => {
            return { startOffset: start, length: end - start }
        })
    }

    const identifyValueBlocks = async (
        startOffsets: Map<number, CID>,
        valueRefs: ValueRef[],
        valueArraySize: number
    ): Promise<CID[]> => {
        const slices = findSlices(valueRefs)
        const result: CID[] = []
        for (const slice of slices) {
            const blocks: CID[] = await identifyBlocks(
                startOffsets,
                slice,
                valueArraySize
            )
            result.push(...blocks)
        }
        return result
    }

    const relevantChunks = (
        startOffsetArray: any[],
        startOffset: number,
        endOffset: number,
        pad: number
    ): any[] => {
        return startOffsetArray.slice(
            bounds.le(startOffsetArray, startOffset),
            bounds.ge(startOffsetArray, endOffset) + pad
        )
    }

    const identifyBlocks = async (
        startOffsets: Map<number, CID>,
        slice: Slice,
        byteArraySize: number
    ): Promise<CID[]> => {
        const startOffset = slice.startOffset
        const endOffset = startOffset + slice.length
        if (startOffset > byteArraySize)
            throw new Error(
                `Start offset out of range ${startOffset} > buffer size ${byteArraySize}`
            )
        if (endOffset > byteArraySize)
            throw new Error(
                `End offset out of range ${endOffset} > buffer size ${byteArraySize}`
            )
        const startOffsetsIndexed = startOffsets
        const startOffsetArray = Array.from(startOffsetsIndexed.keys())
        const selectedChunks = relevantChunks(
            startOffsetArray,
            startOffset,
            endOffset,
            1
        )
        return selectedChunks.map((chunkOffset) =>
            startOffsetsIndexed.get(chunkOffset)
        )
    }

    const computeRecordsSize = async (
        startOffsets: Map<number, CID>,
        recordOffset: number,
        recordCount: number,
        recordSize: number,
        recordArraySize: number,
        blocksChecked: Set<string>,
        fromStore: BlockStore
    ): Promise<number> => {
        const offsets = recordOffsets(recordOffset, recordCount, recordSize)
        let size = 0
        for (const offset of offsets) {
            const cids = await identifyBlocks(
                startOffsets,
                { startOffset: offset, length: recordSize },
                recordArraySize
            )
            for (const cid of cids) {
                if (!blocksChecked.has(cid.toString())) {
                    size += await computeSize([cid], fromStore)
                    blocksChecked.add(cid.toString())
                }
            }
        }
        return size
    }

    const computeValueSize = async (
        startOffsets: Map<number, CID>,
        valueRefs: ValueRef[],
        valueArraySize: number,
        blocksChecked: Set<string>,
        fromStore: BlockStore
    ): Promise<number> => {
        const cids = await identifyValueBlocks(
            startOffsets,
            valueRefs,
            valueArraySize
        )
        let size = 0
        for (const cid of cids) {
            if (!blocksChecked.has(cid.toString())) {
                size += await computeSize([cid], fromStore)
                blocksChecked.add(cid.toString())
            }
        }
        return size
    }

    const pushRecords = async (
        startOffsets: Map<number, CID>,
        recordOffset: number,
        recordCount: number,
        recordSize: number,
        recordArraySize: number,
        blocksAdded: Set<string>,
        blocks: Set<Block>,
        fromStore: BlockStore
    ) => {
        const offsets = recordOffsets(recordOffset, recordCount, recordSize)
        for (const offset of offsets) {
            const cids = await identifyBlocks(
                startOffsets,
                { startOffset: offset, length: recordSize },
                recordArraySize
            )
            for (const cid of cids) {
                if (!blocksAdded.has(cid.toString())) {
                    await pushMany([cid], fromStore, blocks)
                    blocksAdded.add(cid.toString())
                }
            }
        }
    }

    const pushValues = async (
        startOffsets: Map<number, CID>,
        valueRefs: ValueRef[],
        valueArraySize: number,
        blocksAdded: Set<string>,
        blocks: Set<Block>,
        fromStore: BlockStore
    ) => {
        const cids = await identifyValueBlocks(
            startOffsets,
            valueRefs,
            valueArraySize
        )
        for (const cid of cids) {
            if (!blocksAdded.has(cid.toString())) {
                await pushMany([cid], fromStore, blocks)
                blocksAdded.add(cid.toString())
            }
        }
    }

    const packCommit = async ({
        root,
        index,
        blocks,
    }: {
        root: Link
        index: RootIndex
        blocks: Block[]
    }): Promise<Block> => {
        const { vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot } = index
        const roots = [vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot]
        const carRoots = [CID.asCID(root)]
        let bufferSize = CarBufferWriter.headerLength({ roots: carRoots })
        for (const block of blocks) {
            bufferSize += CarBufferWriter.blockLength({
                cid: CID.asCID(block.cid),
                bytes: block.bytes,
            })
        }
        const buffer = new Uint8Array(bufferSize)
        const writer: Writer = CarBufferWriter.createWriter(buffer, {
            roots: carRoots,
        })
        for (const block of blocks) {
            writer.write({
                cid: CID.asCID(block.cid),
                bytes: block.bytes,
            })
        }
        const bytes = writer.close()
        const cid = await linkCodec.encode(bytes)
        return { cid, bytes }
    }

    const packVersionStore = async (
        versionStoreRoot: Link,
        fromStore: BlockStore,
        chunk: (buffer: Uint8Array) => Uint32Array,
        valueCodec: ValueCodec
    ): Promise<Block> => {
        const versionStore: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: fromStore,
        })
        const { index: versionStoreIndex } = await versionStore.blocksExtract()
        const carRoots = [CID.asCID(versionStoreRoot)]
        let bufferSize = CarBufferWriter.headerLength({ roots: carRoots })
        bufferSize += await computeSize([versionStoreRoot], fromStore)
        const { startOffsets } = versionStoreIndex.indexStruct
        bufferSize += await computeSize(startOffsets.values(), fromStore)
        const buffer = new Uint8Array(bufferSize)
        const writer: Writer = CarBufferWriter.createWriter(buffer, {
            roots: carRoots,
        })
        await writeMany(carRoots, fromStore, writer)
        await writeMany(startOffsets.values(), fromStore, writer)
        const bytes = writer.close()
        const cid = await linkCodec.encode(bytes)
        return { cid, bytes }
    }

    const packGraph = async (
        versionRoot: Link,
        fromStore: BlockStore
    ): Promise<Block> => {
        const { buildRootIndex } = blockIndexFactory({
            linkCodec,
            blockStore: fromStore,
        })
        const { index } = await buildRootIndex(versionRoot)
        const {
            vertexIndex,
            edgeIndex,
            propIndex,
            valueIndex,
            indexIndex,
            vertexRoot,
            edgeRoot,
            propRoot,
            valueRoot,
            indexRoot,
        } = index
        const indexes = [
            vertexIndex,
            edgeIndex,
            propIndex,
            valueIndex,
            indexIndex,
        ]
        const roots = [vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot]
        const carRoots = [CID.asCID(versionRoot)]
        let bufferSize = CarBufferWriter.headerLength({ roots: carRoots })
        bufferSize += await computeSize([versionRoot], fromStore)
        bufferSize += await computeSize(roots, fromStore)
        for (const idx of indexes) {
            const { startOffsets } = idx.indexStruct
            bufferSize += await computeSize(startOffsets.values(), fromStore)
        }
        const buffer = new Uint8Array(bufferSize)
        const writer: Writer = CarBufferWriter.createWriter(buffer, {
            roots: carRoots,
        })
        await writeMany(carRoots, fromStore, writer)
        await writeMany(roots, fromStore, writer)
        for (const idx of indexes) {
            const { startOffsets } = idx.indexStruct
            await writeMany(startOffsets.values(), fromStore, writer)
        }
        const bytes = writer.close()
        const cid = await linkCodec.encode(bytes)
        return { cid, bytes }
    }

    const packRandom = async (
        index: RootIndex,
        vertices: Vertex[],
        edges: Edge[],
        props: Prop[],
        fromStore: BlockStore,
        blocksChecked: Set<string>,
        blocksAdded: Set<string>,
        blocks: Set<Block>
    ): Promise<number> => {
        const { vertexIndex, edgeIndex, propIndex, valueIndex } = index
        let bufferSize = 0
        for (const vertex of vertices) {
            bufferSize += await computeRecordsSize(
                vertexIndex.indexStruct.startOffsets,
                vertex.offset,
                1,
                OFFSET_INCREMENTS.VERTEX_INCREMENT,
                vertexIndex.indexStruct.byteArraySize,
                blocksChecked,
                fromStore
            )
        }
        for (const edge of edges) {
            bufferSize += await computeRecordsSize(
                edgeIndex.indexStruct.startOffsets,
                edge.offset,
                1,
                OFFSET_INCREMENTS.EDGE_INCREMENT,
                edgeIndex.indexStruct.byteArraySize,
                blocksChecked,
                fromStore
            )
        }
        for (const prop of props) {
            bufferSize += await computeRecordsSize(
                propIndex.indexStruct.startOffsets,
                prop.offset,
                1,
                OFFSET_INCREMENTS.PROP_INCREMENT,
                propIndex.indexStruct.byteArraySize,
                blocksChecked,
                fromStore
            )
        }
        for (const prop of props) {
            const valueRefs = await readPropValueRefs(
                prop.offset,
                1,
                index,
                fromStore
            )
            bufferSize += await computeValueSize(
                valueIndex.indexStruct.startOffsets,
                valueRefs,
                valueIndex.indexStruct.byteArraySize,
                blocksChecked,
                fromStore
            )
        }

        for (const vertex of vertices) {
            await pushRecords(
                vertexIndex.indexStruct.startOffsets,
                vertex.offset,
                1,
                OFFSET_INCREMENTS.VERTEX_INCREMENT,
                vertexIndex.indexStruct.byteArraySize,
                blocksAdded,
                blocks,
                fromStore
            )
        }
        for (const edge of edges) {
            await pushRecords(
                edgeIndex.indexStruct.startOffsets,
                edge.offset,
                1,
                OFFSET_INCREMENTS.EDGE_INCREMENT,
                edgeIndex.indexStruct.byteArraySize,
                blocksAdded,
                blocks,
                fromStore
            )
        }
        for (const prop of props) {
            await pushRecords(
                propIndex.indexStruct.startOffsets,
                prop.offset,
                1,
                OFFSET_INCREMENTS.PROP_INCREMENT,
                propIndex.indexStruct.byteArraySize,
                blocksAdded,
                blocks,
                fromStore
            )
        }
        for (const prop of props) {
            const valueRefs = await readPropValueRefs(
                prop.offset,
                1,
                index,
                fromStore
            )
            await pushValues(
                valueIndex.indexStruct.startOffsets,
                valueRefs,
                valueIndex.indexStruct.byteArraySize,
                blocksAdded,
                blocks,
                fromStore
            )
        }
        return bufferSize
    }

    const packRange = async (
        index: RootIndex,
        vertexOffsetStart: number,
        vertexCount: number,
        edgeOffsetStart: number,
        edgeCount: number,
        propOffsetStart: number,
        propCount: number,
        fromStore: BlockStore,
        blocksChecked: Set<string>,
        blocksAdded: Set<string>,
        blocks: Set<Block>
    ): Promise<number> => {
        const { vertexIndex, edgeIndex, propIndex, valueIndex } = index
        const valueRefs = await readPropValueRefs(
            propOffsetStart,
            propCount,
            index,
            fromStore
        )
        let bufferSize = 0
        bufferSize += await computeRecordsSize(
            vertexIndex.indexStruct.startOffsets,
            vertexOffsetStart,
            vertexCount,
            OFFSET_INCREMENTS.VERTEX_INCREMENT,
            vertexIndex.indexStruct.byteArraySize,
            blocksChecked,
            fromStore
        )
        bufferSize += await computeRecordsSize(
            edgeIndex.indexStruct.startOffsets,
            edgeOffsetStart,
            edgeCount,
            OFFSET_INCREMENTS.EDGE_INCREMENT,
            edgeIndex.indexStruct.byteArraySize,
            blocksChecked,
            fromStore
        )
        bufferSize += await computeRecordsSize(
            propIndex.indexStruct.startOffsets,
            propOffsetStart,
            propCount,
            OFFSET_INCREMENTS.PROP_INCREMENT,
            propIndex.indexStruct.byteArraySize,
            blocksChecked,
            fromStore
        )
        bufferSize += await computeValueSize(
            valueIndex.indexStruct.startOffsets,
            valueRefs,
            valueIndex.indexStruct.byteArraySize,
            blocksChecked,
            fromStore
        )
        await pushRecords(
            vertexIndex.indexStruct.startOffsets,
            vertexOffsetStart,
            vertexCount,
            OFFSET_INCREMENTS.VERTEX_INCREMENT,
            vertexIndex.indexStruct.byteArraySize,
            blocksAdded,
            blocks,
            fromStore
        )
        await pushRecords(
            edgeIndex.indexStruct.startOffsets,
            edgeOffsetStart,
            edgeCount,
            OFFSET_INCREMENTS.EDGE_INCREMENT,
            edgeIndex.indexStruct.byteArraySize,
            blocksAdded,
            blocks,
            fromStore
        )
        await pushRecords(
            propIndex.indexStruct.startOffsets,
            propOffsetStart,
            propCount,
            OFFSET_INCREMENTS.PROP_INCREMENT,
            propIndex.indexStruct.byteArraySize,
            blocksAdded,
            blocks,
            fromStore
        )
        await pushValues(
            valueIndex.indexStruct.startOffsets,
            valueRefs,
            valueIndex.indexStruct.byteArraySize,
            blocksAdded,
            blocks,
            fromStore
        )
        return bufferSize
    }

    const packComputed = async (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number,
        fromStore: BlockStore,
        chunk: (buffer: Uint8Array) => Uint32Array,
        valueCodec: ValueCodec
    ): Promise<Block> => {
        const versionStore: VersionStore = await versionStoreFactory({
            versionRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: fromStore,
        })
        const graphStore = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore: fromStore,
        })
        const graph = new Graph(versionStore, graphStore)
        return packFragment(
            graph,
            versionRoot,
            vertexOffsetStart,
            vertexCount,
            graphDepth,
            fromStore
        )
    }

    const packFragment = async (
        graph: Graph,
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number,
        fromStore: BlockStore
    ): Promise<Block> => {
        let vertices: Vertex[] = await graph.getVertexRange(
            vertexOffsetStart,
            vertexCount
        )
        const { buildRootIndex } = blockIndexFactory({
            linkCodec,
            blockStore: fromStore,
        })
        const { index } = await buildRootIndex(versionRoot)
        const { vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot } = index

        const blocksChecked = new Set<string>()
        const blocksAdded = new Set<string>()
        const blocks = new Set<Block>()

        const roots = [vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot]
        const carRoots = [CID.asCID(versionRoot)]
        let bufferSize = CarBufferWriter.headerLength({ roots: carRoots })
        bufferSize += await computeSize(carRoots, fromStore)
        bufferSize += await computeSize(roots, fromStore)

        let edgeOffsetStart = 0
        let edgeOffsetMax = 0
        let propOffsetStart = 0
        let propOffsetMax = 0
        const targetVertices = new Set<Vertex>()
        const targetEdges = new Set<Edge>()
        const targetProps = new Set<Prop>()
        for (const vertex of vertices) {
            const edges = await graph.getVertexEdges(vertex)
            for (const edge of edges) {
                targetEdges.add(edge)
                if (edge.offset < edgeOffsetStart || edgeOffsetStart === 0) {
                    edgeOffsetStart = edge.offset
                }
                if (edge.offset > edgeOffsetMax) {
                    edgeOffsetMax = edge.offset
                }
                const props = await graph.getEdgeProps(edge)
                for (const prop of props) {
                    if (
                        prop.offset < propOffsetStart ||
                        propOffsetStart === 0
                    ) {
                        propOffsetStart = prop.offset
                    }
                    if (prop.offset > propOffsetMax) {
                        propOffsetMax = prop.offset
                    }
                }
                const targetVertex: Vertex = await graph.getVertex(edge.target)
                targetVertices.add(targetVertex)
            }
            const props = await graph.getVertexProps(vertex)
            for (const prop of props) {
                targetProps.add(prop)
                if (prop.offset < propOffsetStart || propOffsetStart === 0) {
                    propOffsetStart = prop.offset
                }
                if (prop.offset > propOffsetMax) {
                    propOffsetMax = prop.offset
                }
            }
        }
        const edgeCount =
            (edgeOffsetMax - edgeOffsetStart) / OFFSET_INCREMENTS.EDGE_INCREMENT
        const propCount =
            (propOffsetMax - propOffsetStart) / OFFSET_INCREMENTS.PROP_INCREMENT
        bufferSize += await packRange(
            index,
            vertexOffsetStart,
            vertexCount,
            edgeOffsetStart,
            edgeCount,
            propOffsetStart,
            propCount,
            fromStore,
            blocksChecked,
            blocksAdded,
            blocks
        )
        let levelVertices: Vertex[] = Array.from(targetVertices)
        let levelEdges: Edge[] = Array.from(targetEdges)
        let levelProps: Prop[] = Array.from(targetProps)
        for (let i = 1; i < graphDepth; i++) {
            bufferSize += await packRandom(
                index,
                levelVertices,
                levelEdges,
                levelProps,
                fromStore,
                blocksChecked,
                blocksAdded,
                blocks
            )
            targetVertices.clear()
            targetEdges.clear()
            targetProps.clear()
            for (const vertex of levelVertices) {
                const edges = await graph.getVertexEdges(vertex)
                for (const edge of edges) {
                    targetEdges.add(edge)
                    const targetVertex: Vertex = await graph.getVertex(
                        edge.target
                    )
                    targetVertices.add(targetVertex)
                    const props = await graph.getEdgeProps(edge)
                    for (const prop of props) {
                        targetProps.add(prop)
                    }
                }
                const props = await graph.getVertexProps(vertex)
                for (const prop of props) {
                    targetProps.add(prop)
                }
            }
            levelVertices = Array.from(targetVertices)
            levelEdges = Array.from(targetEdges)
            levelProps = Array.from(targetProps)
        }
        await pushMany(carRoots, fromStore, blocks)
        await pushMany(roots, fromStore, blocks)
        const buffer = new Uint8Array(bufferSize)
        const writer: Writer = CarBufferWriter.createWriter(buffer, {
            roots: carRoots,
        })
        for (const block of blocks) {
            writer.write({
                cid: CID.asCID(block.cid),
                bytes: block.bytes,
            })
        }
        const bytes = writer.close()
        const cid = await linkCodec.encode(bytes)
        return { cid, bytes }
    }
    const unpack = async (
        packBytes: Uint8Array
    ): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> => {
        const blockStore: BlockStore = memoryBlockStoreFactory()
        return await restore(packBytes, blockStore)
    }

    const restore = async (
        packBytes: Uint8Array,
        intoStore: BlockStore
    ): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> => {
        const reader = await CarReader.fromBytes(packBytes)
        const [root] = await reader.getRoots()
        const blocks: Block[] = []
        for await (const { cid, bytes } of reader.blocks()) {
            blocks.push({ cid, bytes })
            await intoStore.put({ cid, bytes })
        }
        const { buildRootIndex } = blockIndexFactory({
            linkCodec,
            blockStore: intoStore,
        })
        const { index } = await buildRootIndex(root)

        return { root, index, blocks }
    }

    const restoreSingleIndex = async (
        packBytes: Uint8Array,
        intoStore: BlockStore
    ): Promise<{
        root: Link
        index: {
            startOffsets: Map<number, any>
            indexSize: number
            byteArraySize: number
        }
        blocks: Block[]
    }> => {
        const reader = await CarReader.fromBytes(packBytes)
        const [root] = await reader.getRoots()
        const blocks: Block[] = []
        for await (const { cid, bytes } of reader.blocks()) {
            blocks.push({ cid, bytes })
            await intoStore.put({ cid, bytes })
        }
        const { buildChunkyIndex } = blockIndexFactory({
            linkCodec,
            blockStore: intoStore,
        })
        const { indexStruct } = await buildChunkyIndex(root)

        return { root, index: indexStruct, blocks }
    }

    const packRandomBlocks = async (blocks: Block[]): Promise<Block> => {
        let bufferSize = 0
        bufferSize += CarBufferWriter.headerLength({ roots: [] })
        for (const block of blocks) {
            bufferSize += CarBufferWriter.blockLength({
                cid: CID.asCID(block.cid),
                bytes: block.bytes,
            })
        }
        const buffer = new Uint8Array(bufferSize)
        const writer: Writer = CarBufferWriter.createWriter(buffer)
        for (const block of blocks) {
            writer.write({
                cid: CID.asCID(block.cid),
                bytes: block.bytes,
            })
        }
        const bytes = writer.close()
        const cid = await linkCodec.encode(bytes)
        return { cid, bytes }
    }

    const restoreRandomBlocks = async (
        packBytes: Uint8Array,
        intoStore: BlockStore
    ): Promise<Block[]> => {
        const reader = await CarReader.fromBytes(packBytes)
        const blocks: Block[] = []
        for await (const { cid, bytes } of reader.blocks()) {
            blocks.push({ cid, bytes })
            await intoStore.put({ cid, bytes })
        }
        return blocks
    }

    return {
        packRandomBlocks,
        packVersionStore,
        packGraph,
        packCommit,
        packComputed,
        packFragment,
        restore,
        restoreSingleIndex,
        restoreRandomBlocks,
        unpack,
    }
}

export { graphPackerFactory, GraphPacker }
