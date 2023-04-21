import { blockIndexFactory } from './block-index'
import bounds from 'binary-search-bounds'
import { BlockStore, memoryBlockStoreFactory } from './block-store'
import { LinkCodec, ValueCodec } from './codecs'
import {
    Block,
    Link,
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
     * Packs selected slices of vertex, edge and property data into a single block.
     */
    packSelected: (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        edgeOffsetStart: number,
        edgeCount: number,
        propOffsetStart: number,
        propCount: number,
        fromStore: BlockStore
    ) => Promise<Block>

    /**
     * Packs a vertex data slice together with the derived edge and property data into a single block.
     */
    packComputed: (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
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
        fromStore: BlockStore
    ) => Promise<Block>
    /**
     * Unpacks and restores packed data into a block store.
     */
    restore: (
        pack: Uint8Array,
        intoStore: BlockStore
    ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>
    /**
     * Unpacks packed data
     */
    unpack: (
        pack: Uint8Array
    ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>
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

    const pushMany = async (
        cids: Iterable<any>,
        fromStore: BlockStore,
        writer: Writer
    ): Promise<void> => {
        for (const cid of cids) {
            const bytes = await fromStore.get(cid)
            writer.write({ cid, bytes })
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
        await pushMany([versionRoot], fromStore, writer)
        await pushMany(roots, fromStore, writer)
        for (const idx of indexes) {
            const { startOffsets } = idx.indexStruct
            await pushMany(startOffsets.values(), fromStore, writer)
        }
        const bytes = writer.close()
        const cid = await linkCodec.encode(bytes)
        return { cid, bytes }
    }

    const packSelected = async (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        edgeOffsetStart: number,
        edgeCount: number,
        propOffsetStart: number,
        propCount: number,
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

        const findBlock = (
            recordOffset: number,
            recordArraySize: number,
            startOffsets: Map<number, CID>
        ): CID => {
            if (recordOffset >= recordArraySize) {
                throw new Error('Record offset out of bounds')
            }
            let prevBlock: CID | undefined = undefined
            let prevBlockOffset = -Infinity
            for (const [blockOffset, cid] of startOffsets) {
                if (
                    recordOffset >= prevBlockOffset &&
                    recordOffset < blockOffset
                ) {
                    break
                }
                prevBlockOffset = blockOffset
                prevBlock = cid
            }
            if (recordOffset >= prevBlockOffset) {
                return prevBlock
            } else {
                throw new Error('Record not found')
            }
        }

        const readPropValueRefs = async (
            propOffsetStart: number,
            propCount: number
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
        const roots = [vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot]
        const carRoots = [CID.asCID(versionRoot)]
        let bufferSize = CarBufferWriter.headerLength({ roots: carRoots })
        bufferSize += await computeSize([versionRoot], fromStore)
        bufferSize += await computeSize(roots, fromStore)
        const blocksChecked = new Set<string>()
        const blocksAdded = new Set<string>()
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

        const addOffsetsToBufferSize = async (
            startOffsets: Map<number, CID>,
            recordOffset: number,
            recordCount: number,
            recordSize: number,
            recordArraySize: number
        ) => {
            const offsets = recordOffsets(recordOffset, recordCount, recordSize)
            for (const offset of offsets) {
                const cid = findBlock(offset, recordArraySize, startOffsets)
                if (!blocksChecked.has(cid.toString())) {
                    bufferSize += await computeSize([cid], fromStore)
                    blocksChecked.add(cid.toString())
                }
            }
        }

        const addPropValueOffsetsToBufferSize = async (
            startOffsets: Map<number, CID>,
            valueRefs: ValueRef[],
            valueArraySize: number
        ) => {
            const cids = await identifyValueBlocks(
                startOffsets,
                valueRefs,
                valueArraySize
            )
            for (const cid of cids) {
                if (!blocksChecked.has(cid.toString())) {
                    bufferSize += await computeSize([cid], fromStore)
                    blocksChecked.add(cid.toString())
                }
            }
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

        const valueRefs = await readPropValueRefs(propOffsetStart, propCount)

        await addOffsetsToBufferSize(
            vertexIndex.indexStruct.startOffsets,
            vertexOffsetStart,
            vertexCount,
            OFFSET_INCREMENTS.VERTEX_INCREMENT,
            vertexIndex.indexStruct.byteArraySize
        )
        await addOffsetsToBufferSize(
            edgeIndex.indexStruct.startOffsets,
            edgeOffsetStart,
            edgeCount,
            OFFSET_INCREMENTS.EDGE_INCREMENT,
            edgeIndex.indexStruct.byteArraySize
        )
        await addOffsetsToBufferSize(
            propIndex.indexStruct.startOffsets,
            propOffsetStart,
            propCount,
            OFFSET_INCREMENTS.PROP_INCREMENT,
            propIndex.indexStruct.byteArraySize
        )
        await addPropValueOffsetsToBufferSize(
            valueIndex.indexStruct.startOffsets,
            valueRefs,
            valueIndex.indexStruct.byteArraySize
        )
        const buffer = new Uint8Array(bufferSize)
        const writer: Writer = CarBufferWriter.createWriter(buffer, {
            roots: carRoots,
        })
        const pushOffsetsToWriter = async (
            startOffsets: Map<number, CID>,
            recordOffset: number,
            recordCount: number,
            recordSize: number,
            recordArraySize: number
        ) => {
            const offsets = recordOffsets(recordOffset, recordCount, recordSize)
            for (const offset of offsets) {
                const cid = findBlock(offset, recordArraySize, startOffsets)
                if (!blocksAdded.has(cid.toString())) {
                    await pushMany([cid], fromStore, writer)
                    blocksAdded.add(cid.toString())
                }
            }
        }

        const pushValueOffsetsToWriter = async (
            startOffsets: Map<number, CID>,
            valueRefs: ValueRef[],
            valueArraySize: number
        ) => {
            const cids = await identifyValueBlocks(
                startOffsets,
                valueRefs,
                valueArraySize
            )
            for (const cid of cids) {
                if (!blocksAdded.has(cid.toString())) {
                    await pushMany([cid], fromStore, writer)
                    blocksAdded.add(cid.toString())
                }
            }
        }

        await pushMany([versionRoot], fromStore, writer)
        await pushMany(roots, fromStore, writer)
        await pushOffsetsToWriter(
            vertexIndex.indexStruct.startOffsets,
            vertexOffsetStart,
            vertexCount,
            OFFSET_INCREMENTS.VERTEX_INCREMENT,
            vertexIndex.indexStruct.byteArraySize
        )
        await pushOffsetsToWriter(
            edgeIndex.indexStruct.startOffsets,
            edgeOffsetStart,
            edgeCount,
            OFFSET_INCREMENTS.EDGE_INCREMENT,
            edgeIndex.indexStruct.byteArraySize
        )
        await pushOffsetsToWriter(
            propIndex.indexStruct.startOffsets,
            propOffsetStart,
            propCount,
            OFFSET_INCREMENTS.PROP_INCREMENT,
            propIndex.indexStruct.byteArraySize
        )
        await pushValueOffsetsToWriter(
            valueIndex.indexStruct.startOffsets,
            valueRefs,
            valueIndex.indexStruct.byteArraySize
        )

        const bytes = writer.close()
        const cid = await linkCodec.encode(bytes)
        return { cid, bytes }
    }

    const packComputed = async (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
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
            fromStore
        )
    }

    const packFragment = async (
        graph: Graph,
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        fromStore: BlockStore
    ): Promise<Block> => {
        const vertices: Vertex[] = await graph.getVertexRange(
            vertexOffsetStart,
            vertexCount
        )

        let edgeOffsetStart = 0
        let edgeOffsetMax = 0

        let propOffsetStart = 0
        let propOffsetMax = 0

        for (const vertex of vertices) {
            const edges = await graph.getVertexEdges(vertex)
            for (const edge of edges) {
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
            }
            const props = await graph.getVertexProps(vertex)
            for (const prop of props) {
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

        return packSelected(
            versionRoot,
            vertexOffsetStart,
            vertexCount,
            edgeOffsetStart,
            edgeCount,
            propOffsetStart,
            propCount,
            fromStore
        )
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

    return {
        packGraph,
        packCommit,
        packSelected,
        packComputed,
        packFragment,
        restore,
        unpack,
    }
}

export { graphPackerFactory, GraphPacker }
