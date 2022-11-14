

import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { BlockCodec, LinkCodec } from "./codecs"
import { Link, Part, RootIndex, Vertex } from './types'
import { BlockStore } from "./block-store"
import { EdgeDecoder, IndexDecoder, OFFSET_INCREMENTS, PropDecoder, VertexDecoder } from './serde'
import fastEqual from 'fast-deep-equal/es6'

interface DeltaFactory {
    baselineDelta: ({
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot,
        currentIndex,
        currentStore
    }: {
        baseRoot: Link,
        baseIndex: RootIndex,
        baseStore: BlockStore
        currentRoot: Link,
        currentIndex: RootIndex
        currentStore: BlockStore
    }) => Promise<{
        vertices: { added: Map<number, Part>, updated: Map<number, Part> },
        edges: { added: Map<number, Part>, updated: Map<number, Part> },
        props: { added: Map<number, Part>, updated: Map<number, Part> },
        indices: { added: Map<number, Part>, updated: Map<number, Part> }
    }>
    baselineChangesRecords: ({
        recordSize,
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot,
        currentIndex,
        currentStore,
        recordsDecode
    }: {
        recordSize: number,
        baseRoot: Link,
        baseIndex: any,
        baseStore: BlockStore
        currentRoot: Link,
        currentIndex: any,
        currentStore: BlockStore,
        recordsDecode: (cidBytes: Uint8Array, blockStore: BlockStore) => Promise<Part[]>
    }) => Promise<{ added: Map<number, Part>, updated: Map<number, Part> }>
}

interface Range {
    start: number,
    end: number
}

const compactRanges = (recordSize: number, maxSize: number) => {
    const ranges: Range[] = []
    const add = (range: Range) => {
        if (ranges.length > 0) {
            const prevRange = ranges[ranges.length - 1]
            if (prevRange.end === range.start) {
                const prevRange = ranges[ranges.length - 1]
                prevRange.end = range.end
            } else
                ranges.push({ start: range.start, end: range.end })

        } else
            ranges.push({ start: range.start, end: range.end })
    }
    const get = () => ranges

    const getAdjusted = () => {
        return ranges.map(range => {
            const lastBefore = lastOffsetBefore(range.start)
            const nextAfter = nextOffsetAfter(range.end) + recordSize
            const endActual = nextAfter > maxSize ? maxSize : nextAfter
            return { start: lastBefore, end: endActual }
        })
    }

    const lastOffsetBefore = (offset: number) => {
        const quotient = Math.floor(offset / recordSize)
        return quotient * recordSize
    }

    const nextOffsetAfter = (offset: number) => {
        const quotient = Math.floor(offset / recordSize)
        return (quotient + 1) * recordSize
    }

    return { add, get, getAdjusted }
}

const deltaFactory = ({ linkCodec, blockCodec }: {
    linkCodec: LinkCodec,
    blockCodec: BlockCodec

}): DeltaFactory => {

    const { encode: linkEncode, decode: linkDecode }: LinkCodec = linkCodec
    const { encode: blockEncode, decode: blockDecode }: BlockCodec = blockCodec

    const { read } = chunkyStore()

    const merge = async (
        { baseRoot, baseIndex, baseStore, currentRoot, currentIndex, currentStore, otherRoot, otherIndex, otherStore }: 
        { baseRoot: Link, baseIndex: RootIndex, baseStore: BlockStore, currentRoot: Link, currentIndex: RootIndex, currentStore: BlockStore, otherRoot: Link, otherIndex: RootIndex, otherStore: BlockStore }) => {

        const currentDelta = baselineDelta({ baseRoot, baseIndex, baseStore, currentRoot, currentIndex, currentStore  })
        const otherDelta = baselineDelta({ baseRoot, baseIndex, baseStore, currentRoot: otherRoot, currentIndex: otherIndex, currentStore: otherStore })

        // FIXME WIP
    }

    const baselineDelta = async (
        { baseRoot, baseIndex, baseStore, currentRoot, currentIndex, currentStore }: 
        { baseRoot: Link, baseIndex: RootIndex, baseStore: BlockStore, currentRoot: Link, currentIndex: RootIndex, currentStore: BlockStore }) => {
        const {
            vertexRoot: baseVertexRoot,
            vertexOffset: baseVertexOffset,
            vertexIndex: baseVertexIndex,
            edgeRoot: baseEdgeRoot,
            edgeOffset: baseEdgeOffset,
            edgeIndex: baseEdgeIndex,
            propRoot: basePropRoot,
            propOffset: basePropOffset,
            propIndex: basePropIndex,
            indexRoot: baseIndexRoot,
            indexOffset: baseIndexOffset,
            indexIndex: baseIndexIndex
        } = baseIndex

        const {
            vertexRoot: currentVertexRoot,
            vertexOffset: currentVertexOffset,
            vertexIndex: currentVertexIndex,
            edgeRoot: currentEdgeRoot,
            edgeOffset: currentEdgeOffset,
            edgeIndex: currentEdgeIndex,
            propRoot: currentPropRoot,
            propOffset: currentPropOffset,
            propIndex: currentPropIndex,
            indexRoot: currentIndexRoot,
            indexOffset: currentIndexOffset,
            indexIndex: currentIndexIndex
        } = currentIndex

        const { added: verticesAdded, updated: verticesUpdated } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.VERTEX_INCREMENT,
            baseRoot: baseVertexRoot,
            baseIndex: baseVertexIndex,
            baseStore,
            currentRoot: currentVertexRoot,
            currentIndex: currentVertexIndex,
            currentStore,
            recordsDecode: async (bytes: Uint8Array, blockStore: BlockStore) => new VertexDecoder(bytes).read()
        })

        const { added: edgesAdded, updated: edgesUpdated } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.EDGE_INCREMENT,
            baseRoot: baseEdgeRoot,
            baseIndex: baseEdgeIndex,
            baseStore,
            currentRoot: currentEdgeRoot,
            currentIndex: currentEdgeIndex,
            currentStore,
            recordsDecode: async (bytes: Uint8Array, blockStore: BlockStore) => new EdgeDecoder(bytes).read()
        })

        const { added: propsAdded, updated: propsUpdated } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.PROP_INCREMENT,
            baseRoot: basePropRoot,
            baseIndex: basePropIndex,
            baseStore,
            currentRoot: currentPropRoot,
            currentIndex: currentPropIndex,
            currentStore,
            recordsDecode: async (bytes: Uint8Array, blockStore: BlockStore) => await new PropDecoder(bytes, linkDecode, blockDecode, blockStore.get).read()
        })

        const { added: indicesAdded, updated: indicesUpdated } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.PROP_INCREMENT,
            baseRoot: basePropRoot,
            baseIndex: basePropIndex,
            baseStore,
            currentRoot: currentPropRoot,
            currentIndex: currentPropIndex,
            currentStore,
            recordsDecode: async (bytes: Uint8Array, blockStore: BlockStore) => await new IndexDecoder(bytes, linkDecode).read()
        })

        return {
            vertices: { added: verticesAdded, updated: verticesUpdated },
            edges: { added: edgesAdded, updated: edgesUpdated },
            props: { added: propsAdded, updated: propsUpdated },
            indices: { added: indicesAdded, updated: indicesUpdated }
        }
    }

    const baselineChangesRecords = async (
        { recordSize, baseRoot, baseIndex, baseStore, currentRoot, currentIndex, currentStore, recordsDecode }: 
        { recordSize: number, baseRoot: Link, baseIndex: any, baseStore: BlockStore, currentRoot: Link, currentIndex: any, currentStore: BlockStore, recordsDecode: (cidBytes: Uint8Array, blockStore: BlockStore) => Promise<Part[]> }): Promise<{ added: Map<number, Part>, updated: Map<number, Part> }> => {
        const {
            startOffsets: baseStartOffsets,
            indexSize: baseIndexSize,
            byteArraySize: baseByteArraySize,
        } = baseIndex.indexStruct
        const {
            startOffsets: currentStartOffsets,
            indexSize: currentIndexSize,
            byteArraySize: currentByteArraySize,
        } = currentIndex.indexStruct
        const { add: addRange, get: getRanges, getAdjusted: getAdjustedRanges } = compactRanges(recordSize, currentByteArraySize)

        let currentBlock = undefined
        let updateMode = false
        for (const [currOffset, currCid] of currentStartOffsets) {
            if (currentBlock !== undefined) currentBlock.end = currOffset
            if (updateMode) addRange(currentBlock)
            if (currOffset <= baseByteArraySize) {
                const baseCid = baseStartOffsets.get(currOffset)
                if (baseCid !== undefined) {
                    if (currCid.toString() === baseCid.toString()) {
                        updateMode = false
                    } else {
                        updateMode = true
                    }
                } else {
                    updateMode = true
                }
            } else {
                updateMode = true
            }
            currentBlock = { start: currOffset, end: undefined, cid: currCid }
        }
        if (updateMode) {
            currentBlock.end = currentByteArraySize
            addRange(currentBlock)
        }

        const adjustedRanges = getAdjustedRanges()
        const added: Map<number, Part> = new Map()
        const updated: Map<number, Part> = new Map()

        for (const range of adjustedRanges) {
            const currentRangeBytes = await read(range.start, range.end - range.start, { root: currentRoot, index: currentIndex, decode: linkDecode, get: currentStore.get })
            const currentRecords: Part[] = await recordsDecode(currentRangeBytes, currentStore)

            if (range.end <= baseByteArraySize) {
                const baseRangeBytes = await read(range.start, range.end - range.start, { root: baseRoot, index: baseIndex, decode: linkDecode, get: baseStore.get })
                const baseRecords: Part[] = await recordsDecode(baseRangeBytes, baseStore)
                for (let i = 0; i < currentRecords.length; i++) {
                    const currentRecord = currentRecords[i]
                    const baseRecord = baseRecords[i]
                    if (!fastEqual(currentRecord, baseRecord)) {
                        updated.set(currentRecord.offset, currentRecord)
                    }
                }
            } else if (range.start < baseByteArraySize) {
                const baseRangeBytes = await read(range.start, baseByteArraySize - range.start, { root: baseRoot, index: baseIndex, decode: linkDecode, get: baseStore.get })
                const baseRecords: Part[] = await recordsDecode(baseRangeBytes, baseStore)
                for (let i = 0; i < baseRecords.length; i++) {
                    const currentRecord = currentRecords[i]
                    const baseRecord = baseRecords[i]
                    if (!fastEqual(currentRecord, baseRecord)) {
                        updated.set(currentRecord.offset, currentRecord)
                    }
                }
                for (let i = baseRecords.length; i < currentRecords.length; i++) {
                    const currentRecord = currentRecords[i]
                    added.set(currentRecord.offset, currentRecord)
                }
            } else {
                for (let i = 0; i < currentRecords.length; i++) {
                    const currentRecord = currentRecords[i]
                    added.set(currentRecord.offset, currentRecord)
                }
            }
        }

        return { added, updated }
    }

    return { baselineDelta, baselineChangesRecords }
}

export { deltaFactory, DeltaFactory } 