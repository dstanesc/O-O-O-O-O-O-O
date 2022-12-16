import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { BlockCodec, LinkCodec, ValueCodec } from './codecs'
import {
    Edge,
    Index,
    Link,
    Part,
    Prop,
    RootIndex,
    ValueRef,
    Vertex,
} from './types'
import { BlockStore } from './block-store'
import {
    EdgeDecoder,
    IndexDecoder,
    OFFSET_INCREMENTS,
    PropDecoder,
    PropValueDecoder,
    VertexDecoder,
} from './serde'
import fastEqual from 'fast-deep-equal/es6'

interface BaselineDelta {
    ({
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot,
        currentIndex,
        currentStore,
    }: {
        baseRoot: Link
        baseIndex: RootIndex
        baseStore: BlockStore
        currentRoot: Link
        currentIndex: RootIndex
        currentStore: BlockStore
    }): Promise<{
        vertices: {
            added: Map<number, Vertex>
            updated: Map<number, Vertex>
            updateBaseline: Map<number, Vertex>
        }
        edges: {
            added: Map<number, Edge>
            updated: Map<number, Edge>
            updateBaseline: Map<number, Edge>
        }
        props: {
            added: Map<number, Prop>
            updated: Map<number, Prop>
            updateBaseline: Map<number, Prop>
        }
        indices: {
            added: Map<number, Index>
            updated: Map<number, Index>
            updateBaseline: Map<number, Index>
        }
    }>
}

interface DeltaFactory {
    baselineDelta: BaselineDelta
    baselineChangesRecords: <T extends Part>({
        recordSize,
        baseRoot,
        baseIndex,
        baseStore,
        baseValueRoot,
        baseValueIndex,
        currentRoot,
        currentIndex,
        currentStore,
        currentValueRoot,
        currentValueIndex,
        recordsDecode,
    }: {
        recordSize: number
        baseRoot: Link
        baseIndex: any
        baseStore: BlockStore
        baseValueRoot?: Link
        baseValueIndex?: any
        currentRoot: Link
        currentIndex: any
        currentStore: BlockStore
        currentValueRoot: Link
        currentValueIndex: any
        recordsDecode: ({
            bytes,
            valueRoot,
            valueIndex,
            blockStore,
        }: {
            bytes: Uint8Array
            valueRoot: Link
            valueIndex: any
            blockStore: BlockStore
        }) => Promise<T[]>
    }) => Promise<{
        added: Map<number, T>
        updated: Map<number, T>
        updateBaseline: Map<number, T>
    }>
}

interface Range {
    start: number
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
            } else ranges.push({ start: range.start, end: range.end })
        } else ranges.push({ start: range.start, end: range.end })
    }
    const get = () => ranges

    const getAdjusted = () => {
        return ranges.map((range) => {
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

const deltaFactory = ({
    linkCodec,
    valueCodec,
}: {
    linkCodec: LinkCodec
    valueCodec: ValueCodec
}): DeltaFactory => {
    const { encode: linkEncode, decode: linkDecode }: LinkCodec = linkCodec
    const { encode: valueEncode, decode: valueDecode }: ValueCodec = valueCodec

    const { read } = chunkyStore()

    const baselineDelta = async ({
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot,
        currentIndex,
        currentStore,
    }: {
        baseRoot: Link
        baseIndex: RootIndex
        baseStore: BlockStore
        currentRoot: Link
        currentIndex: RootIndex
        currentStore: BlockStore
    }) => {
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
            valueRoot: baseValueRoot,
            valueOffset: baseValueOffset,
            valueIndex: baseValueIndex,
            indexRoot: baseIndexRoot,
            indexOffset: baseIndexOffset,
            indexIndex: baseIndexIndex,
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
            valueRoot: currentValueRoot,
            valueOffset: currentValueOffset,
            valueIndex: currentValueIndex,
            indexRoot: currentIndexRoot,
            indexOffset: currentIndexOffset,
            indexIndex: currentIndexIndex,
        } = currentIndex

        const {
            added: verticesAdded,
            updated: verticesUpdated,
            updateBaseline: verticesBaseline,
        } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.VERTEX_INCREMENT,
            baseRoot: baseVertexRoot,
            baseIndex: baseVertexIndex,
            baseStore,
            baseValueRoot: undefined,
            baseValueIndex: undefined,
            currentRoot: currentVertexRoot,
            currentIndex: currentVertexIndex,
            currentStore,
            currentValueRoot: undefined,
            currentValueIndex: undefined,
            recordsDecode: async ({
                bytes,
                valueRoot,
                valueIndex,
                blockStore,
            }: {
                bytes: Uint8Array
                valueRoot: Link
                valueIndex: any
                blockStore: BlockStore
            }) => new VertexDecoder(bytes).read(),
        })

        const {
            added: edgesAdded,
            updated: edgesUpdated,
            updateBaseline: edgesBaseline,
        } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.EDGE_INCREMENT,
            baseRoot: baseEdgeRoot,
            baseIndex: baseEdgeIndex,
            baseStore,
            baseValueRoot: undefined,
            baseValueIndex: undefined,
            currentRoot: currentEdgeRoot,
            currentIndex: currentEdgeIndex,
            currentStore,
            currentValueRoot: undefined,
            currentValueIndex: undefined,
            recordsDecode: async ({
                bytes,
                valueRoot,
                valueIndex,
                blockStore,
            }: {
                bytes: Uint8Array
                valueRoot: Link
                valueIndex: any
                blockStore: BlockStore
            }) => new EdgeDecoder(bytes).read(),
        })

        const {
            added: propsAdded,
            updated: propsUpdated,
            updateBaseline: propsBaseline,
        } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.PROP_INCREMENT,
            baseRoot: basePropRoot,
            baseIndex: basePropIndex,
            baseStore,
            baseValueRoot: baseValueRoot,
            baseValueIndex: baseValueIndex,
            currentRoot: currentPropRoot,
            currentIndex: currentPropIndex,
            currentStore,
            currentValueRoot: currentValueRoot,
            currentValueIndex: currentValueIndex,
            recordsDecode: async ({
                bytes,
                valueRoot,
                valueIndex,
                blockStore,
            }: {
                bytes: Uint8Array
                valueRoot: Link
                valueIndex: any
                blockStore: BlockStore
            }) => {
                const valueGet = async (
                    { root, index }: { root: Link; index: any },
                    { propRef, ref, length }: ValueRef
                ): Promise<Prop> => {
                    const bytes = await read(ref, length, {
                        root,
                        index,
                        decode: linkDecode,
                        get: blockStore.get,
                    })
                    return new PropValueDecoder(bytes, valueDecode).readValue({
                        propRef,
                        ref,
                        length,
                    })
                }

                return await new PropDecoder(bytes, (ref: ValueRef) =>
                    valueGet({ root: valueRoot, index: valueIndex }, ref)
                ).read()
            },
        })

        const {
            added: indicesAdded,
            updated: indicesUpdated,
            updateBaseline: indicesBaseline,
        } = await baselineChangesRecords({
            recordSize: OFFSET_INCREMENTS.PROP_INCREMENT,
            baseRoot: baseIndexRoot,
            baseIndex: baseIndexIndex,
            baseStore,
            baseValueRoot: undefined,
            baseValueIndex: undefined,
            currentRoot: currentIndexRoot,
            currentIndex: currentIndexIndex,
            currentStore,
            currentValueRoot: undefined,
            currentValueIndex: undefined,
            recordsDecode: async ({
                bytes,
                valueRoot,
                valueIndex,
                blockStore,
            }: {
                bytes: Uint8Array
                valueRoot: Link
                valueIndex: any
                blockStore: BlockStore
            }) => await new IndexDecoder(bytes, linkDecode).read(),
        })

        return {
            vertices: {
                added: verticesAdded,
                updated: verticesUpdated,
                updateBaseline: verticesBaseline,
            },
            edges: {
                added: edgesAdded,
                updated: edgesUpdated,
                updateBaseline: edgesBaseline,
            },
            props: {
                added: propsAdded,
                updated: propsUpdated,
                updateBaseline: propsBaseline,
            },
            indices: {
                added: indicesAdded,
                updated: indicesUpdated,
                updateBaseline: indicesBaseline,
            },
        }
    }

    const baselineChangesRecords = async <T extends Part>({
        recordSize,
        baseRoot,
        baseIndex,
        baseStore,
        baseValueRoot,
        baseValueIndex,
        currentRoot,
        currentIndex,
        currentStore,
        currentValueRoot,
        currentValueIndex,
        recordsDecode,
    }: {
        recordSize: number
        baseRoot: Link
        baseIndex: any
        baseStore: BlockStore
        baseValueRoot?: Link
        baseValueIndex?: any
        currentRoot: Link
        currentIndex: any
        currentStore: BlockStore
        currentValueRoot: Link
        currentValueIndex: any
        recordsDecode: ({
            bytes,
            valueRoot,
            valueIndex,
            blockStore,
        }: {
            bytes: Uint8Array
            valueRoot: Link
            valueIndex: any
            blockStore: BlockStore
        }) => Promise<T[]>
    }): Promise<{
        added: Map<number, T>
        updated: Map<number, T>
        updateBaseline: Map<number, T>
    }> => {
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
        const {
            add: addRange,
            get: getRanges,
            getAdjusted: getAdjustedRanges,
        } = compactRanges(recordSize, currentByteArraySize)

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
        const added: Map<number, T> = new Map()
        const updated: Map<number, T> = new Map()
        const updateBaseline: Map<number, T> = new Map()

        for (const range of adjustedRanges) {
            const currentRangeBytes = await read(
                range.start,
                range.end - range.start,
                {
                    root: currentRoot,
                    index: currentIndex,
                    decode: linkDecode,
                    get: currentStore.get,
                }
            )
            const currentRecords: T[] = await recordsDecode({
                bytes: currentRangeBytes,
                valueRoot: currentValueRoot,
                valueIndex: currentValueIndex,
                blockStore: currentStore,
            })

            if (range.end <= baseByteArraySize) {
                const baseRangeBytes = await read(
                    range.start,
                    range.end - range.start,
                    {
                        root: baseRoot,
                        index: baseIndex,
                        decode: linkDecode,
                        get: baseStore.get,
                    }
                )
                const baseRecords: T[] = await recordsDecode({
                    bytes: baseRangeBytes,
                    valueRoot: baseValueRoot,
                    valueIndex: baseValueIndex,
                    blockStore: baseStore,
                })
                for (let i = 0; i < currentRecords.length; i++) {
                    const currentRecord = currentRecords[i]
                    const baseRecord = baseRecords[i]
                    if (!fastEqual(currentRecord, baseRecord)) {
                        updated.set(currentRecord.offset, currentRecord)
                        updateBaseline.set(currentRecord.offset, baseRecord)
                    }
                }
            } else if (range.start < baseByteArraySize) {
                const baseRangeBytes = await read(
                    range.start,
                    baseByteArraySize - range.start,
                    {
                        root: baseRoot,
                        index: baseIndex,
                        decode: linkDecode,
                        get: baseStore.get,
                    }
                )
                const baseRecords: T[] = await recordsDecode({
                    bytes: baseRangeBytes,
                    valueRoot: baseValueRoot,
                    valueIndex: baseValueIndex,
                    blockStore: baseStore,
                })
                for (let i = 0; i < baseRecords.length; i++) {
                    const currentRecord = currentRecords[i]
                    const baseRecord = baseRecords[i]
                    if (!fastEqual(currentRecord, baseRecord)) {
                        updated.set(currentRecord.offset, currentRecord)
                        updateBaseline.set(currentRecord.offset, baseRecord)
                    }
                }
                for (
                    let i = baseRecords.length;
                    i < currentRecords.length;
                    i++
                ) {
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

        return { added, updated, updateBaseline }
    }

    return { baselineDelta, baselineChangesRecords }
}

export { deltaFactory, DeltaFactory, BaselineDelta }
