import { BlockStore } from './block-store'
import { fastCloneEdge, fastCloneProp, fastCloneVertex } from './clone'
import { Link, Part, Vertex, Edge, Prop, RootIndex } from './types'
import { BaselineDelta, deltaFactory, DeltaFactory } from './delta'
import { Graph, Tx } from './graph'
import { blockIndexFactory } from './block-index'
import { BlockCodec, LinkCodec } from './codecs'
import { initRootStore, RootStore } from './root-store'
import { graphStore } from './graph-store'
import { OFFSET_INCREMENTS } from './serde'

enum MergePolicyEnum {
    LastWriterWins,
    MultiValueRegistry,
}

interface MergeOrder {
    ({ current, other }: { current: any; other: any }): {
        first: any
        second: any
    }
}

interface MergePolicy {
    mergeVertex: (
        tx: Tx,
        baseline: Vertex,
        first: Vertex,
        second: Vertex
    ) => Promise<Vertex>
    mergeEdge: (
        tx: Tx,
        baseline: Edge,
        first: Edge,
        second: Edge
    ) => Promise<Edge>
    mergeProp: (
        tx: Tx,
        baseline: Prop,
        first: Prop,
        second: Prop
    ) => Promise<Prop>
}

// account  on the fact that only crdt operations will be supported on the graph
const mergePolicyLastWriterWins = (): MergePolicy => {
    const mergeVertex = async (
        tx: Tx,
        baseline: Vertex,
        first: Vertex,
        second: Vertex
    ): Promise<Vertex> => {
        const target: Vertex = fastCloneVertex(first)
        if (second.nextEdge !== baseline.nextEdge)
            target.nextEdge = second.nextEdge
        if (second.nextProp !== baseline.nextProp)
            target.nextProp = second.nextProp
        return target
    }

    const mergeEdge = async (
        tx: Tx,
        baseline: Edge,
        first: Edge,
        second: Edge
    ): Promise<Edge> => {
        const target: Edge = fastCloneEdge(first)
        if (second.sourceNext !== baseline.sourceNext)
            target.sourceNext = second.sourceNext
        if (second.nextProp !== baseline.nextProp)
            target.nextProp = second.nextProp
        return target
    }

    const mergeProp = async (
        tx: Tx,
        baseline: Prop,
        first: Prop,
        second: Prop
    ): Promise<Prop> => {
        const target: Prop = fastCloneProp(first)
        if (second.nextProp !== baseline.nextProp)
            target.nextProp = second.nextProp
        return target
    }

    return { mergeVertex, mergeEdge, mergeProp }
}

const mergePolicyMultiValueRegistry = (): MergePolicy => {
    const mergeVertex = async (
        tx: Tx,
        baseline: Vertex,
        first: Vertex,
        second: Vertex
    ): Promise<Vertex> => {
        const target: Vertex = fastCloneVertex(first)
        if (second.nextEdge !== baseline.nextEdge)
            await tx.linkVertexEdge(target, second.nextEdge)
        if (second.nextProp !== baseline.nextProp)
            await tx.linkVertexProp(target, second.nextProp)
        return target
    }

    const mergeEdge = async (
        tx: Tx,
        baseline: Edge,
        first: Edge,
        second: Edge
    ): Promise<Edge> => {
        const target: Edge = fastCloneEdge(first)
        if (
            second.sourceNext !== undefined &&
            second.sourceNext !== baseline.sourceNext
        )
            await tx.linkEdge(target, second.sourceNext)
        if (second.nextProp !== baseline.nextProp)
            await tx.linkEdgeProp(target, second.nextProp)

        return target
    }

    const mergeProp = async (
        tx: Tx,
        baseline: Prop,
        first: Prop,
        second: Prop
    ): Promise<Prop> => {
        const target: Prop = fastCloneProp(first)
        if (second.nextProp !== baseline.nextProp)
            await tx.linkProp(target, second.nextProp)
        return target
    }

    return { mergeVertex, mergeEdge, mergeProp }
}

const merge = async (
    {
        baseRoot,
        baseStore,
        currentRoot,
        currentStore,
        otherRoot,
        otherStore,
    }: {
        baseRoot: Link
        baseStore: BlockStore
        currentRoot: Link
        currentStore: BlockStore
        otherRoot: Link
        otherStore: BlockStore
    },
    policy: MergePolicyEnum,
    chunk: (buffer: Uint8Array) => Uint32Array,
    linkCodec: LinkCodec,
    blockCodec: BlockCodec
) => {
    const mergeOrder = <T>({ current, other }: { current: T; other: T }) => {
        return currentRoot.bytes[0] < otherRoot.bytes[0]
            ? { first: current, second: other }
            : { first: other, second: current }
    }

    let mergePolicy: MergePolicy

    switch (policy) {
        case MergePolicyEnum.LastWriterWins:
            mergePolicy = mergePolicyLastWriterWins()
            break
        case MergePolicyEnum.MultiValueRegistry:
            mergePolicy = mergePolicyMultiValueRegistry()
            break
        default:
            throw new Error(`Unknown merge policy ${policy}`)
    }

    const { baselineDelta } = deltaFactory({ linkCodec, blockCodec })

    const { buildRootIndex: buildBaseIndex } = blockIndexFactory({
        linkCodec,
        blockStore: baseStore,
    })
    const { buildRootIndex: buildCurrentIndex } = blockIndexFactory({
        linkCodec,
        blockStore: currentStore,
    })
    const { buildRootIndex: buildOtherIndex } = blockIndexFactory({
        linkCodec,
        blockStore: otherStore,
    })

    const { root: baseRootRef, index: baseIndex } = await buildBaseIndex(
        baseRoot
    )
    const { root: currentRootRef, index: currentIndex } =
        await buildCurrentIndex(currentRoot)
    const { root: otherRootRef, index: otherIndex } = await buildOtherIndex(
        otherRoot
    )

    const {
        first: { root: firstRoot, index: firstIndex, blockStore: firstStore },
        second,
    } = mergeOrder({
        current: {
            root: currentRootRef,
            index: currentIndex,
            blockStore: currentStore,
        },
        other: {
            root: otherRootRef,
            index: otherIndex,
            blockStore: otherStore,
        },
    })

    const rootStore: RootStore = initRootStore({
        root: firstRoot,
        index: firstIndex,
    })
    const store = graphStore({
        chunk,
        linkCodec,
        blockCodec,
        blockStore: firstStore,
    })
    const graph = new Graph(rootStore, store)

    const tx: Tx = graph.tx()

    await tx.start()

    await policyMerge(tx, mergeOrder, mergePolicy, baselineDelta, {
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot,
        currentIndex,
        currentStore,
        otherRoot,
        otherIndex,
        otherStore,
    })

    return await tx.commit()
}

const policyMerge = async (
    tx: Tx,
    mergeOrder: MergeOrder,
    mergePolicy: MergePolicy,
    baselineDelta: BaselineDelta,
    {
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot,
        currentIndex,
        currentStore,
        otherRoot,
        otherIndex,
        otherStore,
    }: {
        baseRoot: Link
        baseIndex: RootIndex
        baseStore: BlockStore
        currentRoot: Link
        currentIndex: RootIndex
        currentStore: BlockStore
        otherRoot: Link
        otherIndex: RootIndex
        otherStore: BlockStore
    }
) => {
    const { vertexIndex, edgeIndex, propIndex } = baseIndex

    const { byteArraySize: vertexRebaseThreshold } = vertexIndex.indexStruct

    const { byteArraySize: edgeRebaseThreshold } = edgeIndex.indexStruct

    const { byteArraySize: propRebaseThreshold } = propIndex.indexStruct

    const currentDelta = await baselineDelta({
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot,
        currentIndex,
        currentStore,
    })

    const otherDelta = await baselineDelta({
        baseRoot,
        baseIndex,
        baseStore,
        currentRoot: otherRoot,
        currentIndex: otherIndex,
        currentStore: otherStore,
    })

    const { first: verticesFirst, second: verticesSecond } = mergeOrder({
        current: currentDelta.vertices.added,
        other: otherDelta.vertices.added,
    })

    const {
        rebased: verticesRebased,
        rebaseOffsetDelta: vertexRebaseOffsetDelta,
    } = await rebaseAdditionOffsets(
        { first: verticesFirst, second: verticesSecond },
        fastCloneVertex,
        OFFSET_INCREMENTS.VERTEX_INCREMENT
    )

    const { first: edgesFirst, second: edgesSecond } = mergeOrder({
        current: currentDelta.edges.added,
        other: otherDelta.edges.added,
    })

    const { rebased: edgesRebased, rebaseOffsetDelta: edgeRebaseOffsetDelta } =
        await rebaseAdditionOffsets(
            { first: edgesFirst, second: edgesSecond },
            fastCloneEdge,
            OFFSET_INCREMENTS.EDGE_INCREMENT
        )

    const { first: propsFirst, second: propsSecond } = mergeOrder({
        current: currentDelta.props.added,
        other: otherDelta.props.added,
    })

    const { rebased: propsRebased, rebaseOffsetDelta: propRebaseOffsetDelta } =
        await rebaseAdditionOffsets(
            { first: propsFirst, second: propsSecond },
            fastCloneProp,
            OFFSET_INCREMENTS.PROP_INCREMENT
        )

    await rebaseVertexEdgeRef({
        verticesRebased,
        edgeRebaseThreshold,
        edgeRebaseOffsetDelta,
    })
    await rebaseVertexPropRef({
        verticesRebased,
        propRebaseThreshold,
        propRebaseOffsetDelta,
    })
    await rebaseEdgeVertexRef({
        edgesRebased,
        vertexRebaseThreshold,
        vertexRebaseOffsetDelta,
    })
    await rebaseEdgeEdgeRef({
        edgesRebased,
        edgeRebaseThreshold,
        edgeRebaseOffsetDelta,
    })
    await rebaseEdgePropRef({
        edgesRebased,
        propRebaseThreshold,
        propRebaseOffsetDelta,
    })
    await rebasePropPropRef({
        propsRebased,
        propRebaseThreshold,
        propRebaseOffsetDelta,
    })

    const { first: verticesFirstUpdated, second: verticesSecondUpdated } =
        mergeOrder({
            current: currentDelta.vertices.updated,
            other: otherDelta.vertices.updated,
        })
    await rebaseVertexEdgeRef({
        verticesRebased: verticesSecondUpdated,
        edgeRebaseThreshold,
        edgeRebaseOffsetDelta,
    })
    await rebaseVertexPropRef({
        verticesRebased: verticesSecondUpdated,
        propRebaseThreshold,
        propRebaseOffsetDelta,
    })

    const { first: edgesFirstUpdated, second: edgesSecondUpdated } = mergeOrder(
        { current: currentDelta.edges.updated, other: otherDelta.edges.updated }
    )
    await rebaseEdgeVertexRef({
        edgesRebased: edgesSecondUpdated,
        vertexRebaseThreshold,
        vertexRebaseOffsetDelta,
    })
    await rebaseEdgeEdgeRef({
        edgesRebased: edgesSecondUpdated,
        edgeRebaseThreshold,
        edgeRebaseOffsetDelta,
    })
    await rebaseEdgePropRef({
        edgesRebased: edgesSecondUpdated,
        propRebaseThreshold,
        propRebaseOffsetDelta,
    })

    const { first: propsFirstUpdated, second: propsSecondUpdated } = mergeOrder(
        { current: currentDelta.props.updated, other: otherDelta.props.updated }
    )
    await rebasePropPropRef({
        propsRebased: propsSecondUpdated,
        propRebaseThreshold,
        propRebaseOffsetDelta,
    })

    for (const [vertexOffset, vertex] of verticesRebased)
        tx.vertices.added.set(vertexOffset, vertex)

    for (const [edgeOffset, edge] of edgesRebased)
        tx.edges.added.set(edgeOffset, edge)

    for (const [propOffset, prop] of propsRebased)
        tx.props.added.set(propOffset, prop)

    const verticesUpdated = await applyUpdates(
        tx,
        {
            first: verticesFirstUpdated,
            second: verticesSecondUpdated,
            baseline: currentDelta.vertices.updateBaseline,
        },
        mergePolicy.mergeVertex
    )
    const edgesUpdated = await applyUpdates(
        tx,
        {
            first: edgesFirstUpdated,
            second: edgesSecondUpdated,
            baseline: currentDelta.edges.updateBaseline,
        },
        mergePolicy.mergeEdge
    )
    const propsUpdated = await applyUpdates(
        tx,
        {
            first: propsFirstUpdated,
            second: propsSecondUpdated,
            baseline: currentDelta.props.updateBaseline,
        },
        mergePolicy.mergeProp
    )

    for (const [vertexOffset, vertex] of verticesUpdated)
        tx.vertices.updated.set(vertexOffset, vertex)

    for (const [edgeOffset, edge] of edgesUpdated)
        tx.edges.updated.set(edgeOffset, edge)

    for (const [propOffset, prop] of propsUpdated)
        tx.props.updated.set(propOffset, prop)
}

const rebaseAdditionOffsets = async <T extends Part>(
    { first, second }: { first: Map<number, T>; second: Map<number, T> },
    fastClone: (part: T) => T,
    recordSize: number
) => {
    let rebased: Map<number, T> = new Map()
    let rebaseOffsetDelta = 0
    if (first.size > 0 && second.size > 0) {
        const firstAddedArray: number[] = Array.from(first.keys())
        const firstOffset = firstAddedArray[0]
        const lastOffset = firstAddedArray[first.size - 1]
        rebaseOffsetDelta = lastOffset - firstOffset + recordSize
        for (const secondPart of second.values()) {
            const secondPartClone = fastClone(secondPart)
            secondPartClone.offset += rebaseOffsetDelta
            rebased.set(secondPartClone.offset, secondPartClone)
        }
    }
    return { rebased, rebaseOffsetDelta }
}

const rebaseVertexEdgeRef = async ({
    verticesRebased,
    edgeRebaseThreshold,
    edgeRebaseOffsetDelta,
}: {
    verticesRebased: Map<number, Vertex>
    edgeRebaseThreshold: number
    edgeRebaseOffsetDelta: number
}) => {
    for (const vertex of verticesRebased.values()) {
        if (vertex.nextEdge >= edgeRebaseThreshold)
            vertex.nextEdge += edgeRebaseOffsetDelta
    }
}

const rebaseVertexPropRef = async ({
    verticesRebased,
    propRebaseThreshold,
    propRebaseOffsetDelta,
}: {
    verticesRebased: Map<number, Vertex>
    propRebaseThreshold: number
    propRebaseOffsetDelta: number
}) => {
    for (const vertex of verticesRebased.values()) {
        if (vertex.nextProp >= propRebaseThreshold)
            vertex.nextProp += propRebaseOffsetDelta
    }
}

const rebaseEdgeVertexRef = async ({
    edgesRebased,
    vertexRebaseThreshold,
    vertexRebaseOffsetDelta,
}: {
    edgesRebased: Map<number, Edge>
    vertexRebaseThreshold: number
    vertexRebaseOffsetDelta: number
}) => {
    for (const edge of edgesRebased.values()) {
        if (edge.source >= vertexRebaseThreshold)
            edge.source += vertexRebaseOffsetDelta
        if (edge.target >= vertexRebaseThreshold)
            edge.target += vertexRebaseOffsetDelta
    }
}

const rebaseEdgeEdgeRef = async ({
    edgesRebased,
    edgeRebaseThreshold,
    edgeRebaseOffsetDelta,
}: {
    edgesRebased: Map<number, Edge>
    edgeRebaseThreshold: number
    edgeRebaseOffsetDelta: number
}) => {
    for (const edge of edgesRebased.values()) {
        if (edge.sourcePrev >= edgeRebaseThreshold)
            edge.sourcePrev += edgeRebaseOffsetDelta
        if (edge.sourceNext >= edgeRebaseThreshold)
            edge.sourceNext += edgeRebaseOffsetDelta
    }
}

const rebaseEdgePropRef = async ({
    edgesRebased,
    propRebaseThreshold,
    propRebaseOffsetDelta,
}: {
    edgesRebased: Map<number, Edge>
    propRebaseThreshold: number
    propRebaseOffsetDelta: number
}) => {
    for (const edge of edgesRebased.values()) {
        if (edge.nextProp >= propRebaseThreshold)
            edge.nextProp += propRebaseOffsetDelta
    }
}

const rebasePropPropRef = async ({
    propsRebased,
    propRebaseThreshold,
    propRebaseOffsetDelta,
}: {
    propsRebased: Map<number, Prop>
    propRebaseThreshold: number
    propRebaseOffsetDelta: number
}) => {
    for (const prop of propsRebased.values()) {
        if (prop.nextProp >= propRebaseThreshold)
            prop.nextProp += propRebaseOffsetDelta
    }
}

const applyUpdates = async <T extends Part>(
    tx: Tx,
    {
        first,
        second,
        baseline,
    }: {
        first: Map<number, T>
        second: Map<number, T>
        baseline: Map<number, T>
    },
    merge: (tx: Tx, baseline: T, first: Part, second: T) => Promise<T>
) => {
    let updated: Map<number, T>
    if (first.size > 0 && second.size > 0) {
        updated = new Map(first)
        for (const secondPart of second.values()) {
            if (first.has(secondPart.offset)) {
                const baselineVertex = baseline.get(secondPart.offset)
                const firstVertex = first.get(secondPart.offset)
                const merged = await merge(
                    tx,
                    baselineVertex,
                    firstVertex,
                    secondPart
                )
                updated.set(secondPart.offset, merged)
            } else {
                updated.set(secondPart.offset, secondPart)
            }
        }
    } else if (first.size > 0) {
        updated = new Map(first)
    } else {
        updated = new Map(second)
    }
    return updated
}

export { merge, MergePolicyEnum }
