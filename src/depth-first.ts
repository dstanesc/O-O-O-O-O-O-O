import { ElementAccessor } from './graph'
import { VertexRef, Part, Vertex, EdgeRef, Edge, PropRef, Prop } from './types'

interface TraversalVisitor {
    startVertex?: (vertex: Vertex) => Promise<void>
    endVertex?: (vertex: Vertex) => Promise<void>
    startEdge?: (edge: Edge) => Promise<void>
    endEdge?: (edge: Edge) => Promise<void>
    startProp?: (edge: Prop) => Promise<void>
    endProp?: (edge: Prop) => Promise<void>
}

async function traverseVertices(
    accessor: ElementAccessor,
    refs: Iterable<VertexRef>,
    visitor: TraversalVisitor
) {
    const vertexTrace = new Set<VertexRef>()
    for (const ref of refs) {
        await traverseVertex(accessor, ref, vertexTrace, visitor)
    }
}

async function traverseEdges(
    accessor: ElementAccessor,
    refs: Iterable<EdgeRef>,
    vertexTrace: Set<VertexRef>,
    visitor: TraversalVisitor
) {
    for (const ref of refs) {
        await traverseEdge(accessor, ref, vertexTrace, visitor)
    }
}

async function traverseVertex(
    accessor: ElementAccessor,
    ref: VertexRef,
    vertexTrace: Set<VertexRef>,
    visitor: TraversalVisitor
) {
    if (ref !== undefined) {
        if (!vertexTrace.has(ref)) {
            const vertex: Vertex = await accessor.getVertex(ref)
            if (visitor.startVertex !== undefined)
                await visitor.startVertex(vertex)
            if (vertex.nextEdge !== undefined)
                await traverseEdge(
                    accessor,
                    vertex.nextEdge,
                    vertexTrace,
                    visitor
                )
            if (
                visitor.startProp !== undefined &&
                vertex.nextProp !== undefined
            )
                await traverseProp(accessor, vertex.nextProp, visitor)
            if (visitor.endVertex !== undefined) await visitor.endVertex(vertex)
        }
    }
}

async function traverseEdge(
    accessor: ElementAccessor,
    ref: EdgeRef,
    vertexTrace: Set<VertexRef>,
    visitor: TraversalVisitor
) {
    if (ref !== undefined) {
        const edge: Edge = await accessor.getEdge(ref)
        if (visitor.startEdge !== undefined) await visitor.startEdge(edge)
        if (visitor.startProp !== undefined && edge.nextProp !== undefined)
            await traverseProp(accessor, edge.nextProp, visitor)
        if (edge.target !== undefined)
            // true
            await traverseVertex(accessor, edge.target, vertexTrace, visitor)
        if (edge.sourceNext !== undefined)
            await traverseEdge(accessor, edge.sourceNext, vertexTrace, visitor)
        if (visitor.endEdge !== undefined) await visitor.endEdge(edge)
    }
}

async function traverseProp(
    accessor: ElementAccessor,
    ref: PropRef,
    visitor: TraversalVisitor
) {
    if (ref !== undefined) {
        const prop: Prop = await accessor.getProp(ref)
        if (visitor.startProp !== undefined) await visitor.startProp(prop)
        if (prop.nextProp !== undefined)
            await traverseProp(accessor, prop.nextProp, visitor)
        if (visitor.endProp !== undefined) await visitor.endProp(prop)
    }
}

export {
    TraversalVisitor,
    traverseVertices,
    traverseEdges,
    traverseVertex,
    traverseEdge,
    traverseProp,
}
