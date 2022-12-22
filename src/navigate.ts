import { Graph } from './graph'
import { PredicateFactory, Predicate, Operation } from './ops'
import {
    Edge,
    Vertex,
    Prop,
    Part,
    Ref,
    VertexRef,
    PropRef,
    EdgeRef,
    PropValue,
    KeyType,
    Type,
    IndexType,
    IndexRef,
    Index,
    IndexedValue,
} from './types'

enum PathElemType {
    VERTEX,
    EDGE,
    EXTRACT,
    TEMPLATE,
}

interface PropPredicate {
    keyType: KeyType
    operation: Operation
}

interface IndexPredicate extends PropPredicate {}

interface PathElem {
    elemType: PathElemType
}

interface ReducedValue {
    context: Part
    value: PropValue
}

interface EdgePathElem extends PathElem {
    types?: Type[]
    propPredicate?: PropPredicate
}

interface VertexPathElem extends PathElem {
    types?: Type[]
    propPredicate?: PropPredicate
}

interface ExtractPathElem extends PathElem {
    props: number[]
    reduce?: (previous: PropValue, current: PropValue) => ReducedValue
}

interface TemplatePathElem extends PathElem {
    template: any
}

class RequestBuilder {
    path: PathElem[]

    request: NavigationThreshold

    constructor() {
        this.path = []
    }

    add(elemType: PathElemType): RequestBuilder {
        if (elemType === PathElemType.EXTRACT) {
            throw new Error(
                'For PathElemType.EXTRACT elemType please use the extract(...props) call'
            )
        }
        let elem: PathElem = { elemType: elemType }
        this.path.push(elem)
        return this
    }

    type(...types: Type[]): RequestBuilder {
        const elemType = this.path[this.path.length - 1].elemType
        switch (elemType) {
            case PathElemType.VERTEX:
                ;(this.path[this.path.length - 1] as VertexPathElem).types =
                    types
                break
            case PathElemType.EDGE:
                ;(this.path[this.path.length - 1] as EdgePathElem).types = types
                break
            default:
                throw new Error(`Cannot set predicate to ${elemType}`)
        }
        return this
    }

    propPred(keyType: KeyType, operation: Operation): RequestBuilder {
        const elemType = this.path[this.path.length - 1].elemType
        switch (elemType) {
            case PathElemType.VERTEX:
                ;(
                    this.path[this.path.length - 1] as VertexPathElem
                ).propPredicate = { keyType, operation }
                break
            case PathElemType.EDGE:
                ;(
                    this.path[this.path.length - 1] as EdgePathElem
                ).propPredicate = { keyType, operation }
                break
            default:
                throw new Error(`Cannot set prop predicate to ${elemType}`)
        }
        return this
    }

    extract(...props: number[]): RequestBuilder {
        let elem: ExtractPathElem = { elemType: PathElemType.EXTRACT, props }
        this.path.push(elem)
        return this
    }

    template(template: any): RequestBuilder {
        let elem: TemplatePathElem = {
            elemType: PathElemType.TEMPLATE,
            template,
        }
        this.path.push(elem)
        return this
    }

    reduce(
        reduceFunc: (previous: PropValue, current: PropValue) => ReducedValue
    ): RequestBuilder {
        const elemType = this.path[this.path.length - 1].elemType
        if (elemType !== PathElemType.EXTRACT) {
            throw new Error('Reduce should be applied to PathElemType.EXTRACT')
        }
        ;(this.path[this.path.length - 1] as ExtractPathElem).reduce =
            reduceFunc
        return this
    }

    maxResults(value: number): RequestBuilder {
        this.request = new NavigationThreshold(value)
        return this
    }

    get(): { path: PathElem[]; request: NavigationThreshold } {
        if (this.request === undefined)
            this.request = new NavigationThreshold(100)
        return { path: this.path, request: this.request }
    }
}

class SearchCompleted {}

class NavigationThreshold {
    yieldCounter: number
    maxYield: number
    constructor(maxYield: number) {
        this.yieldCounter = 0
        this.maxYield = maxYield
    }

    increment() {
        this.yieldCounter++
    }
    isDone() {
        return this.yieldCounter === this.maxYield
    }
    completeWhenDone() {
        if (this.isDone()) {
            throw new SearchCompleted()
        }
    }
}

function checkFinished(index: number, path: PathElem[]): boolean {
    return index === path.length - 1
}

function isExtractPathElem(index: number, path: PathElem[]): boolean {
    const pathElem: PathElem = path[index]
    return pathElem.elemType === PathElemType.EXTRACT
}

function isVertexPathElem(index: number, path: PathElem[]): boolean {
    const pathElem: PathElem = path[index]
    return pathElem.elemType === PathElemType.VERTEX
}

function isEdgePathElem(index: number, path: PathElem[]): boolean {
    const pathElem: PathElem = path[index]
    return pathElem.elemType === PathElemType.EDGE
}

function isTemplatePathElem(index: number, path: PathElem[]): boolean {
    const pathElem: PathElem = path[index]
    return pathElem.elemType === PathElemType.TEMPLATE
}

async function lookaheadIndexedPredicate(
    graph: Graph,
    vertex: Vertex,
    path: PathElem[],
    index: number
): Promise<{ propPredicate: PropPredicate; indexStruct: Index } | undefined> {
    const pathElem: PathElem = path[index]
    let found: { propPredicate: PropPredicate; indexStruct: Index } = undefined
    if (pathElem !== undefined) {
        if (pathElem.elemType === PathElemType.VERTEX) {
            const vertexPathElement = pathElem as VertexPathElem
            const propPredicate: PropPredicate = vertexPathElement.propPredicate
            if (propPredicate !== undefined) {
                const indexStruct = await graph.matchAnyIndex(
                    vertex,
                    propPredicate.keyType
                )
                if (indexStruct !== undefined) {
                    found = { propPredicate, indexStruct }
                }
            }
        }
    }
    return found
}

async function* yieldVertexOrGoDeeper(
    index: number,
    path: PathElem[],
    vertex: Vertex,
    graph: Graph,
    request: NavigationThreshold
): AsyncGenerator<any, void, void> {
    let indexedPredicate = await lookaheadIndexedPredicate(
        graph,
        vertex,
        path,
        index + 2
    )
    if (checkFinished(index, path)) {
        request.increment()
        yield vertex
        request.completeWhenDone()
    } else if (isExtractPathElem(index + 1, path))
        yield* navigateProp(
            graph,
            vertex,
            vertex.nextProp,
            path,
            index + 1,
            request
        )
    else if (isTemplatePathElem(index + 1, path)) {
        yield* navigateTemplate(graph, vertex, path, index + 1, request)
    } else if (indexedPredicate !== undefined)
        yield* navigateIndexedEdge(
            graph,
            indexedPredicate,
            path,
            index + 1,
            request
        )
    else yield* navigateEdge(graph, vertex.nextEdge, path, index + 1, request)
}

async function* yieldEdgeOrGoDeeper(
    index: number,
    path: PathElem[],
    edge: Edge,
    graph: Graph,
    request: NavigationThreshold
): AsyncGenerator<any, void, void> {
    if (checkFinished(index, path)) {
        request.increment()
        yield edge
        request.completeWhenDone()
    } else if (isExtractPathElem(index + 1, path))
        yield* navigateProp(
            graph,
            edge,
            edge.nextProp,
            path,
            index + 1,
            request
        )
    else yield* navigateVertex(graph, edge.target, path, index + 1, request)
}

async function* navigateVertices(
    graph: Graph,
    refs: VertexRef[],
    { path, request }: { path: PathElem[]; request: NavigationThreshold }
): AsyncGenerator<any, void, void> {
    const index = 0
    try {
        for (const ref of refs) {
            yield* navigateVertex(graph, ref, path, index, request)
        }
    } catch (e) {
        if (e instanceof SearchCompleted) {
            return
        } else throw e
    }
}

async function* navigateEdges(
    graph: Graph,
    refs: EdgeRef[],
    { path, request }: { path: PathElem[]; request: NavigationThreshold }
): AsyncGenerator<any, void, void> {
    const index = 0
    try {
        for (const ref of refs) {
            yield* navigateEdge(graph, ref, path, index, request)
        }
    } catch (e) {
        if (e instanceof SearchCompleted) {
            return
        } else throw e
    }
}
async function* navigatePropCatchCompleted(
    graph: Graph,
    part: Part,
    ref: PropRef,
    path: PathElem[],
    index: number,
    request: NavigationThreshold
): AsyncGenerator<any, void, void> {
    try {
        yield* navigateProp(graph, part, ref, path, index, request)
    } catch (e) {
        if (e instanceof SearchCompleted) {
            return
        } else throw e
    }
}

async function* navigateEdgeCatchCompleted(
    graph: Graph,
    ref: EdgeRef,
    path: PathElem[],
    index: number,
    request: NavigationThreshold
): AsyncGenerator<any, void, void> {
    try {
        yield* navigateEdge(graph, ref, path, index, request)
    } catch (e) {
        if (e instanceof SearchCompleted) {
            return
        } else throw e
    }
}

async function* navigateVertex(
    graph: Graph,
    ref: VertexRef,
    path: PathElem[],
    index: number,
    request: NavigationThreshold
): AsyncGenerator<any, void, void> {
    const pathElem: VertexPathElem = path[index] as VertexPathElem
    if (ref !== undefined) {
        const vertex: Vertex = await graph.getVertex(ref)
        if (pathElem.types === undefined) {
            if (pathElem.propPredicate === undefined) {
                yield* yieldVertexOrGoDeeper(
                    index,
                    path,
                    vertex,
                    graph,
                    request
                )
            } else if (
                await graph.matchAnyVertexProp(vertex, pathElem.propPredicate)
            ) {
                yield* yieldVertexOrGoDeeper(
                    index,
                    path,
                    vertex,
                    graph,
                    request
                )
            }
        } else if (pathElem.types.includes(vertex.type)) {
            if (pathElem.propPredicate === undefined) {
                yield* yieldVertexOrGoDeeper(
                    index,
                    path,
                    vertex,
                    graph,
                    request
                )
            } else if (
                await graph.matchAnyVertexProp(vertex, pathElem.propPredicate)
            ) {
                yield* yieldVertexOrGoDeeper(
                    index,
                    path,
                    vertex,
                    graph,
                    request
                )
            }
        }
    }
}

async function* navigateEdge(
    graph: Graph,
    ref: EdgeRef,
    path: PathElem[],
    index: number,
    request: NavigationThreshold
): AsyncGenerator<Part, void, void> {
    const pathElem: EdgePathElem = path[index] as EdgePathElem
    if (ref !== undefined) {
        let edge: Edge = await graph.getEdge(ref)
        if (pathElem.types === undefined) {
            if (pathElem.propPredicate === undefined) {
                yield* yieldEdgeOrGoDeeper(index, path, edge, graph, request)
            } else if (
                await graph.matchAnyEdgeProp(edge, pathElem.propPredicate)
            )
                yield* yieldEdgeOrGoDeeper(index, path, edge, graph, request)
        } else if (pathElem.types.includes(edge.type)) {
            if (pathElem.propPredicate === undefined) {
                yield* yieldEdgeOrGoDeeper(index, path, edge, graph, request)
            } else if (
                await graph.matchAnyEdgeProp(edge, pathElem.propPredicate)
            ) {
                yield* yieldEdgeOrGoDeeper(index, path, edge, graph, request)
            }
        }
        yield* navigateEdge(graph, edge.sourceNext, path, index, request)
    }
}

async function* navigateIndexedEdge(
    graph: Graph,
    indexedPredicate: { propPredicate: PropPredicate; indexStruct: Index },
    path: PathElem[],
    index: number,
    request: NavigationThreshold
): AsyncGenerator<Part, void, void> {
    const { value, ref: edgeRef }: IndexedValue = await graph.searchIndex(
        indexedPredicate.indexStruct,
        indexedPredicate.propPredicate
    )
    let edge: Edge = await graph.getEdge(edgeRef)
    yield* yieldEdgeOrGoDeeper(index, path, edge, graph, request)
}

async function* navigateProp(
    graph: Graph,
    partContext: Part,
    ref: PropRef,
    path: PathElem[],
    index: number,
    request: NavigationThreshold
): AsyncGenerator<any, void, void> {
    const pathElem: ExtractPathElem = path[index] as ExtractPathElem
    if (ref !== undefined) {
        let prop: Prop = await graph.getProp(ref)
        if (pathElem.reduce === undefined) {
            if (pathElem.props.includes(prop.key)) {
                request.increment()
                yield prop
                request.completeWhenDone()
            }
            yield* navigateProp(
                graph,
                partContext,
                prop.nextProp,
                path,
                index,
                request
            )
        } else
            yield await navigatePropsAndReduce(
                graph,
                partContext,
                pathElem,
                prop,
                undefined
            )
    }
}

async function navigatePropsAndReduce(
    graph: Graph,
    context: Part,
    pathElem: ExtractPathElem,
    prop: Prop,
    previousReduced: PropValue
): Promise<ReducedValue> {
    const reducedValue: PropValue = pathElem.reduce(previousReduced, prop.value)
    if (prop.nextProp !== undefined) {
        let nextProp: Prop = await graph.getProp(prop.nextProp)
        return navigatePropsAndReduce(
            graph,
            context,
            pathElem,
            nextProp,
            reducedValue
        )
    }
    return { context, value: reducedValue }
}

async function* navigateTemplate(
    graph: Graph,
    vertex: Vertex,
    path: PathElem[],
    index: number,
    request: NavigationThreshold
): AsyncGenerator<any, void, void> {
    const pathElem: TemplatePathElem = path[index] as TemplatePathElem
    const template: any = pathElem.template
    const result = {}
    for (const [key, value] of Object.entries(template)) {
        const elemType = value['$elemType']
        const schemaType = value['$type']
        if (elemType === PathElemType.EXTRACT) {
            let propElem: ExtractPathElem = {
                elemType: PathElemType.EXTRACT,
                props: [schemaType],
            }
            const propPath = [...path.slice(0, index), propElem]
            for await (const prop of navigatePropCatchCompleted(
                graph,
                vertex,
                vertex.nextProp,
                propPath,
                index,
                new NavigationThreshold(Number.MAX_SAFE_INTEGER)
            )) {
                result[key] = prop.value
            }
        } else if (elemType === PathElemType.EDGE) {
            let edgeElem: EdgePathElem = {
                elemType: PathElemType.EDGE,
                types: [schemaType],
            }
            const edgePath = [...path.slice(0, index), edgeElem]
            const nestedResults = []
            for await (const edge of navigateEdgeCatchCompleted(
                graph,
                vertex.nextEdge,
                edgePath,
                index,
                new NavigationThreshold(Number.MAX_SAFE_INTEGER)
            )) {
                const realEdge = edge as Edge
                const vertex: Vertex = await graph.getVertex(realEdge.target)
                const vertexElem: VertexPathElem = {
                    elemType: PathElemType.VERTEX,
                }
                const templateElem: TemplatePathElem = {
                    elemType: PathElemType.TEMPLATE,
                    template: value,
                }
                const valueOnly = value['$value'] !== undefined
                const templateFragmentPath = [vertexElem, templateElem]
                for await (const nestedResult of navigateTemplate(
                    graph,
                    vertex,
                    templateFragmentPath,
                    1,
                    new NavigationThreshold(Number.MAX_SAFE_INTEGER)
                )) {
                    const realNestedResult = valueOnly
                        ? nestedResult.$value
                        : nestedResult
                    nestedResults.push(realNestedResult)
                }
            }
            if (nestedResults.length > 0) {
                result[key] = nestedResults
            }
        }
    }
    request.increment()
    yield result
    request.completeWhenDone()
}

export {
    PathElem,
    EdgePathElem,
    VertexPathElem,
    ExtractPathElem,
    TemplatePathElem,
    PropPredicate,
    PathElemType,
    RequestBuilder,
    NavigationThreshold,
    navigateVertices,
    navigateEdges,
}
