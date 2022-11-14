


import { Graph, Tx } from "../graph"

import { Edge, EdgeRef, Prop, PropValue, KeyType, Vertex, VertexRef, VertexType, EdgeType, PropType, Link, RootIndex, Block, Part, Ref, Type, IndexType } from "../types"
import { EdgePathElem, ExtractPathElem, PathElem, PathElemType, PropPredicate, VertexPathElem, navigateVertices, navigateEdges, NavigationThreshold } from "../navigate"


interface Navigation {
    path: PathElem[]
    request: NavigationThreshold
    refs: Ref[]
    navigator?: NavigateWrapper
}

abstract class NavigateWrapper {

    graph: Graph
    navigation: Navigation

    constructor(graph: Graph, navigation: Navigation) {
        this.graph = graph
        this.navigation = navigation
        navigation.navigator = this
    }

    abstract merge(previous: PathElem, current: PathElem): PathElem

    mergePush(pathElem: PathElem) {
        const path = this.navigation.path
        if (path.length > 0) {
            let previous = path[path.length - 1]
            if (previous.elemType === pathElem.elemType) {
                previous = this.navigation.path.pop()
                pathElem = this.merge(previous, pathElem)
            }
        }
        this.navigation.path.push(pathElem)
    }

    values(...keys: KeyType[]): NavigateWrapper {
        const pathElem: ExtractPathElem = {
            elemType: PathElemType.EXTRACT,
            props: keys
        }
        this.navigation.path.push(pathElem)
        return this
    }

    maxResults(value: number): NavigateWrapper {
        this.navigation.request = new NavigationThreshold(value)
        return this
    }

    async * exec(): AsyncGenerator<Part, void, void> {
        const navigator: NavigateWrapper = this.navigation.navigator
        const refs = this.navigation.refs
        yield* navigator.navigate(refs)
    }

    abstract navigate(refs: Ref[]): AsyncGenerator<Part, void, void>

}

class NavigateVertexWrapper extends NavigateWrapper {

    constructor(graph: Graph, navigation: Navigation) {
        super(graph, navigation)
    }

    merge(previous: PathElem, current: PathElem): PathElem {

        const p = previous as VertexPathElem
        const c = current as VertexPathElem

        const r: VertexPathElem = { elemType: PathElemType.VERTEX, types: undefined, propPredicate: undefined }

        if (p.types === undefined && c.types === undefined) {
            // ok
        } else if (p.types !== undefined && c.types === undefined)
            r.types = p.types
        else if (p.types === undefined && c.types !== undefined)
            r.types = c.types
        else
            r.types = p.types.filter(type => c.types.includes(type))

        if (p.propPredicate === undefined && c.propPredicate === undefined) {
            // ok
        } else if (p.propPredicate !== undefined && c.propPredicate === undefined)
            r.propPredicate = p.propPredicate
        else if (p.propPredicate === undefined && c.propPredicate !== undefined)
            r.propPredicate = c.propPredicate
        else
            throw new Error(`Cannot merge prop predicates`)
        //r.propPredicate = p.propPredicate && c.propPredicate

        return r
    }


    hasType(...vertexTypes: VertexType[]): NavigateVertexWrapper {
        const pathElem: VertexPathElem = {
            elemType: PathElemType.VERTEX,
            types: vertexTypes.length === 0 ? undefined : vertexTypes,
        }
        this.mergePush(pathElem)
        return this
    }

    has(vertexType: VertexType, propPredicate: PropPredicate): NavigateVertexWrapper {
        const pathElem: VertexPathElem = {
            elemType: PathElemType.VERTEX,
            types: [vertexType],
            propPredicate
        }
        this.mergePush(pathElem)
        return this
    }

    outE(...edgeTypes: EdgeType[]): NavigateEdgeWrapper {
        const pathElem: EdgePathElem = {
            elemType: PathElemType.EDGE,
            types: edgeTypes.length === 0 ? undefined : edgeTypes,
        }
        this.navigation.path.push(pathElem)
        return new NavigateEdgeWrapper(this.graph, this.navigation)
    }

    out(...edgeTypes: EdgeType[]): NavigateVertexWrapper {
        return this.outE(...edgeTypes).inV()
    }

    async * navigate(refs: VertexRef[]): AsyncGenerator<Part, void, void> {
        if (this.navigation.request === undefined)
            this.navigation.request = new NavigationThreshold(100)
        yield* navigateVertices(this.graph, refs, { path: this.navigation.path, request: this.navigation.request })
    }
}

class NavigateEdgeWrapper extends NavigateWrapper {

    constructor(graph: Graph, navigation: Navigation) {
        super(graph, navigation)
    }

    merge(previous: PathElem, current: PathElem): PathElem {

        const p = previous as EdgePathElem
        const c = current as EdgePathElem

        const r: EdgePathElem = { elemType: PathElemType.EDGE, types: undefined, propPredicate: undefined }

        if (p.types === undefined && c.types === undefined) {
            // ok
        } else if (p.types !== undefined && c.types === undefined)
            r.types = p.types
        else if (p.types === undefined && c.types !== undefined)
            r.types = c.types
        else
            r.types = p.types.filter(type => c.types.includes(type))

        if (p.propPredicate === undefined && c.propPredicate === undefined) {
            // ok
        } else if (p.propPredicate !== undefined && c.propPredicate === undefined)
            r.propPredicate = p.propPredicate
        else if (p.propPredicate === undefined && c.propPredicate !== undefined)
            r.propPredicate = c.propPredicate
        else throw new Error(`Cannot merge prop predicates`)
        // r.propPredicate = p.propPredicate && c.propPredicate

        return r
    }

    hasType(...edgeTypes: EdgeType[]): NavigateEdgeWrapper {
        const pathElem: EdgePathElem = {
            elemType: PathElemType.EDGE,
            types: edgeTypes,
        }
        this.mergePush(pathElem)
        return this
    }

    has(edgeType: EdgeType, propPredicate: PropPredicate): NavigateEdgeWrapper {
        const pathElem: EdgePathElem = {
            elemType: PathElemType.EDGE,
            types: [edgeType],
            propPredicate
        }
        this.mergePush(pathElem)
        return this
    }

    inV(...vertexTypes: VertexType[]): NavigateVertexWrapper {
        const pathElem: VertexPathElem = {
            elemType: PathElemType.VERTEX,
            types: vertexTypes.length === 0 ? undefined : vertexTypes,
        }
        this.navigation.path.push(pathElem)
        return new NavigateVertexWrapper(this.graph, this.navigation)
    }

    async * navigate(refs: EdgeRef[]): AsyncGenerator<Part, void, void> {
        if (this.navigation.request === undefined)
            this.navigation.request = new NavigationThreshold(100)
        yield* navigateEdges(this.graph, refs, { path: this.navigation.path, request: this.navigation.request })
    }
}


class CreateWrapper {
    tx: Tx
    props: { keyType: KeyType, value: PropValue, type?: PropType }[]
    constructor(tx: Tx) {
        this.tx = tx
        this.props = []
    }
}

class VertexCreateWrapper extends CreateWrapper {
    vertex: Vertex
    vertexType: VertexType
    constructor(tx: Tx, vertexType: VertexType) {
        super(tx)
        this.vertexType = vertexType
    }

    async next(): Promise<VertexCreateWrapper> {
        this.vertex = this.tx.addVertex(this.vertexType)
        for (const prop of this.props) {
            await this.tx.addVertexProp(this.vertex, prop.keyType, prop.value, prop.type)
        }
        return this
    }

    property(keyType: KeyType, value: PropValue, type?: PropType): VertexCreateWrapper {
        this.props.push({ keyType, value, type })
        return this
    }

    async uniqueIndex(keyType: KeyType, indexType?: IndexType): Promise<VertexCreateWrapper> {
        await this.tx.uniqueIndex(this.vertex, keyType, indexType)
        return this
    }

    get offset() {
        return this.vertex.offset
    }

    get type() {
        return this.vertexType
    }
}

class EdgeCreateWrapper extends CreateWrapper {

    edge: Edge
    edgeType: EdgeType
    fromVertex: VertexCreateWrapper
    toVertex: VertexCreateWrapper

    constructor(tx: Tx, edgeType: EdgeType) {
        super(tx)
        this.edgeType = edgeType
    }

    from(fromVertex: VertexCreateWrapper): EdgeCreateWrapper {
        this.fromVertex = fromVertex
        return this
    }

    to(toVertex: VertexCreateWrapper): EdgeCreateWrapper {
        this.toVertex = toVertex
        return this
    }

    property(keyType: KeyType, value: PropValue, type?: PropType): EdgeCreateWrapper {
        this.props.push({ keyType, value, type })
        return this
    }

    async next(): Promise<EdgeCreateWrapper> {
        if (this.fromVertex === undefined) throw new Error("from vertex needs defined before")
        if (this.toVertex === undefined) throw new Error("to vertex needs defined before")
        this.edge = await this.tx.addEdge(this.fromVertex.vertex, this.toVertex.vertex, this.edgeType)

        for (const prop of this.props) {
            await this.tx.addEdgeProp(this.edge, prop.keyType, prop.value, prop.type)
        }
        return this
    }

    get offset() {
        return this.edge.offset
    }

    get type() {
        return this.edgeType
    }
}

class ProtoGremlinTransaction {
    tx: Tx

    constructor(tx: Tx) {
        this.tx = tx
    }

    addV(vertexType?: VertexType): VertexCreateWrapper {
        return new VertexCreateWrapper(this.tx, vertexType)
    }

    addE(edgeType?: EdgeType): EdgeCreateWrapper {
        return new EdgeCreateWrapper(this.tx, edgeType)
    }

    async commit(): Promise<{ root: Link, index: RootIndex, blocks: Block[] }> {
        const result = await this.tx.commit()
        return result
    }
}


class ProtoGremlin {
    graph: Graph
    constructor(graph: Graph) {
        this.graph = graph
    }

    V(refs: VertexRef[]): NavigateVertexWrapper {
        const pathElem: EdgePathElem = {
            elemType: PathElemType.VERTEX,
        }
        return new NavigateVertexWrapper(this.graph, { path: [pathElem], request: undefined, refs })
    }

    E(refs: EdgeRef[]): NavigateEdgeWrapper {
        const pathElem: EdgePathElem = {
            elemType: PathElemType.EDGE,
        }
        return new NavigateEdgeWrapper(this.graph, { path: [pathElem], request: undefined, refs })
    }

    async tx(): Promise<ProtoGremlinTransaction> {
        const tx: Tx = await this.graph.tx().start()
        return new ProtoGremlinTransaction(tx)
    }

    // For debug only
    async allVertices() {
        return await this.graph.allVertices()
    }

    // For debug only
    async allEdges() {
        return await this.graph.allEdges()
    }
    // For debug only
    async allProps() {
        return await this.graph.allProps()
    }
}


export { ProtoGremlin, ProtoGremlinTransaction, NavigateVertexWrapper, NavigateEdgeWrapper } 