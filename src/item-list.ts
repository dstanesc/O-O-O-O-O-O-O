import { Graph, Tx } from './graph'
import { OFFSET_INCREMENTS } from './serde'
import {
    Version,
    RootIndex,
    Link,
    Vertex,
    Edge,
    Prop,
    Index,
    Offset,
    Block,
    Tag,
    Comment,
} from './types'

interface ItemList {
    tx: () => ItemListTransaction
    get: (index: number) => Promise<ItemValue>
    length: () => Promise<number>
    all: () => Promise<ItemValue[]>
}

type ItemValue = Map<number, any>

interface ItemRef {
    index: number
    offset: number
}

const itemListFactory = (
    {
        versionSet,
        rootGet,
    }: {
        versionSet: ({
            version,
            index,
        }: {
            version: Version
            index: RootIndex
        }) => Promise<Link>
        rootGet: () => Promise<{ root: Link; index: RootIndex }>
    },
    {
        vertexGet,
        edgeGet,
        propGet,
        indexGet,
        offsetsGet,
        verticesAll,
        edgesAll,
        propsAll,
        commit,
    }: {
        vertexGet: (
            { root, index }: { root: Link; index: RootIndex },
            offset: number
        ) => Promise<Vertex>
        edgeGet: (
            { root, index }: { root: Link; index: RootIndex },
            offset: number
        ) => Promise<Edge>
        propGet: (
            { root, index }: { root: Link; index: RootIndex },
            offset: number
        ) => Promise<Prop>
        indexGet: (
            { root, index }: { root: Link; index: RootIndex },
            offset: number
        ) => Promise<Index>
        offsetsGet: ({
            root,
            index,
        }: {
            root: Link
            index: RootIndex
        }) => Promise<{
            vertexOffset: Offset
            edgeOffset: Offset
            propOffset: Offset
            indexOffset: Offset
        }>
        verticesAll: ({
            root,
            index,
        }: {
            root: Link
            index: RootIndex
        }) => Promise<Vertex[]>
        edgesAll: ({
            root,
            index,
        }: {
            root: Link
            index: RootIndex
        }) => Promise<Edge[]>
        propsAll: ({
            root,
            index,
        }: {
            root: Link
            index: RootIndex
        }) => Promise<Prop[]>
        commit: (
            { root, index }: { root: Link; index: RootIndex },
            {
                vertices,
                edges,
                props,
            }: {
                vertices: {
                    added: Map<number, Vertex>
                    updated: Map<number, Vertex>
                }
                edges: {
                    added: Map<number, Edge>
                    updated: Map<number, Edge>
                }
                props: {
                    added: Map<number, Prop>
                    updated: Map<number, Prop>
                }
                indices: {
                    added: Map<number, Index>
                    updated: Map<number, Index>
                }
            }
        ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>
    }
): ItemList => {
    const graph = new Graph(
        {
            versionSet,
            rootGet,
        },
        {
            vertexGet,
            edgeGet,
            propGet,
            indexGet,
            offsetsGet,
            verticesAll,
            edgesAll,
            propsAll,
            commit,
        }
    )

    const tx = (): ItemListTransaction => {
        return new ItemListTransaction(graph.tx())
    }
    const get = async (index: number): Promise<ItemValue> => {
        const offset = index * OFFSET_INCREMENTS.VERTEX_INCREMENT
        const vertex: Vertex = await graph.getVertex(offset)
        const props: Prop[] = await graph.getVertexProps(vertex)
        const itemValue = new Map<number, any>()
        for (const prop of props) {
            itemValue.set(prop.key, prop.value)
        }
        return itemValue
    }
    const length = async (): Promise<number> => {
        const { root, index } = await rootGet()
        const { vertexOffset } = await offsetsGet({ root, index })
        return vertexOffset / OFFSET_INCREMENTS.VERTEX_INCREMENT
    }
    const all = async (): Promise<ItemValue[]> => {
        const { root, index } = await rootGet()
        const vertices: Vertex[] = await verticesAll({ root, index })
        const items: ItemValue[] = []
        for (const vertex of vertices) {
            const props: Prop[] = await graph.getVertexProps(vertex)
            const itemValue = new Map<number, any>()
            for (const prop of props) {
                itemValue.set(prop.key, prop.value)
            }
            items.push(itemValue)
        }
        return items
    }

    return { tx, get, length, all }
}

enum ItemTypes {
    GENERIC = 1,
}

enum RlshpTypes {
    PARENT = 1,
}

class ItemListTransaction {
    tx: Tx
    current: Vertex | undefined
    constructor(tx: Tx) {
        this.tx = tx
    }

    async push(item: ItemValue): Promise<ItemRef> {
        const vertex = this.tx.addVertex(ItemTypes.GENERIC)
        for (const [key, value] of item) {
            this.tx.addVertexProp(vertex, key, value)
        }
        if (this.current) {
            await this.tx.addEdge(vertex, this.current, RlshpTypes.PARENT)
        }
        this.current = vertex
        const offset = vertex.offset
        const index = offset / OFFSET_INCREMENTS.VERTEX_INCREMENT
        return { index, offset }
    }

    async start(): Promise<ItemListTransaction> {
        await this.tx.start()
        return this
    }

    async commit({
        comment,
        tags,
    }: {
        comment?: Comment
        tags?: Tag[]
    }): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> {
        return this.tx.commit({ comment, tags })
    }
}

export { ItemList, ItemValue, ItemRef, ItemListTransaction, itemListFactory }
