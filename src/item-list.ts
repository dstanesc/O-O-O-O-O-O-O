import { BlockStore } from './block-store'
import { LinkCodec, ValueCodec } from './codecs'
import { Graph, Tx } from './graph'
import { GraphStore, graphStoreFactory } from './graph-store'
import { MergePolicyEnum, merge } from './merge'
import { OFFSET_INCREMENTS } from './serde'
import { Signer } from './trust'
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
    ExtRef,
} from './types'
import { VersionStore, versionStoreFactory } from './version-store'

interface ItemList {
    tx: () => ItemListTransaction
    get: (index: number) => Promise<Item>
    range: (startIndex: number, itemCount: number) => Promise<Item[]>
    length: () => Promise<number>
}

type ItemValue = Map<number, any>

interface ItemRef {
    index: number
    offset: number
}

interface Item {
    value: ItemValue
    ref: ItemRef
}

const itemListFactory = (
    versionStore: VersionStore,
    graphStore: GraphStore
): ItemList => {
    const graph = new Graph(versionStore, graphStore)
    const tx = (): ItemListTransaction => {
        return new ItemListTransaction(graph.tx())
    }
    const get = async (index: number): Promise<Item> => {
        const offset = index * OFFSET_INCREMENTS.VERTEX_INCREMENT
        const ref = { index, offset }
        const vertex: Vertex = await graph.getVertex(offset)
        const props: Prop[] = await graph.getVertexProps(vertex)
        const value = new Map<number, any>()
        for (const prop of props) {
            value.set(prop.key, prop.value)
        }
        return { ref, value }
    }
    const range = async (
        startIndex: number,
        itemCount: number
    ): Promise<Item[]> => {
        const items: Item[] = []
        const vertices: Vertex[] = await graph.getVertexRange(
            startIndex * OFFSET_INCREMENTS.VERTEX_INCREMENT,
            itemCount
        )
        let index = startIndex
        for (const vertex of vertices) {
            const props: Prop[] = await graph.getVertexProps(vertex)
            const value = new Map<number, any>()
            for (const prop of props) {
                value.set(prop.key, prop.value)
            }
            const ref = { index, offset: vertex.offset }
            items.push({ ref, value })
            index++
        }
        return items
    }

    const length = async (): Promise<number> => {
        const { root, index } = await versionStore.rootGet()
        const { vertexOffset } = await graphStore.offsetsGet({ root, index })
        return vertexOffset / OFFSET_INCREMENTS.VERTEX_INCREMENT
    }

    return { tx, get, range, length }
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
        signer,
    }: {
        comment?: Comment
        tags?: Tag[]
        signer?: Signer
    }): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> {
        return await this.tx.commit({ comment, tags, signer })
    }
}

const readonlyItemList = async ({
    versionRoot,
    chunk,
    linkCodec,
    valueCodec,
    blockStore,
}: {
    versionRoot: Link
    chunk: (buffer: Uint8Array) => Uint32Array
    linkCodec: LinkCodec
    valueCodec: ValueCodec
    blockStore: BlockStore
}): Promise<ItemList> => {
    const readonlyVersionStore = await versionStoreFactory({
        readOnly: true,
        versionRoot,
        chunk,
        linkCodec,
        valueCodec,
        blockStore,
    })
    const readonlyGraphStore = graphStoreFactory({
        chunk,
        linkCodec,
        valueCodec,
        blockStore,
    })
    const readonlyItemList: ItemList = itemListFactory(
        readonlyVersionStore,
        readonlyGraphStore
    )
    return readonlyItemList
}

const mergeItemLists = async (
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
    chunk: (buffer: Uint8Array) => Uint32Array,
    linkCodec: LinkCodec,
    valueCodec: ValueCodec
): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> => {
    const { root, index, blocks } = await merge(
        {
            baseRoot,
            baseStore,
            currentRoot,
            currentStore,
            otherRoot,
            otherStore,
        },
        MergePolicyEnum.MultiValueRegistry,
        chunk,
        linkCodec,
        valueCodec
    )

    return { root, index, blocks }
}

export {
    ItemList,
    ItemValue,
    ItemRef,
    Item,
    ItemListTransaction,
    itemListFactory,
    readonlyItemList,
    mergeItemLists,
}
