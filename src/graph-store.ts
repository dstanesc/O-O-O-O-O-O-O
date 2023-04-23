import {
    Vertex,
    Edge,
    Prop,
    Offset,
    Link,
    Block,
    RootStruct,
    RootIndex,
    Index,
    PropValue,
    ValueRef,
} from './types'

import {
    VertexEncoder,
    VertexDecoder,
    EdgeEncoder,
    EdgeDecoder,
    PropEncoder,
    PropDecoder,
    RootEncoder,
    RootDecoder,
    OFFSET_INCREMENTS,
    IndexDecoder,
    IndexEncoder,
    PropValueEncoder,
    PropValueDecoder,
} from './serde'

import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { BlockCodec, LinkCodec, ValueCodec } from './codecs'
import { BlockStore } from './block-store'
import { graphPackerFactory } from './graph-packer'

interface GraphStore {
    vertexGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Vertex>
    vertexRange: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        vertexCount: number
    ) => Promise<Vertex[]>
    edgeGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Edge>
    edgeRange: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        edgeCount: number
    ) => Promise<Edge[]>
    propGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Prop>
    propRange: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        propCount: number
    ) => Promise<Prop[]>
    indexGet: (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ) => Promise<Index>
    offsetsGet: ({ root, index }: { root: Link; index: RootIndex }) => Promise<{
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
    packGraph: (versionRoot: Link) => Promise<Block>
    packComputed: (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number
    ) => Promise<Block>
}

const { create, read, append, update, bulk, remove, readIndex } = chunkyStore()

const graphStore = ({
    chunk,
    linkCodec,
    valueCodec,
    blockStore,
}: {
    chunk: (buffer: Uint8Array) => Uint32Array
    linkCodec: LinkCodec
    valueCodec: ValueCodec
    blockStore: BlockStore
}): GraphStore => {
    const { encode: linkEncode, decode: linkDecode }: LinkCodec = linkCodec
    const { encode: valueEncode, decode: valueDecode }: ValueCodec = valueCodec
    const { put: blockPut, get: blockGet } = blockStore

    const vertexGet = async (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ): Promise<Vertex> => {
        const { vertexRoot, vertexIndex } = index
        const bytes = await read(offset, OFFSET_INCREMENTS.VERTEX_INCREMENT, {
            root: vertexRoot,
            index: vertexIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new VertexDecoder(bytes).readVertex()
    }

    const vertexRange = async (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        vertexCount: number
    ): Promise<Vertex[]> => {
        const { vertexRoot, vertexIndex } = index
        const bytes = await read(
            offset,
            vertexCount * OFFSET_INCREMENTS.VERTEX_INCREMENT,
            {
                root: vertexRoot,
                index: vertexIndex,
                decode: linkDecode,
                get: blockGet,
            }
        )
        return new VertexDecoder(bytes).read()
    }

    const edgeGet = async (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ): Promise<Edge> => {
        const { edgeRoot, edgeIndex } = index
        const bytes = await read(offset, OFFSET_INCREMENTS.EDGE_INCREMENT, {
            root: edgeRoot,
            index: edgeIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new EdgeDecoder(bytes).readEdge()
    }

    const edgeRange = async (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        edgeCount: number
    ): Promise<Edge[]> => {
        const { edgeRoot, edgeIndex } = index
        const bytes = await read(
            offset,
            edgeCount * OFFSET_INCREMENTS.EDGE_INCREMENT,
            {
                root: edgeRoot,
                index: edgeIndex,
                decode: linkDecode,
                get: blockGet,
            }
        )
        return new EdgeDecoder(bytes).read()
    }

    const propGet = async (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ): Promise<Prop> => {
        const { propRoot, propIndex } = index
        const bytes = await read(offset, OFFSET_INCREMENTS.PROP_INCREMENT, {
            root: propRoot,
            index: propIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new PropDecoder(bytes, (ref: ValueRef) =>
            valueGet({ root, index }, ref)
        ).readProp()
    }

    const propRange = async (
        { root, index }: { root: Link; index: RootIndex },
        offset: number,
        propCount: number
    ): Promise<Prop[]> => {
        const { propRoot, propIndex } = index
        const bytes = await read(
            offset,
            propCount * OFFSET_INCREMENTS.PROP_INCREMENT,
            {
                root: propRoot,
                index: propIndex,
                decode: linkDecode,
                get: blockGet,
            }
        )
        return new PropDecoder(bytes, (ref: ValueRef) =>
            valueGet({ root, index }, ref)
        ).read()
    }

    const valueGet = async (
        { root, index }: { root: Link; index: RootIndex },
        { propRef, ref, length }: ValueRef
    ): Promise<Prop> => {
        const { valueRoot, valueIndex } = index
        const bytes = await read(ref, length, {
            root: valueRoot,
            index: valueIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new PropValueDecoder(bytes, valueDecode).readValue({
            propRef,
            ref,
            length,
        })
    }

    const indexGet = async (
        { root, index }: { root: Link; index: RootIndex },
        offset: number
    ): Promise<Index> => {
        const { indexRoot, indexIndex } = index
        const bytes = await read(offset, OFFSET_INCREMENTS.INDEX_INCREMENT, {
            root: indexRoot,
            index: indexIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new IndexDecoder(bytes, linkDecode).readIndex()
    }

    const verticesAll = async ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }): Promise<Vertex[]> => {
        const { vertexRoot, vertexOffset, vertexIndex } = index
        // TODO Read chunks for scalability
        const bytes = await read(0, vertexOffset, {
            root: vertexRoot,
            index: vertexIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new VertexDecoder(bytes).read()
    }

    const edgesAll = async ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }): Promise<Edge[]> => {
        const { edgeRoot, edgeOffset, edgeIndex } = index
        // TODO Read chunks for scalability
        const bytes = await read(0, edgeOffset, {
            root: edgeRoot,
            index: edgeIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new EdgeDecoder(bytes).read()
    }

    const propsAll = async ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }): Promise<Prop[]> => {
        const { propRoot, propOffset, propIndex } = index
        // TODO Read chunks for scalability
        const bytes = await read(0, propOffset, {
            root: propRoot,
            index: propIndex,
            decode: linkDecode,
            get: blockGet,
        })
        return new PropDecoder(bytes, (ref: ValueRef) =>
            valueGet({ root, index }, ref)
        ).read()
    }

    const offsetsGet = async ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }): Promise<{
        vertexOffset: number
        edgeOffset: number
        propOffset: number
        indexOffset: number
    }> => {
        const { vertexOffset, edgeOffset, propOffset, indexOffset } = index
        return { vertexOffset, edgeOffset, propOffset, indexOffset }
    }

    const verticesCreate = async (vertices: Map<Offset, Vertex>) => {
        const array = Array.from(vertices.values())
        const buf = new VertexEncoder(array).write()
        const { root, index, blocks } = await create({
            buf,
            chunk,
            encode: linkEncode,
        })
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const verticesBulk = async (
        { root: rootOrig, index: indexOrig }: { root: Link; index: any },
        {
            added,
            updated,
        }: { added: Map<number, Vertex>; updated: Map<number, Vertex> }
    ) => {
        const appendBuffer = new VertexEncoder(
            Array.from(added.values())
        ).write()
        const updateRequests: {
            updateBuffer: Uint8Array
            updateStartOffset: number
        }[] = []
        for (const [offset, vertex] of updated) {
            const updateBuffer = new VertexEncoder([vertex]).write()
            updateRequests.push({ updateBuffer, updateStartOffset: offset })
        }
        const { root, index, blocks } = await bulk(
            {
                root: rootOrig,
                decode: linkDecode,
                get: blockGet,
                put: blockPut,
            },
            { chunk, encode: linkEncode },
            appendBuffer,
            updateRequests
        )
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const edgesCreate = async (edges: Map<Offset, Edge>) => {
        const array = Array.from(edges.values())
        const buf = new EdgeEncoder(array).write()
        const { root, index, blocks } = await create({
            buf,
            chunk,
            encode: linkEncode,
        })
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const edgesBulk = async (
        { root: rootOrig, index: indexOrig }: { root: Link; index: any },
        {
            added,
            updated,
        }: { added: Map<number, Edge>; updated: Map<number, Edge> }
    ) => {
        const appendBuffer = new EdgeEncoder(Array.from(added.values())).write()
        const updateRequests: {
            updateBuffer: Uint8Array
            updateStartOffset: number
        }[] = []
        for (const [offset, edge] of updated) {
            const updateBuffer = new EdgeEncoder([edge]).write()
            updateRequests.push({ updateBuffer, updateStartOffset: offset })
        }
        const { root, index, blocks } = await bulk(
            {
                root: rootOrig,
                decode: linkDecode,
                get: blockGet,
                put: blockPut,
            },
            { chunk, encode: linkEncode },
            appendBuffer,
            updateRequests
        )
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const propsCreate = async (
        props: Map<Offset, Prop>,
        refs: Map<Offset, ValueRef>
    ) => {
        const array = Array.from(props.values())
        const buf = await new PropEncoder(array, refs).write()
        const { root, index, blocks } = await create({
            buf,
            chunk,
            encode: linkEncode,
        })
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const propsBulk = async (
        { root: rootOrig, index: indexOrig }: { root: Link; index: any },
        {
            added,
            updated,
        }: { added: Map<number, Prop>; updated: Map<number, Prop> },
        refs: Map<Offset, ValueRef>
    ) => {
        const appendBuffer = await new PropEncoder(
            Array.from(added.values()),
            refs
        ).write()
        const updateRequests: {
            updateBuffer: Uint8Array
            updateStartOffset: number
        }[] = []
        for (const [offset, prop] of updated) {
            const updateBuffer = await new PropEncoder([prop], refs).write()
            updateRequests.push({ updateBuffer, updateStartOffset: offset })
        }
        const { root, index, blocks } = await bulk(
            {
                root: rootOrig,
                decode: linkDecode,
                get: blockGet,
                put: blockPut,
            },
            { chunk, encode: linkEncode },
            appendBuffer,
            updateRequests
        )
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const propsValueCreate = async (props: Map<Offset, Prop>) => {
        const array = Array.from(props.values())
        const { buf, refs } = new PropValueEncoder(
            0,
            array,
            valueCodec.encode
        ).write()
        const { root, index, blocks } = await create({
            buf,
            chunk,
            encode: linkEncode,
        })
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks, refs }
    }

    const propsValueAppend = async (
        { root: rootOrig, index: indexOrig }: { root: Link; index: any },
        {
            added,
            updated,
        }: { added: Map<number, Prop>; updated: Map<number, Prop> }
    ) => {
        if (updated.size > 0)
            throw new Error('property value update not implemented')
        const lastOffset = indexOrig.indexStruct.byteArraySize
        const { buf, refs } = new PropValueEncoder(
            lastOffset,
            Array.from(added.values()),
            valueCodec.encode
        ).write()

        const { root, index, blocks } = await append(
            {
                root: rootOrig,
                index: indexOrig,
                decode: linkDecode,
                get: blockGet,
            },
            { buf, chunk, encode: linkEncode }
        )
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks, refs }
    }

    const indicesCreate = async (indices: Map<Offset, Index>) => {
        const array = Array.from(indices.values())
        const buf = await new IndexEncoder(array).write()
        const { root, index, blocks } = await create({
            buf,
            chunk,
            encode: linkEncode,
        })
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const indicesBulk = async (
        { root: rootOrig, index: indexOrig }: { root: Link; index: any },
        {
            added,
            updated,
        }: { added: Map<number, Index>; updated: Map<number, Index> }
    ) => {
        const appendBuffer = await new IndexEncoder(
            Array.from(added.values())
        ).write()
        const updateRequests: {
            updateBuffer: Uint8Array
            updateStartOffset: number
        }[] = []
        for (const [offset, index] of updated) {
            const updateBuffer = await new IndexEncoder([index]).write()
            updateRequests.push({ updateBuffer, updateStartOffset: offset })
        }
        const { root, index, blocks } = await bulk(
            {
                root: rootOrig,
                decode: linkDecode,
                get: blockGet,
                put: blockPut,
            },
            { chunk, encode: linkEncode },
            appendBuffer,
            updateRequests
        )
        for (const block of blocks) await blockPut(block)
        return { root, index, blocks }
    }

    const commit = async (
        { root: rootOrig, index: indexOrig }: { root: Link; index: RootIndex },
        {
            vertices,
            edges,
            props,
            indices,
        }: {
            vertices: {
                added: Map<number, Vertex>
                updated: Map<number, Vertex>
            }
            edges: { added: Map<number, Edge>; updated: Map<number, Edge> }
            props: { added: Map<number, Prop>; updated: Map<number, Prop> }
            indices: { added: Map<number, Index>; updated: Map<number, Index> }
        }
    ): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> => {
        const {
            root: vertexRootLink,
            index: vertexIndex,
            blocks: vertexBlocks,
        } = rootOrig === undefined
            ? await verticesCreate(vertices.added)
            : await verticesBulk(
                  { root: indexOrig.vertexRoot, index: indexOrig.vertexIndex },
                  vertices
              )

        const {
            root: edgeRootLink,
            index: edgeIndex,
            blocks: edgeBlocks,
        } = rootOrig === undefined
            ? await edgesCreate(edges.added)
            : await edgesBulk(
                  { root: indexOrig.edgeRoot, index: indexOrig.edgeIndex },
                  edges
              )

        const {
            root: valueRootLink,
            index: valueIndex,
            blocks: valueBlocks,
            refs: valueRefs,
        } = rootOrig === undefined
            ? await propsValueCreate(props.added)
            : await propsValueAppend(
                  {
                      root: indexOrig.valueRoot,
                      index: indexOrig.valueIndex,
                  },
                  props
              )

        const {
            root: propRootLink,
            index: propIndex,
            blocks: propBlocks,
        } = rootOrig === undefined
            ? await propsCreate(props.added, valueRefs)
            : await propsBulk(
                  { root: indexOrig.propRoot, index: indexOrig.propIndex },
                  props,
                  valueRefs
              )

        const {
            root: indexRootLink,
            index: indexIndex,
            blocks: indexBlocks,
        } = rootOrig === undefined
            ? await indicesCreate(indices.added)
            : await indicesBulk(
                  { root: indexOrig.indexRoot, index: indexOrig.indexIndex },
                  indices
              )

        const blocks = []

        const rootStruct: RootStruct = {
            vertexRoot: vertexRootLink,
            vertexOffset: vertexIndex.indexStruct.byteArraySize,
            edgeRoot: edgeRootLink,
            edgeOffset: edgeIndex.indexStruct.byteArraySize,
            propRoot: propRootLink,
            propOffset: propIndex.indexStruct.byteArraySize,
            valueRoot: valueRootLink,
            valueOffset: valueIndex.indexStruct.byteArraySize,
            indexRoot: indexRootLink,
            indexOffset: indexIndex.indexStruct.byteArraySize,
        }

        const rootBytes = new RootEncoder(rootStruct).write().content()
        const rootAfter = await linkEncode(rootBytes)
        const rootAfterBlock: Block = { cid: rootAfter, bytes: rootBytes }

        await blockPut(rootAfterBlock)

        blocks.push(...vertexBlocks)
        blocks.push(...edgeBlocks)
        blocks.push(...propBlocks)
        blocks.push(...valueBlocks)
        blocks.push(...indexBlocks)
        blocks.push(rootAfterBlock)

        const indexAfter: RootIndex = {
            vertexRoot: vertexRootLink,
            vertexOffset: vertexIndex.indexStruct.byteArraySize,
            vertexIndex,
            edgeRoot: edgeRootLink,
            edgeOffset: edgeIndex.indexStruct.byteArraySize,
            edgeIndex,
            propRoot: propRootLink,
            propOffset: propIndex.indexStruct.byteArraySize,
            propIndex,
            valueRoot: valueRootLink,
            valueOffset: valueIndex.indexStruct.byteArraySize,
            valueIndex,
            indexRoot: indexRootLink,
            indexOffset: indexIndex.indexStruct.byteArraySize,
            indexIndex,
        }

        return { root: rootAfter, index: indexAfter, blocks }
    }

    const packGraph = async (versionRoot: Link): Promise<Block> => {
        const { packGraph } = graphPackerFactory(linkCodec)
        return await packGraph(versionRoot, blockStore)
    }

    const packComputed = async (
        versionRoot: Link,
        vertexOffsetStart: number,
        vertexCount: number,
        graphDepth: number
    ): Promise<Block> => {
        const { packComputed: pack } = graphPackerFactory(linkCodec)
        return pack(
            versionRoot,
            vertexOffsetStart,
            vertexCount,
            graphDepth,
            blockStore,
            chunk,
            valueCodec
        )
    }

    return {
        commit,
        vertexGet,
        vertexRange,
        edgeGet,
        edgeRange,
        propGet,
        propRange,
        indexGet,
        offsetsGet,
        verticesAll,
        edgesAll,
        propsAll,
        packGraph,
        packComputed,
    }
}

export { graphStore, graphStore as graphStoreFactory, GraphStore }
