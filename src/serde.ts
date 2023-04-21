import { fastCloneVersionDetails } from './clone'
import {
    Vertex,
    Edge,
    Prop,
    Ref,
    Type,
    PropValue,
    Link,
    Block,
    Index,
    IndexedValue,
    Status,
    Part,
    Version,
    VersionDetails,
    Tag,
    Offset,
    ValueRef,
    PropRef,
    PropRecord,
} from './types'

const REF_EXISTS = 0x1
const TYPE_EXISTS = 0x1
const LINK_EXISTS = 0x1

class BinaryEncoder {
    buffer: Uint8Array
    cursor: number
    constructor(size: number) {
        this.buffer = new Uint8Array(size)
        this.cursor = 0
    }

    content() {
        return this.buffer.subarray(0, this.cursor) // identical w/ fixed size buffer
    }

    /*
     * String, fixed size, first 4 bytes hold the actual size
     */
    // writeStringFixedSize(value: string, size: number) {  // size + 4
    //     const bytes = pack(value)
    //     if (bytes.length > size) throw new Error(`String too large - ${bytes.length}, max allowed ${size} bytes`)
    //     const length = bytes.length
    //     this.writeUInt(length)
    //     const fixedSizeBuffer = new Uint8Array(size)
    //     fixedSizeBuffer.set(bytes, 0)
    //     return this.writeBytes(fixedSizeBuffer)
    // }

    writeByte(byte: number) {
        this.buffer[this.cursor] = byte
        this.cursor += 1
    }

    writeBytes(bytes: Uint8Array) {
        const start = this.cursor
        this.buffer.set(bytes, start)
        this.cursor += bytes.byteLength
        return this.cursor
    }

    /*
     * Unsigned 32-bit integer, little endian
     */
    writeUInt(value: number) {
        if (value < 0 || value > 0xffffffff)
            throw new Error('Integer out of range')
        const start = this.cursor
        this.buffer[start] = value & 0xff
        this.buffer[start + 1] = value >>> 8
        this.buffer[start + 2] = value >>> 16
        this.buffer[start + 3] = value >>> 24
        this.cursor += 4
        return this.cursor
    }

    /*
     *  32-bit integer, little endian
     */
    writeInt(value: number) {
        if (value < -0x80000000 || value > 0x7fffffff)
            throw new Error('Integer out of range')
        const start = this.cursor
        this.buffer[start] = value & 0xff
        this.buffer[start + 1] = value >>> 8
        this.buffer[start + 2] = value >>> 16
        this.buffer[start + 3] = value >>> 24
        this.cursor += 4
        return this.cursor
    }

    writeReserved(flags: number) {
        return this.writeByte(flags)
    }

    writeRefExists() {
        let flags = 0
        flags |= REF_EXISTS
        return this.writeByte(flags)
    }

    writeOffset(offset: number) {
        //if (this.cursor !== offset) throw new Error(`Writing invalid offset ${offset}, current offset is ${this.cursor}`)
        return this.writeUInt(offset)
    }

    writeRef(ref: Ref) {
        // 5
        this.writeRefExists() // 1
        return this.writeUInt(ref) // 4
    }

    writeTypeExists() {
        let flags = 0
        flags |= TYPE_EXISTS
        return this.writeByte(flags)
    }

    writeStatus(flag: Status) {
        let flags = 0
        flags |= flag
        return this.writeByte(flags)
    }

    writeType(type: Type) {
        //5
        this.writeTypeExists() // 1
        return this.writeUInt(type) // 4
    }

    writeLinkExists() {
        let flags = 0
        flags |= LINK_EXISTS
        return this.writeByte(flags)
    }

    writeLink(link: Link) {
        this.writeBytes(link.bytes)
    }

    skipBytes(length: number) {
        this.cursor += length
        return this.cursor
    }

    skipReserved() {
        return this.skipBytes(1)
    }

    skipUInt() {
        // 4
        return this.skipBytes(4)
    }

    skipRef() {
        // 5
        return this.skipBytes(5)
    }

    skipType() {
        // 5
        return this.skipBytes(5)
    }
}

class BinaryDecoder {
    buffer: Uint8Array
    cursor: number
    constructor(buffer: Uint8Array) {
        this.buffer = buffer
        this.cursor = 0
    }
    readByte() {
        const start = this.cursor
        this.cursor += 1
        return this.buffer[start]
    }

    readBytes(length: number) {
        const start = this.cursor
        this.cursor += length
        const bytes = this.buffer.subarray(start, this.cursor)
        return bytes
    }

    // readStringFixedSize(bufferSize: number) {
    //     const stringSize = this.readUInt()
    //     const stringBytes = this.readBytes(stringSize)
    //     this.cursor += bufferSize - stringSize
    //     return unpack(stringBytes)
    // }

    /*
     * Unsigned 32-bit integer, little endian
     */
    readUInt() {
        const start = this.cursor
        const value =
            (this.buffer[start] |
                (this.buffer[start + 1] << 8) |
                (this.buffer[start + 2] << 16)) +
            this.buffer[start + 3] * 0x1000000
        this.cursor += 4
        return value
    }

    /*
     * 32-bit integer, little endian
     */
    readInt() {
        const start = this.cursor
        const value =
            this.buffer[start] |
            (this.buffer[start + 1] << 8) |
            (this.buffer[start + 2] << 16) |
            (this.buffer[start + 3] << 24)
        this.cursor += 4
        return value
    }

    readOffset() {
        return this.readUInt()
    }

    readRefExists() {
        // 1
        return this.readByte()
    }

    readTypeExists() {
        // 1
        return this.readByte()
    }

    readLinkExists() {
        // 1
        return this.readByte()
    }

    readStatus(): Status {
        // 1
        const flags = this.readByte()
        return flags as Status
    }

    readRef() {
        // 5
        const flags = this.readRefExists() // 1
        if (flags & REF_EXISTS) {
            return this.readOffset() // 4
        } else {
            this.skipUInt()
            return undefined
        }
    }

    readType() {
        // 5
        const flags = this.readTypeExists() // 1
        if (flags & TYPE_EXISTS) {
            return this.readUInt() // 4
        } else {
            this.skipUInt()
            return undefined
        }
    }

    readLink(decode: (bytes: Uint8Array) => Link) {
        const bytes = this.readBytes(36)
        return decode(bytes)
    }

    readOptionalLink(decode: (bytes: Uint8Array) => Link): Link | undefined {
        const flags = this.readLinkExists() // 1
        if (flags & LINK_EXISTS) {
            return this.readLink(decode)
        } else {
            this.skipBytes(36)
            return undefined
        }
    }

    skipBytes(length: number) {
        this.cursor += length
    }

    skipReserved() {
        // 1
        return this.skipBytes(1)
    }

    skipUInt() {
        // 4
        return this.skipBytes(4)
    }

    skipRef() {
        // 5
        return this.skipBytes(5)
    }

    skipType() {
        // 5
        return this.skipBytes(5)
    }
}

const VERTEX_SIZE_BYTES = 25

class VertexEncoder extends BinaryEncoder {
    vertices: Vertex[]
    constructor(vertices: Vertex[]) {
        super(vertices.length * VERTEX_SIZE_BYTES)
        this.vertices = vertices
    }

    writeVertex(vertex: Vertex) {
        // 20
        this.writeOffset(vertex.offset) // 4
        if (vertex.type !== undefined)
            // 5
            this.writeType(vertex.type)
        else this.skipType()
        if (vertex.nextEdge !== undefined)
            // 5
            this.writeRef(vertex.nextEdge)
        else this.skipRef()
        if (vertex.nextProp !== undefined)
            // 5
            this.writeRef(vertex.nextProp)
        else this.skipRef()
        if (vertex.nextIndex !== undefined)
            // 5
            this.writeRef(vertex.nextIndex)
        else this.skipRef()
        this.writeStatus(vertex.status) // 1
    }

    write() {
        for (const vertex of this.vertices) this.writeVertex(vertex)
        return this.content()
    }
}

class VertexDecoder extends BinaryDecoder {
    readVertex(): Vertex {
        const offset = this.readOffset()
        const type = this.readType()
        const nextEdge = this.readRef()
        const nextProp = this.readRef()
        const nextIndex = this.readRef()
        const status = this.readStatus()
        const vertex: Vertex = { status, offset }
        if (type !== undefined) vertex.type = type
        if (nextEdge !== undefined) vertex.nextEdge = nextEdge
        if (nextProp !== undefined) vertex.nextProp = nextProp
        if (nextIndex !== undefined) vertex.nextIndex = nextIndex
        return vertex
    }

    read(): Vertex[] {
        if (this.buffer.byteLength % VERTEX_SIZE_BYTES !== 0)
            throw new Error('Invalid vertex serialization')
        const size = Math.trunc(this.buffer.byteLength / VERTEX_SIZE_BYTES)
        const vertices = []
        for (let i = 0; i < size; i++) {
            vertices.push(this.readVertex())
        }
        return vertices
    }
}

const EDGE_SIZE_BYTES = 45

class EdgeEncoder extends BinaryEncoder {
    edges: Edge[]
    constructor(edges: Edge[]) {
        super(edges.length * EDGE_SIZE_BYTES)
        this.edges = edges
    }
    writeEdge(edge: Edge) {
        this.writeOffset(edge.offset) // 4
        if (edge.type !== undefined)
            // 5
            this.writeType(edge.type)
        else this.skipType()
        this.writeRef(edge.source) // 5
        this.writeRef(edge.target) // 5
        if (edge.sourcePrev !== undefined)
            // 5
            this.writeRef(edge.sourcePrev)
        else this.skipRef()
        if (edge.sourceNext !== undefined)
            // 5
            this.writeRef(edge.sourceNext)
        else this.skipRef()
        if (edge.targetPrev !== undefined)
            // 5
            this.writeRef(edge.targetPrev)
        else this.skipRef()
        if (edge.targetNext !== undefined)
            // 5
            this.writeRef(edge.targetNext)
        else this.skipRef()
        if (edge.nextProp !== undefined)
            // 5
            this.writeRef(edge.nextProp)
        else this.skipRef()
        this.writeStatus(edge.status) // 1
    }
    write() {
        for (const edge of this.edges) this.writeEdge(edge)
        return this.content()
    }
}

class EdgeDecoder extends BinaryDecoder {
    readEdge(): Edge {
        const offset = this.readOffset()
        const type = this.readType()
        const source = this.readRef()
        const target = this.readRef()
        const sourcePrev = this.readRef()
        const sourceNext = this.readRef()
        const targetPrev = this.readRef()
        const targetNext = this.readRef()
        const nextProp = this.readRef()
        const status = this.readStatus()
        const edge: Edge = { status, offset, source, target }

        if (type !== undefined) edge.type = type
        if (sourcePrev !== undefined) edge.sourcePrev = sourcePrev
        if (sourceNext !== undefined) edge.sourceNext = sourceNext
        if (targetPrev !== undefined) edge.targetPrev = targetPrev
        if (targetNext !== undefined) edge.targetNext = targetNext
        if (nextProp !== undefined) edge.nextProp = nextProp

        return edge
    }

    read(): Edge[] {
        if (this.buffer.byteLength % EDGE_SIZE_BYTES !== 0)
            throw new Error('Invalid edge serialization')
        const size = Math.trunc(this.buffer.byteLength / EDGE_SIZE_BYTES)
        const edges = []
        for (let i = 0; i < size; i++) {
            edges.push(this.readEdge())
        }
        return edges
    }
}

const PROP_SIZE_BYTES = 32

class PropEncoder extends BinaryEncoder {
    props: Prop[]
    refs: Map<PropRef, ValueRef>
    constructor(props: Prop[], refs: Map<PropRef, ValueRef>) {
        super(props.length * PROP_SIZE_BYTES)
        this.props = props
        this.refs = refs
    }

    async writeValue(propRef: PropRef, value: PropValue) {
        // 13
        const valueRef: ValueRef = this.refs.get(propRef)
        this.writeOffset(propRef) // 4
        if (valueRef !== undefined) {
            this.writeRef(valueRef.ref) // 5
            this.writeUInt(valueRef.length) // 4
        } else {
            this.skipRef()
            this.skipUInt()
        }
    }

    async writeProp(prop: Prop) {
        // 32
        this.writeOffset(prop.offset) // 4
        if (prop.type !== undefined)
            // 5
            this.writeType(prop.type)
        else this.skipType()
        this.writeUInt(prop.key) // 4
        await this.writeValue(prop.offset, prop.value) // 13
        if (prop.nextProp !== undefined)
            // 5
            this.writeRef(prop.nextProp)
        else this.skipRef()
        this.writeStatus(prop.status) // 1
    }

    async write() {
        for (const prop of this.props) await this.writeProp(prop)
        return this.content()
    }
}

class PropRecordDecoder extends BinaryDecoder {
    constructor(buffer: Uint8Array) {
        super(buffer)
    }
    async readPropValueRef(): Promise<ValueRef> {
        const propRef = this.readOffset()
        const ref = this.readRef()
        if (ref !== undefined) {
            const length = this.readUInt()
            const valueRef: ValueRef = { propRef, ref, length }
            return valueRef
        } else {
            this.skipUInt()
            return undefined
        }
    }
    async readProp(): Promise<PropRecord> {
        const offset = this.readOffset() //4
        const type = this.readType() //5
        const key = this.readUInt() //4
        const valueRef = await this.readPropValueRef() // 13
        const nextProp = this.readRef() //5
        const status = this.readStatus() //1
        const propRecord: PropRecord = { status, offset, key, valueRef }
        if (type !== undefined) propRecord.type = type
        if (nextProp !== undefined) propRecord.nextProp = nextProp
        return propRecord
    }

    async read(): Promise<PropRecord[]> {
        if (this.buffer.byteLength % PROP_SIZE_BYTES !== 0)
            throw new Error('Invalid prop serialization')
        const size = Math.trunc(this.buffer.byteLength / PROP_SIZE_BYTES)
        const props = []
        for (let i = 0; i < size; i++) {
            props.push(await this.readProp())
        }
        return props
    }
}

class PropDecoder extends BinaryDecoder {
    valueGet: (ref: ValueRef) => Promise<PropValue>
    constructor(
        buffer: Uint8Array,
        valueGet: (ref: ValueRef) => Promise<PropValue>
    ) {
        super(buffer)
        this.valueGet = valueGet
    }

    async readPropValue(): Promise<PropValue> {
        const propRef = this.readOffset()
        const ref = this.readRef()
        if (ref !== undefined) {
            const length = this.readUInt()
            const valueRef: ValueRef = { propRef, ref, length }
            return await this.valueGet(valueRef)
        } else {
            this.skipUInt()
            return undefined
        }
    }

    async readProp(): Promise<Prop> {
        const offset = this.readOffset() //4
        const type = this.readType() //5
        const key = this.readUInt() //4
        const value = await this.readPropValue() // 13
        const nextProp = this.readRef() //5
        const status = this.readStatus() //1
        const prop: Prop = { status, offset, key, value }
        if (type !== undefined) prop.type = type
        if (nextProp !== undefined) prop.nextProp = nextProp
        return prop
    }

    async read(): Promise<Prop[]> {
        if (this.buffer.byteLength % PROP_SIZE_BYTES !== 0)
            throw new Error('Invalid prop serialization')
        const size = Math.trunc(this.buffer.byteLength / PROP_SIZE_BYTES)
        const props = []
        for (let i = 0; i < size; i++) {
            props.push(await this.readProp())
        }
        return props
    }
}

class PropValueEncoder extends BinaryEncoder {
    props: Prop[]
    valueEncode: (json: any) => Uint8Array
    refOffset: number
    constructor(
        refOffset: number,
        props: Prop[],
        valueEncode: (json: any) => Uint8Array
    ) {
        super(
            props
                .map((prop) => valueEncode(prop.value).byteLength)
                .reduce((a, b) => a + b, 0)
        )
        this.refOffset = refOffset
        this.props = props
        this.valueEncode = valueEncode
    }

    writeValue(propRef: Ref, value: PropValue): ValueRef {
        const bytes: Uint8Array = this.valueEncode(value)
        const ref = this.cursor + this.refOffset
        const length = bytes.byteLength
        this.writeBytes(bytes)
        return { propRef, ref, length }
    }

    writeProp(prop: Prop): ValueRef {
        const valueRef: ValueRef = this.writeValue(prop.offset, prop.value)
        return valueRef
    }

    write(): { buf: Uint8Array; refs: Map<PropRef, ValueRef> } {
        const refs: Map<PropRef, ValueRef> = new Map()
        for (const prop of this.props) {
            const valueRef: ValueRef = this.writeProp(prop)
            refs.set(prop.offset, valueRef)
        }
        const buf = this.content()
        return { buf, refs }
    }
}

class PropValueDecoder extends BinaryDecoder {
    valueDecode: (bytes: Uint8Array) => any
    constructor(buffer: Uint8Array, valueDecode: (bytes: Uint8Array) => any) {
        super(buffer)
        this.valueDecode = valueDecode
    }

    readValue(valueRef: ValueRef): PropValue {
        const bytes = this.readBytes(valueRef.length)
        return this.valueDecode(bytes)
    }

    read(valueRefs: ValueRef[]): Map<PropRef, PropValue> {
        const values: Map<PropRef, PropValue> = new Map()
        for (const valueRef of valueRefs) {
            const value = this.readValue(valueRef)
            values.set(valueRef.propRef, value)
        }
        return values
    }
}

const INDEX_SIZE_BYTES = 56

class IndexEncoder extends BinaryEncoder {
    indices: Index[]
    constructor(indices: Index[]) {
        super(indices.length * INDEX_SIZE_BYTES)
        this.indices = indices
    }

    async writeIndex(index: Index) {
        // 56
        this.writeOffset(index.offset) // 4
        if (index.type !== undefined)
            // 5
            this.writeType(index.type)
        else this.skipType()
        this.writeType(index.key) // 5
        this.writeLink(index.value) // 36
        if (index.nextIndex !== undefined)
            // 5
            this.writeRef(index.nextIndex)
        else this.skipRef()
        this.writeStatus(index.status) // 1
    }

    async write() {
        for (const index of this.indices) await this.writeIndex(index)
        return this.content()
    }
}

class IndexDecoder extends BinaryDecoder {
    linkDecode: (linkBytes: Uint8Array) => Link
    indexDecode: (
        link: Link,
        blockGet: (cid: any) => Promise<Uint8Array>,
        value?: any
    ) => Promise<IndexedValue[]>
    blockGet: (cid: any) => Promise<Uint8Array>

    constructor(
        buffer: Uint8Array,
        linkDecode: (linkBytes: Uint8Array) => Link
    ) {
        super(buffer)
        this.linkDecode = linkDecode
    }

    async readIndex(): Promise<Index> {
        const offset = this.readOffset()
        const type = this.readType()
        const key = this.readType()
        const value = this.readLink(this.linkDecode)
        const nextIndex = this.readRef()
        const status = this.readStatus()
        const index: Index = { status, offset, key, value }
        if (type !== undefined) index.type = type
        if (nextIndex !== undefined) index.nextIndex = nextIndex
        return index
    }

    async read(): Promise<Index[]> {
        if (this.buffer.byteLength % INDEX_SIZE_BYTES !== 0)
            throw new Error('Invalid index serialization')
        const size = Math.trunc(this.buffer.byteLength / INDEX_SIZE_BYTES)
        const indices = []
        for (let i = 0; i < size; i++) {
            indices.push(await this.readIndex())
        }
        return indices
    }
}

class RootEncoder extends BinaryEncoder {
    vertexRoot: Link // 36
    vertexOffset: number // 4
    edgeRoot: Link // 36
    edgeOffset: number // 4
    propRoot: Link // 36
    propOffset: number // 4
    valueRoot: Link // 36
    valueOffset: number // 4
    indexRoot: Link // 36
    indexOffset: number // 4

    constructor({
        vertexRoot,
        vertexOffset,
        edgeRoot,
        edgeOffset,
        propRoot,
        propOffset,
        valueRoot,
        valueOffset,
        indexRoot,
        indexOffset,
    }: {
        vertexRoot: Link
        vertexOffset: number
        edgeRoot: Link
        edgeOffset: number
        propRoot: Link
        propOffset: number
        valueRoot: Link
        valueOffset: number
        indexRoot: Link
        indexOffset: number
    }) {
        super(200)
        this.vertexRoot = vertexRoot
        this.vertexOffset = vertexOffset
        this.edgeRoot = edgeRoot
        this.edgeOffset = edgeOffset
        this.propRoot = propRoot
        this.propOffset = propOffset
        this.valueRoot = valueRoot
        this.valueOffset = valueOffset
        this.indexRoot = indexRoot
        this.indexOffset = indexOffset
    }

    write() {
        this.writeUInt(this.vertexOffset) //4
        this.writeUInt(this.edgeOffset) //4
        this.writeUInt(this.propOffset) //4
        this.writeUInt(this.valueOffset) //4
        this.writeUInt(this.indexOffset) //4
        this.writeLink(this.vertexRoot as Link) //36
        this.writeLink(this.edgeRoot as Link) //36
        this.writeLink(this.propRoot as Link) //36
        this.writeLink(this.valueRoot as Link) //36
        this.writeLink(this.indexRoot as Link) //36
        return this
    }
}

class RootDecoder extends BinaryDecoder {
    linkDecode: (bytes: Uint8Array) => Link
    constructor(buffer: Uint8Array, linkDecode: (bytes: Uint8Array) => Link) {
        super(buffer)
        this.linkDecode = linkDecode
    }
    read(): {
        vertexRoot: Link
        vertexOffset: number
        edgeRoot: Link
        edgeOffset: number
        propRoot: Link
        propOffset: number
        valueRoot: Link
        valueOffset: number
        indexRoot: Link
        indexOffset: number
    } {
        const vertexOffset = this.readOffset()
        const edgeOffset = this.readOffset()
        const propOffset = this.readOffset()
        const valueOffset = this.readOffset()
        const indexOffset = this.readOffset()
        const vertexRoot = this.readLink(this.linkDecode)
        const edgeRoot = this.readLink(this.linkDecode)
        const propRoot = this.readLink(this.linkDecode)
        const valueRoot = this.readLink(this.linkDecode)
        const indexRoot = this.readLink(this.linkDecode)
        return {
            vertexRoot,
            vertexOffset,
            edgeRoot,
            edgeOffset,
            propRoot,
            propOffset,
            valueRoot,
            valueOffset,
            indexRoot,
            indexOffset,
        }
    }
}

const VERSION_CONST_SIZE_BYTES = 77
const VERSION_ID_SIZE_BYTES = 36

class VersionEncoder extends BinaryEncoder {
    id: Link
    versions: Version[]
    valueEncode: (json: any) => Uint8Array
    constructor(
        id: Link,
        versions: Version[],
        valueEncode: (json: any) => Uint8Array
    ) {
        super(
            VERSION_ID_SIZE_BYTES +
                versions
                    .map(
                        (version) =>
                            valueEncode(version.details).byteLength +
                            VERSION_CONST_SIZE_BYTES
                    )
                    .reduce((a, b) => a + b, 0)
        )
        this.id = id
        this.versions = versions
        this.valueEncode = valueEncode
    }
    writeDetails(details: VersionDetails) {
        const bytes: Uint8Array = this.valueEncode(details)
        const length = bytes.byteLength
        this.writeUInt(length) // 4
        this.writeBytes(bytes) // n
    }
    async writeVersion(version: Version) {
        this.writeLink(version.root) // 36
        if (version.parent !== undefined) {
            this.writeLinkExists() // 1
            this.writeLink(version.parent) // 36
        } else this.skipBytes(37)
        this.writeDetails(version.details) // 4 + n
    }
    write() {
        this.writeLink(this.id) // 36
        for (const version of this.versions) this.writeVersion(version)
        return this.content()
    }
}

class VersionDecoder extends BinaryDecoder {
    linkDecode: (linkBytes: Uint8Array) => Link
    valueDecode: (bytes: Uint8Array) => any
    constructor(
        buffer: Uint8Array,
        linkDecode: (linkBytes: Uint8Array) => Link,
        valueDecode: (bytes: Uint8Array) => any
    ) {
        super(buffer)
        this.linkDecode = linkDecode
        this.valueDecode = valueDecode
    }

    readVersionDetails(): VersionDetails {
        const length = this.readUInt()
        const bytes = this.readBytes(length)
        return fastCloneVersionDetails(this.valueDecode(bytes))
    }

    readVersion(): Version {
        const root = this.readLink(this.linkDecode)
        const parent = this.readOptionalLink(this.linkDecode)
        const details = this.readVersionDetails()
        return parent === undefined
            ? { root, details }
            : { root, parent, details }
    }

    read(): { id: Link; versions: Version[] } {
        const id: Link = this.readLink(this.linkDecode)
        const versions = []
        while (this.cursor < this.buffer.byteLength)
            versions.push(this.readVersion())
        return { id, versions }
    }
}

const OFFSET_INCREMENTS = {
    VERTEX_INCREMENT: VERTEX_SIZE_BYTES,
    EDGE_INCREMENT: EDGE_SIZE_BYTES,
    PROP_INCREMENT: PROP_SIZE_BYTES,
    INDEX_INCREMENT: INDEX_SIZE_BYTES,
}

export {
    BinaryEncoder,
    BinaryDecoder,
    VertexEncoder,
    VertexDecoder,
    EdgeEncoder,
    EdgeDecoder,
    PropRecordDecoder,
    PropEncoder,
    PropDecoder,
    PropValueEncoder,
    PropValueDecoder,
    IndexEncoder,
    IndexDecoder,
    RootEncoder,
    RootDecoder,
    VersionEncoder,
    VersionDecoder,
    OFFSET_INCREMENTS,
}
