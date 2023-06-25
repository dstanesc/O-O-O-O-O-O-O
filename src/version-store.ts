import { BlockStore } from './block-store'
import { BlockCodec, LinkCodec, ValueCodec } from './codecs'
import { Block, Link, RootIndex, Version, VersionDetails } from './types'
import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { VersionDecoder, VersionEncoder } from './serde'
import { v4 as uuidV4, parse as uuidParse } from 'uuid'

import { blockIndexFactory } from './block-index'
import { MergePolicyEnum, merge } from './merge'
import { graphPackerFactory } from './graph-packer'

const { create, readAll } = chunkyStore()

interface VersionStoreDiff {
    last: Version | undefined
    common: Version[]
    missing: Version[]
}

interface VersionStore {
    id: () => string

    versionStoreRoot: () => Link

    currentRoot: () => Link

    rootSet: ({
        root,
        index,
        parent,
        mergeParent,
    }: {
        root: Link
        index?: RootIndex
        parent?: Link
        mergeParent?: Link
    }) => Promise<Link>

    versionSet: ({
        version,
        index,
    }: {
        version: Version
        index?: RootIndex
    }) => Promise<Link | undefined>

    rootGet: () => Promise<{ root: Link; index: RootIndex } | undefined>

    versionGet: () => Promise<
        { version: Version; index: RootIndex } | undefined
    >

    checkout: (root: Link) => void

    log: () => Version[]

    blocksExtract: () => Promise<{
        root: any
        index: {
            indexStruct: {
                startOffsets: Map<number, any>
                indexSize: number
                byteArraySize: number
            }
            indexBuffer: Uint8Array
        }
        blocks: { cid: any; bytes: Uint8Array }[]
    }>

    diff: (other: VersionStore) => VersionStoreDiff

    packMissingBlocks: (
        other: VersionStore,
        otherBlockStore: BlockStore
    ) => Promise<Block>

    mergeVersions: (
        other: VersionStore
    ) => Promise<{ root: Link; index: RootIndex; blocks: Block[] }>
}

const VERSION_UNDEFINED = { version: undefined, index: undefined }
const ROOT_UNDEFINED = { root: undefined, index: undefined }

const versionStoreFactory = async ({
    readOnly = false,
    storeRoot,
    versionRoot,
    chunk,
    linkCodec,
    valueCodec,
    blockStore,
}: {
    readOnly?: boolean
    storeRoot?: Link
    versionRoot?: Link
    chunk: (buffer: Uint8Array) => Uint32Array
    linkCodec: LinkCodec
    valueCodec: ValueCodec
    blockStore: BlockStore
}): Promise<VersionStore> => {
    const versions = new Map<string, Version>()
    const indices = new Map<string, RootIndex>()
    let identity: Link
    let byteArrayRoot: Link
    let currentVersion: Link
    const { buildRootIndex } = blockIndexFactory({
        linkCodec,
        blockStore,
    })
    const versionStoreRoot = () => byteArrayRoot
    const id = () => identity.toString()
    const log = () => {
        const versionArray = Array.from(versions.values())
        return versionArray.reverse()
    }
    const init = async (storeRoot?: Link): Promise<void> => {
        if (storeRoot !== undefined) {
            const bytes = await readAll({
                root: storeRoot,
                decode: linkCodec.decode,
                get: blockStore.get,
            })
            const { id: storeId, versions: versionArray } = new VersionDecoder(
                bytes,
                linkCodec.decode,
                valueCodec.decode
            ).read()
            byteArrayRoot = storeRoot
            versionArray.forEach((v) => versions.set(v.root.toString(), v))
            identity = storeId
            currentVersion = versionArray[versionArray.length - 1].root
        } else {
            const bytes = uuidParse(uuidV4())
            const buffer = new Uint8Array(16)
            buffer.set(bytes, 0)
            identity = await linkCodec.encode(buffer)
        }
    }
    const versionSet = async ({
        version,
        index,
    }: {
        version: Version
        index?: RootIndex
    }): Promise<Link | undefined> => {
        if (index === undefined) {
            const { index: indexBuilt } = await buildRootIndex(version.root)
            index = indexBuilt
        }
        versions.set(version.root.toString(), version)
        indices.set(version.root.toString(), index)
        currentVersion = version.root

        if (!readOnly) {
            const { root, blocks } = await blocksExtract()
            byteArrayRoot = root
            for (const block of blocks) {
                await blockStore.put(block)
            }
            return root
        } else return undefined
    }

    const blocksExtract = async (): Promise<{
        root: any
        index: {
            indexStruct: {
                startOffsets: Map<number, any>
                indexSize: number
                byteArraySize: number
            }
            indexBuffer: Uint8Array
        }
        blocks: { cid: any; bytes: Uint8Array }[]
    }> => {
        const buf = new VersionEncoder(
            identity,
            Array.from(versions.values()),
            valueCodec.encode
        ).write()
        const { root, index, blocks } = await create({
            buf,
            chunk,
            encode: linkCodec.encode,
        })
        return { root, index, blocks }
    }

    const versionGet = async (): Promise<{
        version: Version
        index: RootIndex
    }> => {
        if (currentVersion !== undefined) {
            const version: Version = versions.get(currentVersion.toString())
            const index: RootIndex = indices.get(currentVersion.toString())
            if (index !== undefined) return { version, index }
            else {
                const { index } = await buildRootIndex(version.root)
                return { version, index }
            }
        } else return VERSION_UNDEFINED
    }

    const rootSet = async ({
        root,
        index,
        parent,
        mergeParent,
    }: {
        root: Link
        index?: RootIndex
        parent?: Link
        mergeParent?: Link
    }): Promise<Link> => {
        const existingVersion = versions.get(root.toString())
        if (existingVersion !== undefined) {
            currentVersion = existingVersion.root
            return existingVersion.root
        } else {
            const details: VersionDetails = { timestamp: Date.now() }
            const version: Version = { root, details }
            if (parent !== undefined) {
                version.parent = parent
            }
            if (mergeParent !== undefined) {
                version.mergeParent = mergeParent
            }
            return await versionSet({ version, index })
        }
    }

    const rootGet = async (): Promise<{ root: Link; index: RootIndex }> => {
        const { version, index } = await versionGet()
        return version === undefined
            ? ROOT_UNDEFINED
            : { root: version.root, index }
    }

    const checkout = (root: Link) => {
        if (versions.has(root.toString())) {
            currentVersion = root
        } else throw new Error(`Unknown version ${root.toString()}`)
    }

    const diff = (other: VersionStore): VersionStoreDiff => {
        if (other.id() !== id())
            throw new Error(
                `Cannot compare version stores with different identities ${other.id()} !== ${id()}`
            )
        const otherLog = other.log()
        let missing: Version[] = []
        let common: Version[] = []
        for (const version of otherLog) {
            if (versions.has(version.root.toString())) {
                common.push(version)
            } else {
                missing.push(version)
            }
        }
        return {
            last: common.length > 0 ? common[0] : undefined,
            common,
            missing,
        }
    }

    const loadBlocks = async (
        cids: Set<any>,
        otherBlockStore: BlockStore
    ): Promise<Block[]> => {
        const out: Block[] = []
        for (const cid of cids) {
            out.push({ cid, bytes: await otherBlockStore.get(cid) })
        }
        return out
    }

    const findMissingBlocks = async (
        other: VersionStore,
        otherBlockStore: BlockStore
    ): Promise<Set<any>> => {
        const { last, common, missing }: VersionStoreDiff = diff(other)
        const out = new Set()
        const { buildRootIndex } = blockIndexFactory({
            linkCodec,
            blockStore: otherBlockStore,
        })
        for (const version of missing) {
            out.add(version.root)
            const { index: rootIndex } = await buildRootIndex(version.root)
            out.add(rootIndex.vertexRoot)
            out.add(rootIndex.edgeRoot)
            out.add(rootIndex.propRoot)
            out.add(rootIndex.valueRoot)
            out.add(rootIndex.indexRoot)
            for (const cid of rootIndex.vertexIndex.indexStruct.startOffsets.values()) {
                out.add(cid)
            }
            for (const cid of rootIndex.edgeIndex.indexStruct.startOffsets.values()) {
                out.add(cid)
            }
            for (const cid of rootIndex.propIndex.indexStruct.startOffsets.values()) {
                out.add(cid)
            }
            for (const cid of rootIndex.valueIndex.indexStruct.startOffsets.values()) {
                out.add(cid)
            }
            for (const cid of rootIndex.indexIndex.indexStruct.startOffsets.values()) {
                out.add(cid)
            }
        }
        return out
    }

    const packMissingBlocks = async (
        other: VersionStore,
        otherBlockStore: BlockStore
    ): Promise<Block> => {
        const { packRandomBlocks } = graphPackerFactory(linkCodec)
        const missing: Set<any> = await findMissingBlocks(
            other,
            otherBlockStore
        )
        const blocks: Block[] = await loadBlocks(missing, otherBlockStore)
        const bundle: Block = await packRandomBlocks(blocks)
        return bundle
    }

    const mergeVersions = async (
        other: VersionStore
    ): Promise<{ root: Link; index: RootIndex; blocks: Block[] }> => {
        const { last, common, missing }: VersionStoreDiff = diff(other)
        if (missing.length > 0) {
            const first = currentVersion
            const second = other.currentRoot()
            const { version: otherVersion, index: otherIndex } =
                await other.versionGet()
            if (
                otherVersion.parent !== undefined &&
                otherVersion.parent.toString() === first.toString()
            ) {
                // fast forward
                versionSet({ version: otherVersion, index: otherIndex })
                const { extractVersionBlocks } = graphPackerFactory(linkCodec)
                const otherBlocks: Block[] = await extractVersionBlocks(
                    { root: second, index: otherIndex },
                    blockStore
                )
                return { root: second, index: otherIndex, blocks: otherBlocks }
            } else {
                const { root, index, blocks } = await merge(
                    {
                        baseRoot: last.root,
                        baseStore: blockStore,
                        currentRoot: first,
                        currentStore: blockStore,
                        otherRoot: second,
                        otherStore: blockStore,
                    },
                    MergePolicyEnum.MultiValueRegistry,
                    chunk,
                    linkCodec,
                    valueCodec
                )
                rootSet({ root, index, parent: first, mergeParent: second })
                return { root, index, blocks }
            }
        } else throw new Error(`Nothing to merge`)
    }

    await init(storeRoot)

    if (versionRoot !== undefined) {
        await rootSet({ root: versionRoot })
    }

    return {
        id,
        currentRoot: () => currentVersion,
        versionStoreRoot,
        versionSet,
        versionGet,
        rootSet,
        rootGet,
        checkout,
        log,
        blocksExtract,
        diff,
        packMissingBlocks,
        mergeVersions,
    }
}

export { versionStoreFactory, VersionStore, VersionStoreDiff }
