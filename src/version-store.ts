import { BlockStore } from './block-store'
import { BlockCodec, LinkCodec, ValueCodec } from './codecs'
import { Link, RootIndex, Version, VersionDetails } from './types'
import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { VersionDecoder, VersionEncoder } from './serde'
import { v4 as uuidV4, parse as uuidParse } from 'uuid'

import { blockIndexFactory } from './block-index'

const { create, readAll } = chunkyStore()

interface VersionStore {
    id: () => string

    versionStoreRoot: () => Link

    currentRoot: () => Link

    rootSet: ({
        root,
        index,
    }: {
        root: Link
        index?: RootIndex
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
    }: {
        root: Link
        index?: RootIndex
    }): Promise<Link> => {
        const existingVersion = versions.get(root.toString())
        if (existingVersion !== undefined) {
            currentVersion = existingVersion.root
            return existingVersion.root
        } else {
            const details: VersionDetails = { timestamp: Date.now() }
            const version: Version = { root, details }
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
    }
}

export { versionStoreFactory, VersionStore }
