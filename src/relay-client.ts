import { blockIndexFactory } from './block-index'
import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from './block-store'
import { LinkCodec, ValueCodec } from './codecs'
import { Graph } from './graph'
import { graphPackerFactory } from './graph-packer'
import { GraphStore, graphStoreFactory } from './graph-store'
import { Block, ContentDiff, Link, Version } from './types'
import { VersionStore, versionStoreFactory } from './version-store'
import axios, {
    AxiosError,
    AxiosInstance,
    AxiosResponse,
    CreateAxiosDefaults,
} from 'axios'

interface RelayClientPlumbing {
    storePush(
        chunkSize: number,
        bytes: Uint8Array
    ): Promise<PlumbingStorePushResponse>
    storePull(
        chunkSize: number,
        versionStoreId: string
    ): Promise<Uint8Array | undefined>
    storeResolve(versionStoreId: string): Promise<string | undefined>
    graphPush(bytes: Uint8Array): Promise<PlumbingGraphPushResponse>
    graphPull(versionRoot: string): Promise<Uint8Array | undefined>
    indexPull(versionRoot: string): Promise<Uint8Array | undefined>
    blocksPush(bytes: Uint8Array): Promise<PlumbingBlocksPushResponse>
    blocksPull(links: string[]): Promise<Uint8Array | undefined>
}

type PlumbingStorePushResponse = {
    storeRoot: string
    versionRoot: string
}

type PlumbingGraphPushResponse = {
    versionRoot: string
}

type PlumbingBlocksPushResponse = {
    blockCount: number
}

type BasicPushResponse = {
    storeRoot: Link
    versionRoot: Link
}

interface RelayClientBasic {
    push(versionStoreRoot: Link, versionRoot?: Link): Promise<BasicPushResponse>
    pull(
        versionStoreId: string,
        localVersionStoreRoot?: Link
    ): Promise<
        | { versionStore: VersionStore; graphStore: GraphStore; graph: Graph }
        | undefined
    >
}

const relayClientBasicFactory = (
    {
        chunk,
        chunkSize,
        linkCodec,
        valueCodec,
        blockStore,
        incremental = false,
    }: {
        chunk: (buffer: Uint8Array) => Uint32Array
        chunkSize: number
        linkCodec: LinkCodec
        valueCodec: ValueCodec
        blockStore: BlockStore
        incremental?: boolean
    },
    config: CreateAxiosDefaults<any>
): RelayClientBasic => {
    const plumbing = relayClientPlumbingFactory(config)
    const {
        packVersionStore,
        restoreSingleIndex: restoreVersionStore,
        packGraphVersion,
        packRootIndex,
        packRandomBlocks,
        restoreGraphVersion,
        restoreRootIndex,
        restoreRandomBlocks,
    } = graphPackerFactory(linkCodec)

    const push = async (
        versionStoreRoot: Link,
        versionRoot?: Link
    ): Promise<BasicPushResponse> => {
        let localVersionRoot: Link
        const localVersionStore: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })
        const localVersionStoreBundle: Block = await packVersionStore(
            versionStoreRoot,
            blockStore,
            chunk,
            valueCodec
        )
        if (versionRoot === undefined) {
            localVersionRoot = localVersionStore.currentRoot()
        } else {
            localVersionRoot = versionRoot
        }
        const graphVersionBundles: Block[] = []
        let remoteVersionStoreBytes: Uint8Array | undefined
        if (incremental) {
            try {
                remoteVersionStoreBytes = await plumbing.storePull(
                    chunkSize,
                    localVersionStore.id()
                )
            } catch (error) {
                if (axios.isAxiosError(error)) {
                    const axiosError: AxiosError = error
                    if (axiosError.response?.status !== 404) {
                        throw error
                    }
                }
            }
        }
        if (remoteVersionStoreBytes !== undefined) {
            const diffStore: MemoryBlockStore = memoryBlockStoreFactory()
            const { root: remoteVersionStoreRoot } = await restoreVersionStore(
                remoteVersionStoreBytes,
                diffStore
            )
            const remoteVersionStore: VersionStore = await versionStoreFactory({
                storeRoot: remoteVersionStoreRoot,
                chunk,
                linkCodec,
                valueCodec,
                blockStore: diffStore,
            })
            const remoteVersionRoot: Link = remoteVersionStore.currentRoot()
            const remoteVersionRoots: Link[] = remoteVersionStore
                .log()
                .map((version) => version.root)

            if (
                remoteVersionRoots
                    .map((root) => linkCodec.encodeString(root))
                    .includes(linkCodec.encodeString(localVersionRoot))
            ) {
                return {
                    storeRoot: versionStoreRoot,
                    versionRoot: localVersionRoot,
                }
            } else {
                const remoteRootIndexBytes = await plumbing.indexPull(
                    linkCodec.encodeString(remoteVersionRoot)
                )
                const { blocks: remoteRootIndexBlocks } =
                    await restoreRootIndex(remoteRootIndexBytes, diffStore)
                const localRootIndexBundle: Block = await packRootIndex(
                    localVersionRoot,
                    blockStore
                )
                const { blocks: localRootIndexBlocks } = await restoreRootIndex(
                    localRootIndexBundle.bytes,
                    diffStore
                )
                const selectedBlocks: Block[] = []
                for (const block of localRootIndexBlocks) {
                    const linkString = linkCodec.encodeString(block.cid)
                    if (
                        !remoteRootIndexBlocks
                            .map((block) => linkCodec.encodeString(block.cid))
                            .includes(linkString)
                    ) {
                        selectedBlocks.push(block)
                    }
                }
                const blockIndexBuilder = blockIndexFactory({
                    linkCodec,
                    blockStore: diffStore,
                })
                const contentDiff: ContentDiff =
                    await blockIndexBuilder.diffRootIndex({
                        currentRoot: remoteVersionRoot,
                        otherRoot: localVersionRoot,
                    })
                for (const link of contentDiff.added) {
                    const bytes = await blockStore.get(link)
                    const block: Block = { cid: link, bytes }
                    selectedBlocks.push(block)
                }
                const diffBundle = await packRandomBlocks(selectedBlocks)
                const blocksPushResponse: { blockCount: number } =
                    await plumbing.blocksPush(diffBundle.bytes)
                if (blocksPushResponse.blockCount !== selectedBlocks.length) {
                    throw new Error(
                        `Failed to push all blocks pushed: ${selectedBlocks.length}, confirmed: ${blocksPushResponse.blockCount}`
                    )
                }
                const storePushResponse: {
                    storeRoot: string
                    versionRoot: string
                } = await plumbing.storePush(
                    chunkSize,
                    localVersionStoreBundle.bytes
                )
                return {
                    storeRoot: linkCodec.parseString(
                        storePushResponse.storeRoot
                    ),
                    versionRoot: linkCodec.parseString(
                        storePushResponse.versionRoot
                    ),
                }
            }
        } else {
            const graphVersionBundle: Block = await packGraphVersion(
                localVersionRoot,
                blockStore
            )
            const graphPushResponse: { versionRoot: string } =
                await plumbing.graphPush(graphVersionBundle.bytes)

            const storePushResponse: {
                storeRoot: string
                versionRoot: string
            } = await plumbing.storePush(
                chunkSize,
                localVersionStoreBundle.bytes
            )

            return {
                storeRoot: linkCodec.parseString(storePushResponse.storeRoot),
                versionRoot: linkCodec.parseString(
                    storePushResponse.versionRoot
                ),
            }
        }
    }

    const pull = async (
        versionStoreId: string,
        localVersionStoreRoot?: Link
    ): Promise<
        | { versionStore: VersionStore; graphStore: GraphStore; graph: Graph }
        | undefined
    > => {
        let remoteVersionStoreBytes: Uint8Array | undefined
        if (incremental && localVersionStoreRoot !== undefined) {
            try {
                remoteVersionStoreBytes = await plumbing.storePull(
                    chunkSize,
                    versionStoreId
                )
            } catch (error) {
                if (axios.isAxiosError(error)) {
                    const axiosError: AxiosError = error
                    if (axiosError.response?.status !== 404) {
                        throw error
                    }
                }
            }
            if (remoteVersionStoreBytes !== undefined) {
                const diffStore: MemoryBlockStore = memoryBlockStoreFactory()
                const { root: remoteVersionStoreRoot } =
                    await restoreVersionStore(
                        remoteVersionStoreBytes,
                        diffStore
                    )
                const remoteVersionStore: VersionStore =
                    await versionStoreFactory({
                        storeRoot: remoteVersionStoreRoot,
                        chunk,
                        linkCodec,
                        valueCodec,
                        blockStore: diffStore,
                    })
                const remoteVersionRoot: Link = remoteVersionStore.currentRoot()
                const localVersionStore: VersionStore =
                    await versionStoreFactory({
                        storeRoot: localVersionStoreRoot,
                        chunk,
                        linkCodec,
                        valueCodec,
                        blockStore,
                    })
                const localVersionRoot = localVersionStore.currentRoot()
                if (
                    linkCodec.encodeString(localVersionRoot) !==
                    linkCodec.encodeString(remoteVersionRoot)
                ) {
                    const remoteRootIndexBytes = await plumbing.indexPull(
                        linkCodec.encodeString(remoteVersionRoot)
                    )
                    const { blocks: remoteRootIndexBlocks } =
                        await restoreRootIndex(remoteRootIndexBytes, diffStore)
                    const localRootIndexBundle: Block = await packRootIndex(
                        localVersionRoot,
                        blockStore
                    )
                    const { blocks: localRootIndexBlocks } =
                        await restoreRootIndex(
                            localRootIndexBundle.bytes,
                            diffStore
                        )
                    const requiredBlockIdentifiers: string[] = []
                    for (const block of remoteRootIndexBlocks) {
                        const linkString = linkCodec.encodeString(block.cid)
                        if (
                            !localRootIndexBlocks
                                .map((block) =>
                                    linkCodec.encodeString(block.cid)
                                )
                                .includes(linkString)
                        ) {
                            requiredBlockIdentifiers.push(linkString)
                        }
                    }
                    const blockIndexBuilder = blockIndexFactory({
                        linkCodec,
                        blockStore: diffStore,
                    })

                    const contentDiff: ContentDiff =
                        await blockIndexBuilder.diffRootIndex({
                            currentRoot: localVersionRoot,
                            otherRoot: remoteVersionRoot,
                        })
                    for (const link of contentDiff.added) {
                        requiredBlockIdentifiers.push(
                            linkCodec.encodeString(link)
                        )
                    }
                    const randomBlocksBundle: Uint8Array | undefined =
                        await plumbing.blocksPull(requiredBlockIdentifiers)

                    if (randomBlocksBundle !== undefined) {
                        const selectedBlocks = await restoreRandomBlocks(
                            randomBlocksBundle,
                            diffStore
                        )
                        const localVersionStoreBundle: Block =
                            await packVersionStore(
                                localVersionStoreRoot,
                                blockStore,
                                chunk,
                                valueCodec
                            )
                        const { root: storeRootExisting } =
                            await restoreVersionStore(
                                localVersionStoreBundle.bytes,
                                diffStore
                            )
                        const graphStoreBundleExisting: Block =
                            await packGraphVersion(localVersionRoot, blockStore)
                        const { root: versionRootExisting } =
                            await restoreGraphVersion(
                                graphStoreBundleExisting.bytes,
                                diffStore
                            )
                        const versionStoreExisting: VersionStore =
                            await versionStoreFactory({
                                storeRoot: localVersionStoreRoot,
                                versionRoot: localVersionRoot,
                                chunk,
                                linkCodec,
                                valueCodec,
                                blockStore: diffStore,
                            })
                        const {
                            root: mergedRoot,
                            index: mergedIndex,
                            blocks: mergedBlocks,
                        } = await versionStoreExisting.mergeVersions(
                            remoteVersionStore
                        )
                        await diffStore.push(blockStore)
                        const mergedVersionRoot =
                            versionStoreExisting.currentRoot()
                        const mergedVersionStoreRoot =
                            versionStoreExisting.versionStoreRoot()
                        const versionStore: VersionStore =
                            await versionStoreFactory({
                                storeRoot: mergedVersionStoreRoot,
                                versionRoot: mergedVersionRoot,
                                chunk,
                                linkCodec,
                                valueCodec,
                                blockStore,
                            })
                        const graphStore = graphStoreFactory({
                            chunk,
                            linkCodec,
                            valueCodec,
                            blockStore,
                        })
                        const graph = new Graph(versionStore, graphStore)
                        return {
                            versionStore,
                            graphStore,
                            graph,
                        }
                    } else {
                        throw new Error(
                            `Failed to pull selected blocks: ${JSON.stringify(
                                requiredBlockIdentifiers
                            )}`
                        )
                    }
                } else {
                    const versionStore = await versionStoreFactory({
                        storeRoot: localVersionStoreRoot,
                        versionRoot: localVersionRoot,
                        chunk,
                        linkCodec,
                        valueCodec,
                        blockStore,
                    })
                    const graphStore = graphStoreFactory({
                        chunk,
                        linkCodec,
                        valueCodec,
                        blockStore,
                    })
                    const graph = new Graph(versionStore, graphStore)
                    return {
                        versionStore,
                        graphStore,
                        graph,
                    }
                }
            } else {
                return undefined
            }
        } else {
            try {
                remoteVersionStoreBytes = await plumbing.storePull(
                    chunkSize,
                    versionStoreId
                )
            } catch (error) {
                if (axios.isAxiosError(error)) {
                    const axiosError: AxiosError = error
                    if (axiosError.response?.status !== 404) {
                        throw error
                    }
                }
            }
            if (remoteVersionStoreBytes !== undefined) {
                const transientStore: MemoryBlockStore =
                    memoryBlockStoreFactory()
                const {
                    root: versionStoreRoot,
                    index: versionStoreIndex,
                    blocks: versionStoreBlocks,
                } = await restoreVersionStore(
                    remoteVersionStoreBytes,
                    transientStore
                )
                const versionStoreRemote: VersionStore =
                    await versionStoreFactory({
                        storeRoot: versionStoreRoot,
                        chunk,
                        linkCodec,
                        valueCodec,
                        blockStore: transientStore,
                    })
                const remoteVersions: Version[] = versionStoreRemote.log()
                for (const version of remoteVersions) {
                    try {
                        await blockStore.get(version.root)
                    } catch (e) {
                        const graphVersionBytes = await plumbing.graphPull(
                            version.root.toString()
                        )
                        if (graphVersionBytes !== undefined) {
                            await restoreGraphVersion(
                                graphVersionBytes,
                                transientStore
                            )
                        }
                    }
                }
                if (localVersionStoreRoot !== undefined) {
                    const localVersionStoreBundle: Block =
                        await packVersionStore(
                            localVersionStoreRoot,
                            blockStore,
                            chunk,
                            valueCodec
                        )
                    const { root: storeRootExisting } =
                        await restoreVersionStore(
                            localVersionStoreBundle.bytes,
                            transientStore
                        )

                    const versionStoreLocal: VersionStore =
                        await versionStoreFactory({
                            storeRoot: localVersionStoreRoot,
                            chunk,
                            linkCodec,
                            valueCodec,
                            blockStore: transientStore,
                        })

                    const localVersions: Version[] = versionStoreLocal.log()
                    for (const version of localVersions) {
                        const localGraphVersionBundle = await packGraphVersion(
                            version.root,
                            blockStore
                        )
                        await restoreGraphVersion(
                            localGraphVersionBundle.bytes,
                            transientStore
                        )
                    }
                    const {
                        root: mergedRoot,
                        index: mergedIndex,
                        blocks: mergedBlocks,
                    } = await versionStoreLocal.mergeVersions(
                        versionStoreRemote
                    )
                    await transientStore.push(blockStore)
                    const mergedVersionRoot = versionStoreLocal.currentRoot()
                    const mergedVersionStoreRoot =
                        versionStoreLocal.versionStoreRoot()
                    const versionStore: VersionStore =
                        await versionStoreFactory({
                            storeRoot: mergedVersionStoreRoot,
                            versionRoot: mergedVersionRoot,
                            chunk,
                            linkCodec,
                            valueCodec,
                            blockStore,
                        })
                    const graphStore = graphStoreFactory({
                        chunk,
                        linkCodec,
                        valueCodec,
                        blockStore,
                    })
                    const graph = new Graph(versionStore, graphStore)
                    return {
                        versionStore,
                        graphStore,
                        graph,
                    }
                } else {
                    await transientStore.push(blockStore)
                    const versionStore: VersionStore =
                        await versionStoreFactory({
                            storeRoot: versionStoreRoot,
                            chunk,
                            linkCodec,
                            valueCodec,
                            blockStore,
                        })
                    const graphStore = graphStoreFactory({
                        chunk,
                        linkCodec,
                        valueCodec,
                        blockStore,
                    })
                    const graph = new Graph(versionStore, graphStore)
                    return {
                        versionStore,
                        graphStore,
                        graph,
                    }
                }
            } else {
                return undefined
            }
        }
    }

    return { push, pull }
}

const relayClientPlumbingFactory = (
    config: CreateAxiosDefaults<any>
): RelayClientPlumbing => {
    const httpClient = axios.create(config)
    const storePush = async (
        chunkSize: number,
        bytes: Uint8Array
    ): Promise<PlumbingStorePushResponse> => {
        const response = await httpClient.put('/store/push', bytes.buffer, {
            params: {
                chunkSize: chunkSize,
            },
            headers: {
                'Content-Type': 'application/octet-stream',
            },
        })
        return response.data
    }

    const storePull = async (
        chunkSize: number,
        versionStoreId: string
    ): Promise<Uint8Array | undefined> => {
        const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
            '/store/pull',
            {
                responseType: 'arraybuffer',
                params: {
                    chunkSize: chunkSize,
                    id: versionStoreId,
                },
            }
        )
        if (response.data) {
            const bytes = new Uint8Array(response.data)
            return bytes
        } else return undefined
    }

    const storeResolve = async (
        versionStoreId: string
    ): Promise<string | undefined> => {
        const response = await httpClient.get('/store/resolve', {
            params: {
                id: versionStoreId,
            },
        })
        return response.data
    }

    const graphPush = async (
        bytes: Uint8Array
    ): Promise<PlumbingGraphPushResponse> => {
        const response = await httpClient.put(
            '/graph/version/push',
            bytes.buffer,
            {
                headers: {
                    'Content-Type': 'application/octet-stream',
                },
            }
        )
        return response.data
    }

    const graphPull = async (
        versionRoot: string
    ): Promise<Uint8Array | undefined> => {
        const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
            '/graph/version/pull',
            {
                responseType: 'arraybuffer',
                params: {
                    id: versionRoot,
                },
            }
        )
        if (response.data) {
            const bytes = new Uint8Array(response.data)
            return bytes
        } else return undefined
    }

    const indexPull = async (
        versionRoot: string
    ): Promise<Uint8Array | undefined> => {
        const response: AxiosResponse<ArrayBuffer> = await httpClient.get(
            '/graph/index/pull',
            {
                responseType: 'arraybuffer',
                params: {
                    id: versionRoot,
                },
            }
        )
        if (response.data) {
            const bytes = new Uint8Array(response.data)
            return bytes
        } else return undefined
    }

    const blocksPush = async (
        bytes: Uint8Array
    ): Promise<PlumbingBlocksPushResponse> => {
        const response = await httpClient.put('/blocks/push', bytes.buffer, {
            headers: {
                'Content-Type': 'application/octet-stream',
            },
        })
        return response.data
    }

    const blocksPull = async (
        links: string[]
    ): Promise<Uint8Array | undefined> => {
        const response: AxiosResponse<ArrayBuffer> = await httpClient.put(
            '/blocks/pull',
            { links },
            {
                responseType: 'arraybuffer',
                headers: {
                    'Content-Type': 'application/json',
                },
            }
        )
        if (response.data) {
            const bytes = new Uint8Array(response.data)
            return bytes
        } else return undefined
    }

    return {
        storePush,
        storePull,
        storeResolve,
        graphPush,
        graphPull,
        indexPull,
        blocksPush,
        blocksPull,
    }
}

export {
    relayClientPlumbingFactory,
    RelayClientPlumbing,
    relayClientBasicFactory,
    RelayClientBasic,
    BasicPushResponse,
    PlumbingGraphPushResponse,
    PlumbingStorePushResponse,
}
