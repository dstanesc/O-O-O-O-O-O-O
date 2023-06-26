import {
    BlockStore,
    MemoryBlockStore,
    memoryBlockStoreFactory,
} from './block-store'
import { LinkCodec, ValueCodec } from './codecs'
import { Graph } from './graph'
import { graphPackerFactory } from './graph-packer'
import { GraphStore, graphStoreFactory } from './graph-store'
import { Block, Link, Version } from './types'
import { VersionStore, versionStoreFactory } from './version-store'
import axios, { AxiosInstance, AxiosResponse, CreateAxiosDefaults } from 'axios'

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
}

type PlumbingStorePushResponse = {
    storeRoot: string
    versionRoot: string
}

type PlumbingGraphPushResponse = {
    versionRoot: string
}

type BasicPushResponse = {
    storeRoot: Link
    versionRoot: Link
    versionsPushed: {
        versionRoot: Link
    }[]
}

interface RelayClientBasic {
    push(versionStoreRoot: Link): Promise<BasicPushResponse>
    pull(
        versionStoreId: string
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
    }: {
        chunk: (buffer: Uint8Array) => Uint32Array
        chunkSize: number
        linkCodec: LinkCodec
        valueCodec: ValueCodec
        blockStore: BlockStore
    },
    config: CreateAxiosDefaults<any>
): RelayClientBasic => {
    const plumbing = relayClientPlumbingFactory(config)
    const {
        packVersionStore,
        restoreSingleIndex: restoreVersionStore,
        packGraphVersion,
        restoreGraphVersion,
    } = graphPackerFactory(linkCodec)

    const push = async (
        versionStoreRoot: Link,
        versionRoot?: Link
    ): Promise<BasicPushResponse> => {
        let versionRoots: Link[]
        if (versionRoot === undefined) {
            const versionStore: VersionStore = await versionStoreFactory({
                storeRoot: versionStoreRoot,
                chunk,
                linkCodec,
                valueCodec,
                blockStore,
            })
            versionRoots = versionStore.log().map((version) => version.root)
        } else {
            versionRoots = [versionRoot]
        }

        const graphVersionBundles: Block[] = []
        for (const root of versionRoots) {
            const graphVersionBundle: Block = await packGraphVersion(
                root,
                blockStore
            )
            graphVersionBundles.push(graphVersionBundle)
        }

        const graphPushResponses: { versionRoot: string }[] = []
        for (const bundle of graphVersionBundles) {
            const response: { versionRoot: string } = await plumbing.graphPush(
                bundle.bytes
            )
            graphPushResponses.push(response)
        }

        const bundle: Block = await packVersionStore(
            versionStoreRoot,
            blockStore,
            chunk,
            valueCodec
        )

        const storePushResponse: { storeRoot: string; versionRoot: string } =
            await plumbing.storePush(chunkSize, bundle.bytes)

        return {
            storeRoot: linkCodec.parseString(storePushResponse.storeRoot),
            versionRoot: linkCodec.parseString(storePushResponse.versionRoot),
            versionsPushed: graphPushResponses.map((response) => ({
                versionRoot: linkCodec.parseString(response.versionRoot),
            })),
        }
    }

    const pull = async (
        versionStoreId: string
    ): Promise<
        | { versionStore: VersionStore; graphStore: GraphStore; graph: Graph }
        | undefined
    > => {
        const storeBytes = await plumbing.storePull(chunkSize, versionStoreId)
        const memoryStore: MemoryBlockStore = memoryBlockStoreFactory()
        const {
            root: versionStoreRoot,
            index: versionStoreIndex,
            blocks: versionStoreBlocks,
        } = await restoreVersionStore(storeBytes, memoryStore)
        const versionStoreTransient: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore: memoryStore,
        })
        const versions: Version[] = versionStoreTransient.log()
        for (const version of versions) {
            try {
                await blockStore.get(version.root)
            } catch (e) {
                const graphVersionBytes = await plumbing.graphPull(
                    version.root.toString()
                )
                if (graphVersionBytes !== undefined) {
                    await restoreGraphVersion(graphVersionBytes, memoryStore)
                }
            }
        }
        await memoryStore.push(blockStore)
        const versionStore: VersionStore = await versionStoreFactory({
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

    return { storePush, storePull, storeResolve, graphPush, graphPull }
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
