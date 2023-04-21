import { blockIndexFactory } from './block-index'
import { BlockStore } from './block-store'
import { LinkCodec } from './codecs'
import { Link } from './types'

/**
 * Provides ability to load and push complete versions from and to another block store.
 */
interface StagingBlockStore extends BlockStore {
    pushVersion: (versionRoot: Link, otherStore: BlockStore) => Promise<void>
    loadVersion: (versionRoot: Link, fromStore: BlockStore) => Promise<void>
}

const stagingBlockStoreFactory = (
    linkCodec: LinkCodec,
    stage: BlockStore
): StagingBlockStore => {
    const { put, get } = stage

    const pushMany = async (
        cids: Iterable<any>,
        otherStore: BlockStore
    ): Promise<void> => {
        for (const cid of cids) {
            const bytes = await get(cid)
            await otherStore.put({ cid, bytes })
        }
    }

    const loadMany = async (
        cids: Iterable<any>,
        fromStore: BlockStore
    ): Promise<void> => {
        for (const cid of cids) {
            const bytes = await fromStore.get(cid)
            await put({ cid, bytes })
        }
    }

    const pushVersion = async (
        versionRoot: Link,
        otherStore: BlockStore
    ): Promise<void> => {
        const { buildRootIndex } = blockIndexFactory({
            linkCodec,
            blockStore: stage,
        })
        const { index } = await buildRootIndex(versionRoot)
        const {
            vertexIndex,
            edgeIndex,
            propIndex,
            valueIndex,
            indexIndex,
            vertexRoot,
            edgeRoot,
            propRoot,
            valueRoot,
            indexRoot,
        } = index

        const indexes = [
            vertexIndex,
            edgeIndex,
            propIndex,
            valueIndex,
            indexIndex,
        ]

        const roots = [vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot]

        for (const idx of indexes) {
            const { startOffsets } = idx.indexStruct
            await pushMany(startOffsets.values(), otherStore)
        }

        for (const root of roots) {
            await pushMany([root], otherStore)
        }
        await pushMany([versionRoot], otherStore)
    }

    const loadVersion = async (
        versionRoot: Link,
        fromStore: BlockStore
    ): Promise<void> => {
        const { buildRootIndex } = blockIndexFactory({
            linkCodec,
            blockStore: fromStore,
        })
        const { index } = await buildRootIndex(versionRoot)
        const {
            vertexIndex,
            edgeIndex,
            propIndex,
            valueIndex,
            indexIndex,
            vertexRoot,
            edgeRoot,
            propRoot,
            valueRoot,
            indexRoot,
        } = index

        const indexes = [
            vertexIndex,
            edgeIndex,
            propIndex,
            valueIndex,
            indexIndex,
        ]

        const roots = [vertexRoot, edgeRoot, propRoot, valueRoot, indexRoot]

        for (const index of indexes) {
            const { startOffsets } = index.indexStruct
            await loadMany(startOffsets.values(), fromStore)
        }

        for (const root of roots) {
            await loadMany([root], fromStore)
        }

        await loadMany([versionRoot], fromStore)
    }

    return { get, put, pushVersion, loadVersion }
}

export { StagingBlockStore, stagingBlockStoreFactory }
