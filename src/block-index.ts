import { RootDecoder } from './serde'
import { Link, RootIndex, BlockIndex, ContentDiff } from './types'
import { chunkyStore } from '@dstanesc/store-chunky-bytes'
import { LinkCodec } from './codecs'
import { BlockStore } from './block-store'

interface BlockIndexFactory {
    buildRootIndex: (
        root: Link
    ) => Promise<{ root: Link; index: RootIndex; indexBuffer: Uint8Array }>
    buildChunkyIndex: (root: Link) => Promise<BlockIndex>
    diffRootIndex: ({
        currentRoot,
        otherRoot,
    }: {
        currentRoot: Link
        otherRoot: Link
    }) => Promise<ContentDiff>
    diffChunkyIndex: ({
        currentIndex,
        otherIndex,
    }: {
        currentIndex: BlockIndex
        otherIndex: BlockIndex
    }) => Promise<ContentDiff>
}

const blockIndexFactory = ({
    linkCodec,
    blockStore,
}: {
    linkCodec: LinkCodec
    blockStore: BlockStore
}): BlockIndexFactory => {
    const {
        encode: linkEncode,
        decode: linkDecode,
        encodeString: encodeLinkString,
        parseString: parseLinkString,
    }: LinkCodec = linkCodec
    const { put: blockPut, get: blockGet } = blockStore
    const { readIndex } = chunkyStore()

    const buildChunkyIndex = async (root: Link): Promise<BlockIndex> => {
        return await readIndex(root, blockGet, linkDecode)
    }

    const buildRootIndex = async (
        root: Link
    ): Promise<{ root: Link; index: RootIndex; indexBuffer: Uint8Array }> => {
        const bytes = await blockGet(root)
        const rootStruct = new RootDecoder(bytes, linkDecode).read()
        const vertexIndex = await buildChunkyIndex(rootStruct.vertexRoot)
        const edgeIndex = await buildChunkyIndex(rootStruct.edgeRoot)
        const propIndex = await buildChunkyIndex(rootStruct.propRoot)
        const valueIndex = await buildChunkyIndex(rootStruct.valueRoot)
        const indexIndex = await buildChunkyIndex(rootStruct.indexRoot)
        const index = Object.assign(
            { vertexIndex, edgeIndex, propIndex, valueIndex, indexIndex },
            rootStruct
        )
        return { root, index, indexBuffer: bytes }
    }

    const diffRootIndex = async ({
        currentRoot,
        otherRoot,
    }: {
        currentRoot: Link
        otherRoot: Link
    }): Promise<ContentDiff> => {
        const currentRootIndex = await buildRootIndex(currentRoot)
        const otherRootIndex = await buildRootIndex(otherRoot)
        const addedLinks: Link[] = []
        const removedLinks: Link[] = []
        const {
            vertexIndex: currentVertexIndex,
            edgeIndex: currentEdgeIndex,
            propIndex: currentPropIndex,
            valueIndex: currentValueIndex,
            indexIndex: currentIndexIndex,
        } = currentRootIndex.index
        const {
            vertexIndex: otherVertexIndex,
            edgeIndex: otherEdgeIndex,
            propIndex: otherPropIndex,
            valueIndex: otherValueIndex,
            indexIndex: otherIndexIndex,
        } = otherRootIndex.index
        const vertexDiff = await diffChunkyIndex({
            currentIndex: currentVertexIndex,
            otherIndex: otherVertexIndex,
        })
        const edgeDiff = await diffChunkyIndex({
            currentIndex: currentEdgeIndex,
            otherIndex: otherEdgeIndex,
        })
        const propDiff = await diffChunkyIndex({
            currentIndex: currentPropIndex,
            otherIndex: otherPropIndex,
        })
        const valueDiff = await diffChunkyIndex({
            currentIndex: currentValueIndex,
            otherIndex: otherValueIndex,
        })
        const indexDiff = await diffChunkyIndex({
            currentIndex: currentIndexIndex,
            otherIndex: otherIndexIndex,
        })
        addedLinks.push(...vertexDiff.added)
        addedLinks.push(...edgeDiff.added)
        addedLinks.push(...propDiff.added)
        addedLinks.push(...valueDiff.added)
        addedLinks.push(...indexDiff.added)
        removedLinks.push(...vertexDiff.removed)
        removedLinks.push(...edgeDiff.removed)
        removedLinks.push(...propDiff.removed)
        removedLinks.push(...valueDiff.removed)
        removedLinks.push(...indexDiff.removed)
        return { added: addedLinks, removed: removedLinks }
    }

    const diffChunkyIndex = async ({
        currentIndex,
        otherIndex,
    }: {
        currentIndex: BlockIndex
        otherIndex: BlockIndex
    }): Promise<ContentDiff> => {
        const addedLinks: Link[] = []
        const removedLinks: Link[] = []
        const currentLinks = new Set(
            Array.from(currentIndex.indexStruct.startOffsets.values(), (cid) =>
                linkCodec.encodeString(cid)
            )
        )
        const otherLinks = new Set(
            Array.from(otherIndex.indexStruct.startOffsets.values(), (cid) =>
                linkCodec.encodeString(cid)
            )
        )
        for (const cidString of otherLinks) {
            if (!currentLinks.has(cidString)) {
                addedLinks.push(linkCodec.parseString(cidString))
            }
        }
        for (const cidString of currentLinks) {
            if (!otherLinks.has(cidString)) {
                removedLinks.push(linkCodec.parseString(cidString))
            }
        }

        return { added: addedLinks, removed: removedLinks }
    }
    return {
        buildRootIndex,
        buildChunkyIndex,
        diffRootIndex,
        diffChunkyIndex,
    }
}

export { blockIndexFactory, BlockIndexFactory }
