import {
    linkCodecFactory,
    LinkCodec,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { graphStoreFactory } from '../graph-store'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import * as assert from 'assert'
import { VersionStore, versionStoreFactory } from '../version-store'
import {
    Item,
    ItemList,
    itemListFactory,
    ItemListTransaction,
    ItemRef,
    ItemValue,
    mergeItemLists,
    readonlyItemList,
} from '../item-list'

describe('revise and merge item list', function () {
    test('simple itemxmx', async () => {
        const { chunk } = chunkerFactory(512, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
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

        enum KeyTypes {
            NAME = 1,
        }
        const itemListOrig: ItemList = itemListFactory(versionStore, graphStore)
        const tx = itemListOrig.tx()
        await tx.start()
        await tx.push(new Map([[KeyTypes.NAME, 'item 0']]))
        await tx.push(new Map([[KeyTypes.NAME, 'item 1']]))
        await tx.push(new Map([[KeyTypes.NAME, 'item 2']]))
        const { root: original } = await tx.commit({})

        /**
         * Revise original, first user
         */

        const graphStore1 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const itemList1: ItemList = itemListFactory(versionStore, graphStore1)
        const tx1 = itemList1.tx()
        await tx1.start()
        await tx1.push(new Map([[KeyTypes.NAME, 'item user1']]))
        const { root: first } = await tx1.commit({})

        /**
         * Revise original, second user
         */
        versionStore.checkout(original)

        const graphStore2 = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const itemList2: ItemList = itemListFactory(versionStore, graphStore2)
        const tx2 = itemList2.tx()
        await tx2.start()
        await tx2.push(new Map([[KeyTypes.NAME, 'item user2']]))
        const { root: second } = await tx2.commit({})

        const {
            root: mergeRoot,
            index: mergeIndex,
            blocks: mergeBlocks,
        } = await mergeItemLists(
            {
                baseRoot: original,
                baseStore: blockStore,
                currentRoot: first,
                currentStore: blockStore,
                otherRoot: second,
                otherStore: blockStore,
            },
            chunk,
            linkCodec,
            valueCodec
        )

        const versionStoreNew: VersionStore = await versionStoreFactory({
            versionRoot: mergeRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const graphStoreNew = graphStoreFactory({
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const itemListMerged: ItemList = itemListFactory(
            versionStoreNew,
            graphStoreNew
        )

        const len = await itemListMerged.length()

        assert.strictEqual(5, len)

        const item0 = await itemListMerged.get(0)
        assert.strictEqual('item 0', item0.value.get(KeyTypes.NAME))

        const item1 = await itemListMerged.get(1)
        assert.strictEqual('item 1', item1.value.get(KeyTypes.NAME))

        const item2 = await itemListMerged.get(2)
        assert.strictEqual('item 2', item2.value.get(KeyTypes.NAME))

        const item3 = await itemListMerged.get(3)
        assert.strictEqual('item user2', item3.value.get(KeyTypes.NAME))

        const item4 = await itemListMerged.get(4)
        assert.strictEqual('item user1', item4.value.get(KeyTypes.NAME))
    })
})
