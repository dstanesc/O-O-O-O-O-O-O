import {
    linkCodecFactory,
    blockCodecFactory,
    BlockCodec,
    LinkCodec,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { graphStore } from '../graph-store'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { Graph } from '../graph'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import * as assert from 'assert'
import { VersionStore, versionStoreFactory } from '../version-store'
import {
    Item,
    ItemList,
    itemListFactory,
    ItemListTransaction,
    ItemValue,
} from '../item-list'

describe('Minimal item list', function () {
    test('internal api, creation and retrieval by index', async () => {
        const { chunk } = chunkerFactory(512, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const store = graphStore({ chunk, linkCodec, valueCodec, blockStore })

        /**
         * Create an item list
         */
        enum KeyTypes {
            NAME = 1,
        }
        const itemList: ItemList = itemListFactory(versionStore, store)
        const tx = itemList.tx()
        await tx.start()
        await tx.push(new Map([[KeyTypes.NAME, 'item 0']]))
        await tx.push(new Map([[KeyTypes.NAME, 'item 1']]))
        await tx.push(new Map([[KeyTypes.NAME, 'item 2']]))
        const { root, index, blocks } = await tx.commit({
            comment: 'First commit',
            tags: ['v0.0.1'],
        })

        console.log('root', root.toString())

        const len = await itemList.length()
        assert.strictEqual(3, len)

        const item0 = await itemList.get(0)
        assert.strictEqual('item 0', item0.value.get(KeyTypes.NAME))

        const item1 = await itemList.get(1)
        assert.strictEqual('item 1', item1.value.get(KeyTypes.NAME))

        const item2 = await itemList.get(2)
        assert.strictEqual('item 2', item2.value.get(KeyTypes.NAME))

        assert.equal(
            'bafkreie2iaqxhv56xdtqyih57txqllfwswu7ixhgbnkmfts5weybfuodgu',
            root.toString()
        )

        /**
         * Add more items
         */
        const tx2 = itemList.tx()
        await tx2.start()
        await tx2.push(new Map([[KeyTypes.NAME, 'item 3']]))
        await tx2.push(new Map([[KeyTypes.NAME, 'item 4']]))
        await tx2.push(new Map([[KeyTypes.NAME, 'item 5']]))
        const {
            root: root2,
            index: index2,
            blocks: blocks2,
        } = await tx2.commit({
            comment: 'Second commit',
            tags: ['v0.0.2'],
        })

        console.log('root', root2.toString())

        const len2 = await itemList.length()
        assert.strictEqual(6, len2)

        const item3 = await itemList.get(3)
        assert.strictEqual('item 3', item3.value.get(KeyTypes.NAME))

        const item4 = await itemList.get(4)
        assert.strictEqual('item 4', item4.value.get(KeyTypes.NAME))

        const item5 = await itemList.get(5)
        assert.strictEqual('item 5', item5.value.get(KeyTypes.NAME))

        assert.equal(
            'bafkreie53yzg5oscjwtktgf46o33zipoyomm3ra42lclsv3eapksssflta',
            root2.toString()
        )

        /**
         * Rehydrate the item list from original version store
         */
        const itemList2: ItemList = itemListFactory(versionStore, store)

        const len3 = await itemList2.length()
        assert.strictEqual(6, len3)

        const item6 = await itemList2.get(3)
        assert.strictEqual('item 3', item6.value.get(KeyTypes.NAME))

        const item7 = await itemList2.get(4)
        assert.strictEqual('item 4', item7.value.get(KeyTypes.NAME))

        const item8 = await itemList2.get(5)
        assert.strictEqual('item 5', item8.value.get(KeyTypes.NAME))

        /**
         * Rehydrate the item list from root, fresh version store
         */

        const versionRoot = linkCodec.parseString(
            'bafkreie53yzg5oscjwtktgf46o33zipoyomm3ra42lclsv3eapksssflta'
        )
        const versionStore2: VersionStore = await versionStoreFactory({
            versionRoot,
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const itemList3: ItemList = itemListFactory(versionStore2, store)

        const len4 = await itemList3.length()
        assert.strictEqual(6, len4)

        const item9 = await itemList3.get(0)
        assert.strictEqual('item 0', item9.value.get(KeyTypes.NAME))

        const item10 = await itemList3.get(1)
        assert.strictEqual('item 1', item10.value.get(KeyTypes.NAME))

        const item11 = await itemList3.get(2)
        assert.strictEqual('item 2', item11.value.get(KeyTypes.NAME))

        const item12 = await itemList3.get(3)
        assert.strictEqual('item 3', item12.value.get(KeyTypes.NAME))

        const item13 = await itemList3.get(4)
        assert.strictEqual('item 4', item13.value.get(KeyTypes.NAME))

        const item14 = await itemList3.get(5)
        assert.strictEqual('item 5', item14.value.get(KeyTypes.NAME))

        /**
         * Navigate back in time to the first commit
         */
        versionStore.checkout(root)

        const itemList5 = itemListFactory(versionStore, store)
        const len5 = await itemList5.length()
        assert.strictEqual(3, len5)

        const item15 = await itemList5.get(0)
        assert.strictEqual('item 0', item15.value.get(KeyTypes.NAME))

        const item16 = await itemList5.get(1)
        assert.strictEqual('item 1', item16.value.get(KeyTypes.NAME))

        const item17 = await itemList5.get(2)
        assert.strictEqual('item 2', item17.value.get(KeyTypes.NAME))
    })

    test('internal api, creation and range retrieval, rangex', async () => {
        const { chunk } = chunkerFactory(512, compute_chunks)
        const linkCodec: LinkCodec = linkCodecFactory()
        const blockCodec: BlockCodec = blockCodecFactory()
        const valueCodec: ValueCodec = valueCodecFactory()
        const blockStore: BlockStore = memoryBlockStoreFactory()
        const versionStore: VersionStore = await versionStoreFactory({
            chunk,
            linkCodec,
            blockCodec,
            blockStore,
        })
        const store = graphStore({ chunk, linkCodec, valueCodec, blockStore })

        /**
         * Create an item list
         */
        enum KeyTypes {
            ID = 11,
            NAME = 33,
        }
        const itemList: ItemList = itemListFactory(versionStore, store)
        const tx = itemList.tx()
        await tx.start()
        for (let i = 0; i < 100; i++) {
            const itemValue: ItemValue = new Map<number, any>()
            itemValue.set(KeyTypes.ID, i)
            itemValue.set(KeyTypes.NAME, `item ${i}`)
            await tx.push(itemValue)
        }

        const { root, index, blocks } = await tx.commit({
            comment: 'First commit',
            tags: ['v0.0.1'],
        })

        /**
         * Item range from 25 to 75
         */
        const range: Item[] = await itemList.range(25, 50) // start index, count
        console.log(range)
        assert.strictEqual(50, range.length)
        for (let i = 0; i < range.length; i++) {
            assert.strictEqual(
                `item ${i + 25}`,
                range[i].value.get(KeyTypes.NAME)
            )
        }
    })
})
