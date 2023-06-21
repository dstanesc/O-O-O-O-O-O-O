import {
    linkCodecFactory,
    LinkCodec,
    ValueCodec,
    valueCodecFactory,
} from '../codecs'
import { graphStoreFactory } from '../graph-store'
import { compute_chunks } from '@dstanesc/wasm-chunking-fastcdc-node'
import { chunkerFactory } from '../chunking'
import { Graph } from '../graph'
import { BlockStore, memoryBlockStoreFactory } from '../block-store'
import { VersionStore, versionStoreFactory } from '../version-store'
import { Signer, signerFactory, verify } from '../trust'
import * as assert from 'assert'
import crypto from 'crypto'
import base64 from 'base64-js'

const { subtle } = crypto.webcrypto

describe('Trust management', function () {
    test('Verify signed commits', async () => {
        const { chunk } = chunkerFactory(1024, compute_chunks)
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
        const graph = new Graph(versionStore, graphStore)

        const tx = graph.tx()

        await tx.start()

        const v1 = tx.addVertex()
        const v2 = tx.addVertex()
        const v3 = tx.addVertex()

        await tx.addEdge(v1, v2)
        await tx.addEdge(v1, v3)

        await tx.addVertexProp(v2, 1, { hello: 'v2' })
        await tx.addVertexProp(v2, 1, { hello: 'v3' })

        /**
         * Generate a key pair, in practice this would be done once and persisted
         */
        const { publicKey, privateKey } = await subtle.generateKey(
            {
                name: 'RSA-PSS',
                modulusLength: 2048,
                publicExponent: new Uint8Array([1, 0, 1]),
                hash: 'SHA-256',
            },
            true,
            ['sign', 'verify']
        )

        /**
         * Sign the root while committing
         */

        const signer: Signer = signerFactory({ subtle, privateKey })

        const { root } = await tx.commit({
            comment: 'First draft',
            tags: ['v0.0.1'],
            signer,
        })

        const versionStoreRoot = versionStore.versionStoreRoot()

        /**
         * Simulate a remote read based on the version store root
         */
        const otherVersionStore: VersionStore = await versionStoreFactory({
            storeRoot: versionStoreRoot,
            chunk,
            linkCodec,
            valueCodec,
            blockStore,
        })

        const { version } = await otherVersionStore.versionGet()

        console.log(
            'signature',
            base64.fromByteArray(version.details.signature)
        )

        const trusted = await verify({
            subtle,
            publicKey,
            root: version.root,
            signature: version.details.signature,
        })

        assert.strictEqual(trusted, true)

        /**
         * Verify that a random root is not trusted
         */
        const untrusted = await verify({
            subtle,
            publicKey,
            root: linkCodec.parseString(
                'bafkreigy7o4ouzr2dgv3nzub5omjrsie3liam2mpojkaw7vazwoas3qbz4'
            ),
            signature: version.details.signature,
        })

        assert.strictEqual(untrusted, false)
    })
})
