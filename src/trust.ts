import { RootDecoder } from './serde'
import { Link } from './types'

interface Signer {
    sign: (root: Link) => Promise<Uint8Array>
}

const signerFactory = ({
    subtle,
    privateKey,
}: {
    subtle: SubtleCrypto
    privateKey: CryptoKey
}): Signer => {
    const sign = async (root: Link): Promise<Uint8Array> => {
        const buffer: ArrayBuffer = await subtle.sign(
            {
                name: 'RSA-PSS',
                saltLength: 32,
            },
            privateKey,
            root.bytes
        )
        return new Uint8Array(buffer)
    }

    return { sign }
}

const verify = async ({
    subtle,
    publicKey,
    root,
    signature,
}: {
    subtle: SubtleCrypto
    publicKey: CryptoKey
    root: Link
    signature: Uint8Array
}): Promise<boolean> => {
    return await subtle.verify(
        {
            name: 'RSA-PSS',
            saltLength: 32,
        },
        publicKey,
        signature,
        root.bytes
    )
}

export { signerFactory, Signer, verify }
