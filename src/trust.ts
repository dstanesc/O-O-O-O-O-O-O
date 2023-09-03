import { Link } from './types'

interface Signer {
    sign: (root: Link) => Promise<Uint8Array>
    exportPublicKey: () => Promise<string>
}

const signerFactory = ({
    subtle,
    privateKey,
    publicKey,
}: {
    subtle: SubtleCrypto
    privateKey: CryptoKey
    publicKey: CryptoKey
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

    const exportPublicKey = async (): Promise<string> => {
        const exported = await subtle.exportKey('jwk', publicKey)
        return JSON.stringify(exported)
    }
    return { sign, exportPublicKey }
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

const importPublicKey = async ({
    subtle,
    key: keyString,
}: {
    subtle: SubtleCrypto
    key: string
}): Promise<CryptoKey> => {
    const key = JSON.parse(keyString)
    return await subtle.importKey(
        'jwk',
        key,
        { name: 'RSA-PSS', hash: 'SHA-256' },
        true,
        ['verify']
    )
}

export { signerFactory, Signer, verify, importPublicKey }
