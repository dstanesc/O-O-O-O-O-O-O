import { Link, RootIndex } from './types'

interface RootStore {
    rootSet: ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }) => Promise<void>
    rootGet: () => Promise<{ root: Link; index: RootIndex }>
    storeGet: () => { root: any; index: any }
}

const emptyRootStore = (): RootStore => {
    let store = { root: undefined, index: undefined }
    const rootSet = async ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }): Promise<void> => {
        store = { root, index }
    }
    const rootGet = async (): Promise<{ root: Link; index: RootIndex }> => {
        return store
    }
    const storeGet = (): { root: any; index: any } => store
    return { rootSet, rootGet, storeGet }
}

const initRootStore = ({
    root,
    index,
}: {
    root: Link
    index: RootIndex
}): RootStore => {
    let store = { root: root, index: index }
    const rootSet = async ({
        root,
        index,
    }: {
        root: Link
        index: RootIndex
    }): Promise<void> => {
        store = { root, index }
    }
    const rootGet = async (): Promise<{ root: Link; index: RootIndex }> => {
        return store
    }
    const storeGet = (): { root: any; index: any } => store
    return { rootSet, rootGet, storeGet }
}

export { RootStore, emptyRootStore, initRootStore }
