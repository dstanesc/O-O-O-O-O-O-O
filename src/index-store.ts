import { IndexedValue, Link } from "./types";

interface IndexStore {
    indexCreate: (values: IndexedValue[]) => Promise<Link>
    indexSearch: (link: Link, value: any) => Promise<IndexedValue>
}

export { IndexStore }