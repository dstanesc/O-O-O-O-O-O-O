import { Edge, Index, Prop, VersionDetails, Vertex } from './types'

const fastCloneVertex = (vertex: Vertex): Vertex => {
    const clone: Vertex = { offset: vertex.offset, status: vertex.status }
    if (vertex.type !== undefined) clone.type = vertex.type
    if (vertex.nextEdge !== undefined) clone.nextEdge = vertex.nextEdge
    if (vertex.nextProp !== undefined) clone.nextProp = vertex.nextProp
    if (vertex.nextIndex !== undefined) clone.nextIndex = vertex.nextIndex
    return clone
}

const fastCloneEdge = (edge: Edge): Edge => {
    const clone: Edge = {
        offset: edge.offset,
        status: edge.status,
        source: edge.source,
        target: edge.target,
    }
    if (edge.type !== undefined) clone.type = edge.type
    if (edge.sourcePrev !== undefined) clone.sourcePrev = edge.sourcePrev
    if (edge.sourceNext !== undefined) clone.sourceNext = edge.sourceNext
    if (edge.targetPrev !== undefined) clone.targetPrev = edge.targetPrev
    if (edge.targetNext !== undefined) clone.targetNext = edge.targetNext
    if (edge.nextProp !== undefined) clone.nextProp = edge.nextProp
    return clone
}

const fastCloneProp = (prop: Prop): Prop => {
    const clone: Prop = {
        offset: prop.offset,
        status: prop.status,
        key: prop.key,
        value: prop.value,
    }
    if (prop.type !== undefined) clone.type = prop.type
    if (prop.nextProp !== undefined) clone.nextProp = prop.nextProp
    return clone
}

const fastCloneIndex = (index: Index): Index => {
    const clone: Index = {
        offset: index.offset,
        status: index.status,
        key: index.key,
        value: index.value,
    }
    if (index.type !== undefined) clone.type = index.type
    if (index.nextIndex !== undefined) clone.nextIndex = index.nextIndex
    return clone
}

const fastCloneVersionDetails = (
    versionDetails: VersionDetails
): VersionDetails => {
    const clone: VersionDetails = {
        timestamp: versionDetails.timestamp,
    }
    if (versionDetails.author !== undefined)
        clone.author = versionDetails.author
    if (versionDetails.comment !== undefined)
        clone.comment = versionDetails.comment
    if (versionDetails.signature !== undefined)
        clone.signature = versionDetails.signature
    if (versionDetails.tags !== undefined) clone.tags = versionDetails.tags
    return clone
}

export { fastCloneVertex, fastCloneEdge, fastCloneProp, fastCloneIndex, fastCloneVersionDetails }
