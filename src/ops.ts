enum Operator {
    INCL,
    IT,
    EQ,
    GT,
    GTE,
    LT,
    LTE
}

interface PreProcessor {
    (ctx: any): any
}

interface Predicate {
    (ctx: any): boolean
}

interface PredicateFactory {
    (value: any): Predicate
}

interface Operation {
    operator: Operator
    operand: any
    predicate: Predicate
}

function eq(value: any, pre?: PreProcessor): Operation {
    const operator = Operator.EQ
    const operand = value
    const predicate = (ctx: any) => pre === undefined ? ctx === value : pre(ctx) === value
    return { operator, operand, predicate }
}

function gt(value: any, pre?: PreProcessor): Operation {
    const operator = Operator.GT
    const operand = value
    const predicate = (ctx: any) => pre === undefined ? ctx > value : pre(ctx) > value
    return { operator, operand, predicate }
}

function gte(value: any, pre?: PreProcessor): Operation {
    const operator = Operator.GTE
    const operand = value
    const predicate = (ctx: any) => pre === undefined ? ctx >= value : pre(ctx) >= value
    return { operator, operand, predicate }
}

function lt(value: any, pre?: PreProcessor): Operation {
    const operator = Operator.LT
    const operand = value
    const predicate = (ctx: any) => pre === undefined ? ctx < value : pre(ctx) < value
    return { operator, operand, predicate }
}

function lte(value: any, pre?: PreProcessor): Operation {
    const operator = Operator.LTE
    const operand = value
    const predicate = (ctx: any) => pre === undefined ? ctx <= value : pre(ctx) <= value
    return { operator, operand, predicate }
}

//(value: any) => [55, 33].includes(value)

function incl(values: any[], pre?: PreProcessor): Operation {
    const operator = Operator.INCL
    const operand = values
    const predicate = (ctx: any) => pre === undefined ? values.includes(ctx) : values.includes(pre(ctx))
    return { operator, operand, predicate }
}

function isTrue(): Operation {
    const operator = Operator.IT
    const operand = undefined
    const predicate = (ctx: any) => true
    return { operator, operand, predicate }
}

export { Predicate, Operation, Operator, PredicateFactory, eq, gt, gte, lt, lte, isTrue, incl }