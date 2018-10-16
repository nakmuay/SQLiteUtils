from sql_query import *


def main():
    int1 = SqlInteger(1) 
    int2 = SqlInteger(2)
    col1 = SqlColumnName('x')
    col2 = SqlColumnReference(col1, alias="apa")

    expr1 = SqlBinaryOperator('<', int1, int2)
    expr2 = SqlBinaryOperator('>', int1, expr1)
    expr3 = SqlBinaryOperator('<', expr1, expr2)
    expr4 = SqlBinaryOperator('+', col1, col2)
    expr5 = SqlBinaryOperator('-', expr3, expr4)
    expr6 = SqlBinaryOperator('<', col1, int2)

    v = SqlQueryFormatterVisitor()
    v.format(expr5)
    print(v)
    return

    columns = []
    columns.append(SqlColumnReference('x'))
    columns.append(SqlColumnReference('x1', alias="apa1"))
    columns.append(SqlColumnReference('x2', alias="apa2"))
    columns.append(SqlColumnReference('x3', alias="apa3"))

    v = SqlQueryFormatterVisitor()
    s = SqlSelectClause(columns)
    v.format(s)

    f = SqlFromClause(SqlTableReference("apa_table", alias="table_alias"))
    v.format(f)

    w = SqlWhereClause(expr6)
    v.format(w)
    print(v)


if __name__ == "__main__":
    main()
