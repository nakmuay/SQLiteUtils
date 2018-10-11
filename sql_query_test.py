from sql_query import *


def main():
    int1 = SqlInteger(1) 
    int2 = SqlInteger(2)
    col1 = SqlColumnReference('x')
    col2 = SqlColumnReference('x', alias="apa")

    expr1 = SqlBinaryOperator('<', int1, int2)
    expr2 = SqlBinaryOperator('>', int1, expr1)
    expr3 = SqlBinaryOperator('<', expr1, expr2)
    expr4 = SqlBinaryOperator('+', col1, col2)
    expr5 = SqlBinaryOperator('-', expr3, expr4)

    v = SqlQueryFormatterVisitor()
    expr5.accept(v)

    columns = []
    columns.append(SqlColumnReference('x'))
    columns.append(SqlColumnReference('x1', alias="apa1"))
    columns.append(SqlColumnReference('x2', alias="apa2"))
    columns.append(SqlColumnReference('x3', alias="apa3"))

    v = SqlQueryFormatterVisitor()
    s = SqlSelectClause(columns)
    s.accept(v)

    f = SqlFromClause(SqlTableReference("apa_table", alias="table_alias"))
    f.accept(v)

    print(v)


if __name__ == "__main__":
    main()
