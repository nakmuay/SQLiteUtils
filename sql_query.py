from abc import ABC, abstractmethod


class SqlNode():

    def accept(self, visitor):
        visitor.visit(self)


class SqlSelectClause(SqlNode):

    def __init__(self, column_references):
        self._columns = column_references

    @property
    def columns(self):
        return self._columns


class SqlFromClause(SqlNode):

    def __init__(self, table_reference):
        self._table = table_reference

    @property
    def table(self):
        return self._table


class SqlWhereClause(SqlNode):

    def __init__(self, expr):
        self._expression = expr

    @property
    def expression(self):
        return self._expression


class SqlTableReference(SqlNode):

    def __init__(self, table_name, alias=None):
        self._name = table_name
        self._alias = alias

    @property
    def name(self):
        return self._name

    @property
    def alias(self):
        return self._alias


class SqlColumnName(SqlNode):

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name


class SqlColumnReference(SqlNode):

    def __init__(self, expr, alias=None):
        self._expression = expr
        self._alias = alias

    @property
    def expression(self):
        return self._expression

    @property 
    def alias(self):
        return self._alias


class SqlInteger(SqlNode):

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class SqlBinaryOperator(SqlNode):

    def __init__(self, operator, left, right):
        self._operator = operator
        self._left = left
        self._right = right

    @property
    def operator(self):
        return self._operator

    @property 
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right


class SqlQueryWalkerVisitor(ABC):

    def visit(self, node):
        if isinstance(node, SqlSelectClause):
            self._visit_sqlselectclause(node)
            return

        if isinstance(node, SqlFromClause):
            self._visit_sqlfromclause(node)
            return

        if isinstance(node, SqlWhereClause):
            self._visit_sqlwhereclause(node)
            return

        if isinstance(node, SqlTableReference):
            self._visit_sqltablereference(node)
            return

        if isinstance(node, SqlColumnName):
            self._visit_sqlcolumnname(node)
            return

        if isinstance(node, SqlColumnReference):
            self._visit_sqlcolumnreference(node)
            return

        if isinstance(node, SqlInteger):
            self._visit_sqlinteger(node)
            return

        if isinstance(node, SqlBinaryOperator):
            self._visit_sqlbinaryoperator(node)
            return

        msg = "No visit method is implemented for type '{0}'".format(type(node).__name__)
        raise NotImplementedError(msg)
        
    @abstractmethod
    def _visit_sqlselectclause(self, node):
        pass

    @abstractmethod
    def _visit_sqlfromclause(self, node):
        pass

    @abstractmethod
    def _visit_sqlwhereclause(self, node):
        pass

    @abstractmethod
    def _visit_sqltablereference(self, node):
        pass

    @abstractmethod
    def _visit_sqlcolumnname(self, node):
        pass

    @abstractmethod
    def _visit_sqlcolumnreference(self, node):
        pass

    @abstractmethod
    def _visit_sqlinteger(self, node):
        pass

    @abstractmethod
    def _visit_sqlbinaryoperator(self, op_node):
        pass


class SqlQueryFormatterVisitor(SqlQueryWalkerVisitor):

    def __init__(self):
        super().__init__()
        self._builder = []

    def format(self, query):
        query.accept(self)

    def _visit_sqlselectclause(self, select_clause):
        self._builder.append("SELECT ")

        num_cols = len(select_clause.columns) 
        for i, c in enumerate(select_clause.columns):
            c.accept(self)

            if i < num_cols-1: 
                self._builder.append(", ")

    def _visit_sqlfromclause(self, from_clause):
        self._builder.append("\nFROM ")
        from_clause.table.accept(self)

    def _visit_sqlwhereclause(self, where_clause):
        self._builder.append("\nWHERE ")
        where_clause.expression.accept(self)

    def _visit_sqltablereference(self, table_node):
        self._builder.append("{0}".format(table_node.name))
        if table_node.alias is not None:
            self._builder.append(" AS {0}".format(table_node.alias))

    def _visit_sqlcolumnname(self, col):
        self._builder.append("{0}".format(col.name))

    def _visit_sqlcolumnreference(self, col_node):
        col_node.expression.accept(self)
        if col_node.alias is not None:
            self._builder.append(" AS {0}".format(col_node.alias))

    def _visit_sqlinteger(self, int_node):
        self._builder.append("{0}".format(int_node.value))

    def _visit_sqlbinaryoperator(self, op_node):
        self._builder.append("(")
        op_node.left.accept(self)
        self._builder.append(" {0} ".format(op_node.operator))
        op_node.right.accept(self)
        self._builder.append(")")

    def __str__(self):
        return "".join([s for s in self._builder])
