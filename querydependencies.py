import json
from pprint import pprint
import pydot
import os
from PIL import Image
import sqlparse
import attr

os.environ["PATH"] += os.pathsep + 'C:/Program Files (x86)/Graphviz2.38/bin'

def normalizeTableName (t, database=''):
    parts = t.split('.')
    assert (len(parts) <= 3)
    if len(parts)==3:
        db, schema, table = parts
    elif len(parts)==2:
        db = database
        schema, table = parts
    else:
        db = database
        schema = 'dbo'
        table = parts[0]
    db = db.lower().replace('[', '').replace(']', '')
    schema = schema.lower().replace('[', '').replace(']', '')
    table = table.lower().replace('[', '').replace(']', '')
    if database:
        return '%s.%s.%s' % (database, schema, table)
    else:
        return '%s.%s' % (schema, table)


def graphQueryDependencies (configFile):
    config = json.load(open(configFile))
    pprint (config)
    graph = pydot.Dot(graph_type='digraph')
    tableNodes = dict()

    defaultdb = config['database']

    for fileBlock in config['files']:
        curr_fname = fileBlock['filename']
        cluster = pydot.Cluster(curr_fname, label=curr_fname)
        for queryBlock in fileBlock['queries']:
            queryLabel  = '%s - %d' % (curr_fname, queryBlock['id'])
            qn = pydot.Node(queryLabel, shape='plaintext')
            cluster.add_node(qn)

            for table in queryBlock.get('reads', []):
                table = normalizeTableName(table, defaultdb)
                tableNodes.setdefault(table, pydot.Node(table))
                graph.add_edge(pydot.Edge(tableNodes[table], qn))

            for table in queryBlock.get('updates', []):
                table = normalizeTableName(table, defaultdb)
                tableNodes.setdefault(table, pydot.Node(table))
                cluster.add_edge(pydot.Edge(qn, tableNodes[table]))

            for table in queryBlock.get('inserts', []):
                table = normalizeTableName(table, defaultdb)
                tableNodes.setdefault(table, pydot.Node(table))
                cluster.add_edge(pydot.Edge(qn, tableNodes[table]))

            for table in queryBlock.get('deletes', []):
                table = normalizeTableName(table, defaultdb)
                tableNodes.setdefault(table, pydot.Node(table))
                cluster.add_edge(pydot.Edge(qn, tableNodes[table], label='deletes'))

        graph.add_subgraph(cluster)

    graph.write_png ('output.png')

    im = Image.open('output.png')
    im.show()


def getIdentifier (tok):
    schema = tok.get_parent_name()
    table = tok.get_real_name()
    if schema:
        fullname = '%s.%s' % (schema, table)
    else:
        fullname = '%s' % (table)

    if fullname.lower() != 'top':
        return fullname

@attr.s
class Statement (object):
    sql = attr.ib()
    reads = attr.ib()
    updates = attr.ib()
    inserts = attr.ib()
    deletes = attr.ib()


def parseSQLStatement (text):
    subqueries = []
    st = Statement(sql='', reads=set(), updates=set(), inserts=set(), deletes=set())
    for block in sqlparse.parse(text):
        stype = block.get_type()
        stmt_into = False
        in_dml = False
        in_cte = False
        in_from = False
        cte_names = set()
        for tok in block.tokens:
            if tok.value.upper() == 'FROM':
                in_from = True

            if tok.ttype is sqlparse.tokens.CTE:
                in_cte = True

            if in_cte:
                if tok.is_group:
                    cte_names.add(tok.normalized.upper())
                    for subtok in tok.tokens:
                        if type(subtok) is sqlparse.sql.Parenthesis:
                            subqueries.append(str(subtok.normalized))
                            print (subtok.normalized)


            if tok.ttype is sqlparse.tokens.DML:
                in_dml = True
                in_cte = False
                stmt_into = False

            if not in_dml:
                continue

            if tok.value == 'into':
                stmt_into = True

            if stmt_into:
                if tok.is_group:
                    for subtok in tok.tokens:
                        if type(subtok) is sqlparse.sql.Identifier:
                            st.inserts.add(getIdentifier(subtok))
                            continue

            if type(tok) is sqlparse.sql.Parenthesis:
                subqueries.append(str(tok.normalized))

            if tok.is_group:
                for subtok in tok.tokens:
                    if type(subtok) is sqlparse.sql.Parenthesis:
                        subqueries.append(str(subtok.normalized))
                        continue

            if type(tok) is sqlparse.sql.Identifier:
                ids = [tok,]
            elif type(tok) is sqlparse.sql.IdentifierList:
                ids = [i for i in tok.tokens if type(i) is sqlparse.sql.Identifier]
            else:
                ids = []

            for tok in ids:
                fullname = getIdentifier (tok)
                if fullname:
                    if stmt_into:
                        st.inserts.add(fullname)
                        st.ddl.add(fullname)
                        stmt_into = False
                    elif stype == 'DELETE':
                        st.deletes.add(fullname)
                    elif stype == 'UPDATE':
                        st.updates.add(fullname)
                    elif fullname.upper() not in cte_names and in_from:
                        st.reads.add(fullname)

    for sq in subqueries:
        sq = sq.strip()
        if sq.startswith('('):
            sq = sq[1:]
        if sq.endswith(')'):
            sq = sq[:-1]

        parsed = parseSQLStatement(sq.strip())
        st.reads.update(parsed.reads)
    return st

def test ():
    #parsed = parseSQLStatement(open('./test/query1.sql').read())
    #assert (parsed.reads == {'dw.Months', 'fact.sales'})

    #parsed = parseSQLStatement(open('./test/query2.sql').read())
    #assert (parsed.reads=={'dw.Customers'} and parsed.ddl=={'#temp'} and parsed.reads=={'dw.Customers'})

    #parsed = parseSQLStatement(open('./test/query3.sql').read())
    #assert (parsed.reads=={'dbo.customers', 'dw.months'})

    #parsed = parseSQLStatement(open('./test/query4.sql').read())
    #assert (parsed.reads=={'Months', 'dw.customers'})

    #parsed = parseSQLStatement(open('./test/insert.sql').read())
    #assert (parsed.reads=={'source9', 'other'} and parsed.inserts=={'dest'})

    #parsed = parseSQLStatement(open('./test/delete.sql').read())
    #assert (parsed.reads=={'oldcustomers'} and parsed.deletes=={'customers'})

    #parsed = parseSQLStatement(open('./test/simple.sql').read())
    #assert (parsed.reads=={'customer'})

    #parsed = parseSQLStatement(open('./test/update.sql').read())
    #assert (parsed.updates=={'customers'})

    parsed = parseSQLStatement(open('./test/update2.sql').read())
    pprint (parsed)
    assert (parsed.updates=={'customers'} and parsed.reads=={'customers', 'months'})
#graphQueryDependencies('./test1.json')
test()