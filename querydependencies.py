import json
from pprint import pprint
import pydot
import os
from PIL import Image

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


graphQueryDependencies('./test1.json')