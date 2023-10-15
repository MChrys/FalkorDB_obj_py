import sys



from graph import RedisNode, check_graph_exist, RedisGraph, check_node_exist, check_edge_exists,RedisEdge


if check_graph_exist('my_graph'):
    g = RedisGraph('my_graph')
    g.delete_graph()

#UPDATE @-> create Node only if id doesn't exist
#UPDATE @-> Permettre de creer tout les Node directement depuis la class RedisNode 
# : on a  une liste tocreate, ou on ajoute tout les nodes pas encore créer (self), on vérifie que le 
# Node n'existe pas avant, ensuite avec RedisNode.create_all() class methode, 
# on va iterer dans la liste to create, creer tous les noeuds et si la création se passe
#  bien on l'enlève de la liste toCreate
# il faut aussi tenter d'ajouter quand on update les _properties, car si le node exister,
# qu'il a était enlevé de tocreate , car existant deja, si on update les properties ca devient un nouveau Node to create
# au si il existe deja on peu simplement enlevé le node plutot de tocreate, et mettre le Node comme created avec le bool

#UPDATE @-> add the bool created to Edge Node and graph that is on True when created or then False
#UPDATE @-> creer le setter Demiurge_properties qui permet d'interagir avec un onbject properties et
#  manipulé self.properties comme un dict avec les proeprties du Node dans Redis 
node1 = RedisNode( 'node1','my_graph')
node2 = RedisNode( 'node2','my_graph')
node3 = RedisNode( 'node3','my_graph')
node4 = RedisNode( 'node4','my_graph')
node1.create()
node2.create()
node3.create()
node4.create()
graph = RedisGraph('my_graph')
nodes = graph.get_all_nodes()
edges = graph.get_all_edges()
edge = RedisEdge(
                  node1, 
                  node2, 
                  'edge123',
                  properties={'project':'demiurge',
                               'branch':'main'}
                               )
edge2 = RedisEdge(
                  node1, 
                  node3, 
                  'edge124',
                  properties={'project':'demiurge',
                               'branch':'main'}
                               )
edge3 = RedisEdge(
                  node4, 
                  node1, 
                  'edge125',
                  properties={'project':'demiurge',
                               'branch':'main'}
                               )

edge.create()
edges = graph.get_all_edges()
edge2.create()
edge3.create()
edges = graph.get_all_edges()

assert check_edge_exists(edge.graph_name, edge.md5)
start_md5 = edge.get_start_md5()
end_md5 = edge.get_end_md5()
assert edge.get_start_md5()== node1.md5
assert edge.get_end_md5() == node2.md5
node1_child =node1.get_node_child_edges()
assert node2 in node1.get_childs_nodes()
assert node3 in node1.get_childs_nodes()
assert node4 in node1.get_parents_nodes()

assert edge.properties['branch'] == 'main'
edge.update({'branch':'test'})
assert edge.properties['branch'] == 'test'
edge.delete()
assert check_edge_exists(edge.graph_name, edge.md5) == False
assert len(graph.get_all_edges()) == 2
edges = graph.get_all_edges()
print(edges[0])
print('ok')