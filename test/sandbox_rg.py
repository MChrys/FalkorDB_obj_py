import sys



from graph import RedisNode, check_graph_exist, RedisGraph, check_node_exist, check_edge_exists,RedisEdge


if check_graph_exist('my_graph'):
    g = RedisGraph('my_graph')
    g.delete_graph()


node1 = RedisNode(
    'node1',
    properties={
        'branch':'main'
        })
node2 = RedisNode('node2')
node3 = RedisNode('node3')
node4 = RedisNode('node4','my_graph')

graph = RedisGraph('my_graph')



#Create Node
graph.create_node(node1, node2, node3)

#Create Edge
edge1 = graph.create_edge(
    node1, 
    node2,
    properties={
        'branch':'main'
    }
    )
edge2 = graph.create_edge(
    node1, 
    node3,
    properties={
        'branch':'main'
    }
    )
edge_md5 = edge1.md5
#get all edges
edges = graph.get_all_edges()
assert edge1 in edges
assert edge2 in edges

#get all nodes
nodes = graph.get_all_nodes()
assert node1 in nodes
assert node2 in nodes 
assert node3 in nodes
assert node4 not in nodes
node4.create()
assert node4 in graph.get_all_nodes()


# get node child edges
edges = graph.get_node_child_edges(node1)
assert edge1 in edges
assert edge2 in edges

# get node parent edges
edge3 = graph.create_edge(
    node4, 
    node1,
    properties={
        'branch':'main'
    }
    )
edges_p =graph.get_node_parent_edges(node1)
assert edge3 in edges_p

# get parents nodes
p_nodes = graph.get_parents_nodes(node1)
assert node4 in p_nodes

# get children nodes 
c_nodes = graph.get_childs_nodes(node1)
assert node2 in c_nodes
assert node3 in c_nodes


#get_node
node2_2 = graph.get_node(node2.md5)
assert node2 == node2_2

# get edge
edge1_1 = graph.get_edge(edge1.md5)
assert edge1_1 == edge1

# delete_edge
graph.delete_edge(node1,node2) # supprimer le edge1 à partir des nodes
edges = graph.get_all_edges()
assert edge1 not in edges
graph.delete_edge(edge2) # supprimer un edge directement à partir d'un edge
edges = graph.get_all_edges()
assert edge2 not in edges
# delete_node
graph.delete_node(node4)
assert node4 not in graph.get_all_nodes()

#delete
graph.delete_graph()
assert not check_graph_exist('my_graph')