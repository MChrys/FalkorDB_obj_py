import sys


from graph import RedisNode, check_graph_exist, RedisGraph, check_node_exist

if check_graph_exist('my_graph'):
    g = RedisGraph('my_graph')
    g.delete_graph()
node_props = {
    'branch':'main',
    'project': 'demiurge'
}
redis_node =  RedisNode(graph_name='my_graph', md5='node123', properties=node_props)
assert redis_node.create()
assert redis_node.graph_name == 'my_graph'
assert redis_node.md5 == 'node123'
assert redis_node.properties['branch'] == 'main'

redis_node.update({'branch':'test'})
print(redis_node.properties)
assert redis_node.properties['branch'] == 'test'
assert redis_node.get_label()== 'Node'
assert check_node_exist(redis_node.graph_name,redis_node.md5)
redis_node.delete()
assert check_node_exist(redis_node.graph_name,redis_node.md5) == False

print('ok')