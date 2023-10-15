import pytest
import sys



from graph import RedisNode

@pytest.fixture
def redis_node():
    # Créer un objet RedisNode pour les tests
    return RedisNode(graph_name='my_graph', md5='node123', create=True)

def test_create_redis_node(redis_node):
    assert redis_node.graph_name == 'my_graph'
    assert redis_node.md5 == 'node123'
    assert redis_node.properties == {}

def test_update_redis_node(redis_node):
    redis_node['property1'] = 'value1'
    redis_node['property2'] = 'value2'
    assert redis_node['property1'] == 'value1'
    assert redis_node['property2'] == 'value2'

def test_delete_property_redis_node(redis_node):
    del redis_node['property1']
    assert 'property1' not in redis_node

def test_str_representation_redis_node(redis_node):
    assert str(redis_node) == "Node(None) (ID: node123, Properties: {})"