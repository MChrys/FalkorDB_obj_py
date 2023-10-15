from typing import Any


import redis
from redisgraph.query_result import QueryResult
from redisgraph.exceptions import VersionMismatchException
from redisgraph.execution_plan import ExecutionPlan
from functions import extract_number, extract_string_between_brackets
def get_connect()->redis.Redis:

    return redis.Redis()
def quote_string(v):
    """
    RedisGraph strings must be quoted,
    quote_string wraps given v with quotes incase
    v is a string.
    """

    if isinstance(v, bytes):
        v = v.decode()
    elif not isinstance(v, str):
        return v
    if len(v) == 0:
        return '""'

    v = v.replace('\\', '\\\\')
    v = v.replace('"', '\\"')

    return '"{}"'.format(v)

def stringify_param_value(value):
    """
    Turn a parameter value into a string suitable for the params header of
    a Cypher command.

    You may pass any value that would be accepted by `json.dumps()`.

    Ways in which output differs from that of `str()`:
        * Strings are quoted.
        * None --> "null".
        * In dictionaries, keys are _not_ quoted.

    :param value: The parameter value to be turned into a string.
    :return: string
    """
    if isinstance(value, str):
        return quote_string(value)
    elif value is None:
        return "null"
    elif isinstance(value, (list, tuple)):
        return f'[{",".join(map(stringify_param_value, value))}]'
    elif isinstance(value, dict):
        return f'{{{",".join(f"{k}:{stringify_param_value(v)}" for k, v in value.items())}}}'
    else:
        return str(value)



def check_graph_exist(graph_name):
    
    conn = get_connect()
        
    query =  "MATCH (n) RETURN n"
    try:
        result = conn.execute_command("GRAPH.QUERY",graph_name, query)
        return len(result[1])>0
    except redis.exceptions.ResponseError as e:
        if "Graph does not exist" in str(e):
            return False
        raise e
    finally:
        conn.close()

def check_edge_exists(graph_name, edge_id):
    conn = get_connect()
    query = f"MATCH ()-[e:Edge {{id: '{edge_id}'}}]-() RETURN e LIMIT 1"
    #g = RedisGraph(graph_name=graph_name)
    #result = g.query(q)
    result = conn.execute_command("GRAPH.QUERY", graph_name, query)
    conn.close()
    return len(result[1]) > 0

def check_node_exist(graph_name, node_id, label :str = None):
    conn = get_connect()
    #query = f"MATCH (n) WHERE n.id = '{node_id}' RETURN n LIMIT 1"

    if label:
        query = f"MATCH (n:{label}) WHERE n.id = '{node_id}' RETURN n "
    else :
        query = f"MATCH (n) WHERE n.id = '{node_id}' RETURN n "
    #g = RedisGraph(graph_name=graph_name)
    #result = g.query(q)
    result = conn.execute_command("GRAPH.QUERY", graph_name, query)
    #conn.close()
    result =  len(result[1]) > 0
    return result

class RedisGraph:
    def __init__(self, graph_name):
        self.graph_name = graph_name
        #self._refresh_labels()
        

    def delete_graph(self):
        if not self.graph_name:
            raise ValueError("you can't delete if graph_name is None")
        conn = get_connect()
        result = conn.execute_command("GRAPH.DELETE", self.graph_name).decode()

        conn.close()
        if 'Graph removed' in result:
            return True
        else :
            return False
    
    def create_node(
            self, 
            *nodes:'RedisNode' 
            ):
        results = []
        for node in nodes:
            if not isinstance(node, RedisNode):
                raise ValueError("Invalid node object")
            
            #si on a pas initialisé le nom du graph on récupère celui de graph 
            if not node.graph_name:
                node(graph_name=self.graph_name)
            if node.graph_name != self.graph_name:
                raise ValueError('this Node is not set for this graph')
            result = node.create()
            results.append(result)
        if len(results) == 1:
            results = results[0]
        return results

    def create_edge(
                    self,   
                    start_node :'RedisNode' or str , 
                    end_node:'RedisNode' or str , 
                    edge:  str = None, 
                    properties: dict=None,
                    label :str =None
                    )-> 'RedisEdge': 
        
        if start_node and isinstance(start_node, str):
                start_node =  self.get_node(start_node)
        if end_node and isinstance(end_node, str):
                end_node = self.get_node(end_node)

        if isinstance(edge, str):
            edge = self.get_edge(str)
            #start_node = edge.get_start_md5()
            #end_node = edge.get_start_md5()
        elif not isinstance(edge, RedisEdge) :
            if  start_node is None or  end_node is None:
                raise ValueError("start Node or end Node is not setted")
            edge = RedisEdge(
                start_node_md5=start_node,
                end_node_md5=end_node,
                properties=properties,
                create = True,
                label=label
            )

            
        # properties_str = self._format_properties(str(edge.properties))
        # query = f"MATCH (start:Node {{id: '{edge.get_start_md5()}'}}), (end:Node {{id: '{edge.get_end_md5()}'}}) CREATE (start)-[:Edge {{id: '{edge.md5}', properties: {properties_str}}}]->(end)"
        # result  = self._execute_query(query)
        return edge

    def delete_node(self, 
                    node: 'RedisNode' or str,
                    properties= None):
        # UPDATE X-> ajouter properties pour supprimer seulement les noeud avec ces properties
        if isinstance(node, RedisNode):
            md5 = node.md5
        else :
            md5 = node
        query = f"MATCH (n {{id: '{md5}'}}) DELETE n"
        result = self._execute_query(query)
        return result

    def delete_edge(self,
                     edge: 'RedisEdge' or str or 'RedisNode', 
                     endnode : 'RedisNode'= None,
                     properties = None
                     ):
        # UDPATE X-> if node Edge put start node and end node that delete edge between them
        # UPDATE X-> ajouter properties pour supprimer seulement les noeud avec ces properties
        if isinstance(edge , RedisEdge) :
            if check_edge_exists(graph_name = self.graph_name,edge_id =edge.md5):
                md5 = edge.md5
                query = f"MATCH ()-[e {{id: '{md5}'}}]-() DELETE e"
                result = self._execute_query(query)
            else :  
                raise ValueError(f" edge {edge.md5} is not created ")
        elif isinstance(edge, str) : # si c'est le md5 du edge
            if check_edge_exists(edge):
                md5 = edge
            else :
                raise ValueError(f" edge {edge} is not created ")
            query = f"MATCH ()-[e {{id: '{md5}'}}]-() DELETE e"
            result = self._execute_query(query)
        elif isinstance(edge, RedisNode) :
            if not endnode or not isinstance(endnode, RedisNode):
                raise ValueError(f'endnode  {endnode} should be a RedisNode, set endnode')
            query = f"MATCH (start)-[e]->(end) " \
                    f"WHERE start.id = '{edge.md5}' AND end.id = '{endnode.md5}' " \
                    f"DELETE e"
            result = self._execute_query(query)
        return extract_number(result[0][0].decode()) == 1

    def get_node(self, md5, label=None)-> 'RedisNode' or None:
        if label:
            query = f"MATCH (n:{label} {{id: '{md5}'}}) RETURN n"        
        else :
            query = f"MATCH (n {{id: '{md5}'}}) RETURN n"

        result = self._execute_query(query)
        if result and len(result[1]) > 0:
            #result = result[0]['n']['properties']
            if label:
                return RedisNode(graph_name=self.graph_name, md5=md5, load=True, typex=label)
            else :
                return RedisNode(graph_name=self.graph_name, md5=md5, load=True)
        return None

    def get_node_child_edges(
            self , 
            node : 'RedisNode' or str, 
            typex :str = None # le type d'edge
            )-> list['RedisEdge']:
        """
        récupérer les edges d'un noeud
        """
        if isinstance(node , RedisNode):
            md5 = node.md5
        else :
            md5 = md5
        if typex :
            typex = f":{typex}"
        else :
            typex = ""
        #UPDATE @-> DONE retourn une liste de RedisEdges et donc recup l'id des edges  et les load succintement  
        #query = f"MATCH (start)-[e]->() WHERE start.id = '{md5}' RETURN e"
        query = f"MATCH (start)-[e{typex}]->() WHERE start.id = '{md5}' RETURN e.id"
        result = self._execute_query(query)
        edges = []
        for record in result[1]:
            edges.append(RedisEdge(md5= record[0].decode(), graph_name=self.graph_name))
        return edges
    
    def get_node_parent_edges(
            self , 
            node : 'RedisNode' or str, 
            typex :str = None # le type d'edge
            )-> list['RedisEdge']:
        """
        récupérer les edges d'un noeud
        """
        if isinstance(node , RedisNode):
            md5 = node.md5
        else :
            md5 = md5
        if typex :
            typex = f":{typex}"
        else :
            typex = ""
        #UPDATE @-> DONE retourn une liste de RedisEdges et donc recup l'id des edges  et les load succintement  
        query = f"MATCH ()-[e{typex}]->(end) WHERE end.id = '{md5}' RETURN e.id"
        result = self._execute_query(query)
        edges = []
        for record in result[1]:
            edges.append(RedisEdge(md5= record[0].decode(), graph_name=self.graph_name, load=True))
        if len(edges) == 1:
            edges = edges[0]
        return edges
    
    
    def get_parents_nodes(
            self , 
            node : 'RedisNode' or str, 
            typex :str = None # le type d'edge
            )-> list['RedisNode']:
        """
        get all parents node from a child node
        """
        if isinstance(node , RedisNode):
            md5 = node.md5
        else :
            md5 = md5

        if typex :
            typex = f":{typex}"
        else :
            typex = ""

        query = f"MATCH (start)-[{typex}]->(end) WHERE end.id = '{md5}' RETURN start.id"
        result = self._execute_query(query)
        edges = []
        for record in result[1]:
            edges.append(RedisNode(md5= record[0].decode(), graph_name=self.graph_name))
        return edges

    def get_childs_nodes(
            self , 
            node : 'RedisNode' or str, 
            typex :str = None # le type d'edge
            )-> list['RedisNode']:
        """
        get all childs nodes from a parent nodes
        """

        if isinstance(node , RedisNode):
            md5 = node.md5
        else :
            md5 = md5

        if typex :
            typex = f":{typex}"
        else :
            typex = ""

        query = f"MATCH (start)-[{typex}]->(end) WHERE start.id = '{md5}' RETURN end.id"
        result = self._execute_query(query)
        edges = []
        for record in result[1]:
            edges.append(RedisNode(md5= record[0].decode(), graph_name=self.graph_name))
        return edges
    
    def get_edge(
            self, 
            md5 : str = None

            
            ):
        """
        get an edge from its id
        """

        query = f"MATCH ()-[e {{id: '{md5}'}}]-() RETURN e"
        result = self._execute_query(query)
        if result and len(result[1]) == 2:
            #edge_properties = result[0]['e']['properties']
            return RedisEdge(md5=md5,graph_name=self.graph_name, load=True)
        return None

    def get_all_nodes(self)-> list['RedisNode']:
        query = f"MATCH (n) RETURN n.id"
        result = self._execute_query(query)
        nodes = []
        for record in result[1]:
            md5 = record[0].decode()
            #properties = record['n']['properties']['properties']
            nodes.append(RedisNode(graph_name = self.graph_name, md5=md5, load=True))
        return nodes

    def get_all_edges(self)-> list['RedisEdge']:
        query = "MATCH ()-[e]->() RETURN e.id"
        result = self._execute_query(query)
        edges = []

        for record in result[1]:
            md5 = record[0].decode()
            edges.append(RedisEdge( md5= md5, graph_name=self.graph_name, load=True))
        return edges

    def _execute_query(self, query):
        conn = get_connect()
        result = conn.execute_command("GRAPH.QUERY", self.graph_name, query)
        conn.close()
        return result 

    def _format_properties(self, properties):
        if properties is None:
            return "{}"
        else:
            return str(properties).replace("'", '"')
        
    def call_procedure(self, procedure, *args, read_only=False, **kwagrs):
        args = [quote_string(arg) for arg in args]
        q = 'CALL %s(%s)' % (procedure, ','.join(args))

        y = kwagrs.get('y', None)
        if y:
            q += ' YIELD %s' % ','.join(y)

        return self.query(q, read_only=read_only)

    def get_label(self, idx):
        """
        Returns a label by it's index

        Args:
            idx: The index of the label
        """
        try:
            label = self._labels[idx]
        except IndexError:
            # Refresh labels.
            self._refresh_labels()
            label = self._labels[idx]
        return label

    def labels(self):
        return self.call_procedure("db.labels", read_only=True).result_set

    def relationshipTypes(self):
        return self.call_procedure("db.relationshipTypes", read_only=True).result_set

    def propertyKeys(self):
        return self.call_procedure("db.propertyKeys", read_only=True).result_set

    def _build_params_header(self, params):
        if not isinstance(params, dict):
            raise TypeError("'params' must be a dict")
        # Header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            params_header += str(key) + "=" + stringify_param_value(value) + " "
        return params_header

    def _clear_schema(self):
        self._labels = []
        self._properties = []
        self._relationshipTypes = []

    def _refresh_labels(self):
        lbls = self.labels()

        # Unpack data.
        self._labels = [None] * len(lbls)
        for i, l in enumerate(lbls):
            self._labels[i] = l[0]

    def _refresh_relations(self):
        rels = self.relationshipTypes()

        # Unpack data.
        self._relationshipTypes = [None] * len(rels)
        for i, r in enumerate(rels):
            self._relationshipTypes[i] = r[0]

    def _refresh_attributes(self):
        props = self.propertyKeys()

        # Unpack data.
        self._properties = [None] * len(props)
        for i, p in enumerate(props):
            self._properties[i] = p[0]

    def __contains__(self, item: 'RedisNode' or 'RedisEdge')->bool:
        return item.exist() and self.graph_name == item.graph_name

    def _refresh_schema(self):
        self._clear_schema()
        self._refresh_labels()
        self._refresh_relations()
        self._refresh_attributes()

    def query(self, q, params=None, timeout=None, read_only=False):
        """
        Executes a query against the graph.

        Args:
            q: the query
            params: query parameters
            timeout: maximum runtime for read queries in milliseconds
            read_only: executes a readonly query if set to True
        """

        # maintain original 'q'
        query = q

        # handle query parameters
        if params is not None:
            query = self._build_params_header(params) + query

        # construct query command
        # ask for compact result-set format
        # specify known graph version
        cmd = "GRAPH.RO_QUERY" if read_only else "GRAPH.QUERY"
        # command = [cmd, self.name, query, "--compact", "version", self.version]
        command = [cmd, self.graph_name, query, "--compact"]

        # include timeout is specified
        if timeout:
            if not isinstance(timeout, int):
                raise Exception("Timeout argument must be a positive integer")
            command += ["timeout", timeout]

        # issue query
        try:
            conn = get_connect()
            response = conn.execute_command(*command)
            conn.close()
            result = QueryResult(self, response)
            return result
        except redis.exceptions.ResponseError as e:
            if "wrong number of arguments" in str(e):
                print("Note: RedisGraph Python requires server version 2.2.8 or above")
            if "unknown command" in str(e) and read_only:
                # `GRAPH.RO_QUERY` is unavailable in older versions.
                return self.query(q, params, timeout, read_only=False)
            raise e
        except VersionMismatchException as e:
            # client view over the graph schema is out of sync
            # set client version and refresh local schema
            self.version = e.version
            self._refresh_schema()
            # re-issue query
            return self.query(q, params, timeout, read_only)


import redis
import datetime
import uuid
class RedisNode():
    def __init__(self, 
                 md5=None, 
                 graph_name=None, 
                 properties={}, 
                 typex=None,
                 create : bool= False, # if 
                 load : bool = False
                 ):
        #UPDATE X-> 
        #self.graph_name = graph_name
        #self.md5 = md5
        self._properties: dict =properties
        self.typex = typex
        #self.redis = redis.Redis()
        #
        if md5 :
            #RedisMetadata.__init__(self, md5, create=create)
            if create:
                if  self.load(md5, graph_name, True):
                    pass 
                else :
                    raise ValueError("Node was not create")
            elif load  :
                self.load(md5, graph_name)
            elif graph_name :
                self.md5= md5
                self.graph_name = graph_name
            else :
                self.md5= md5
                self.graph_name = None

        
        else:
            date = datetime.datetime.utcnow().strftime("%Y-%m-%d#%H:%M:%S.%f%Z")
            uid = uuid.uuid1()
            typex = typex
            import hashlib
            hashid = hashlib.md5("{}-{}-{}".format(typex, date,uid).encode())
            self.md5 = hashid.hexdigest()
            self.graph_name = graph_name

    def __bool__(self):
        return self.exist()

    def __call__(self, 
                 graph_name= None,
                 md5 = None,
                 properties = None,
                 typex=  None
                 ) -> None:
        if graph_name:
            self.graph_name = graph_name

        if md5:
            self.md5 = md5

        if properties:
            self._properties = properties

        if typex :
            self.typex = typex


    @property
    def properties(self):
        if not self.md5 or not self.graph_name:
            raise ValueError("load node before try to get properties")
        query = f"MATCH (n {{id: '{self.md5}'}}) RETURN n"
        result = self._execute_query(query)
        if result and len(result) > 0:
            result = {k.decode(): v.decode() for k,v in result[1][0][0][2][1] if k.decode()!= 'id'}
        
            return result
        else :
            return {}
        
    def cache_properties(self):
        return self._properties

    def _execute_query(self, query)-> int:
        conn = get_connect()
        result = conn.execute_command("GRAPH.QUERY", self.graph_name, query)
        conn.close()
        return result

    def create(self, graph_name=None):
        if self.md5 is None:
            raise ValueError("Node don't have ID")
        if self.graph_name is None :
            if graph_name:
                self.graph_name = graph_name
            else :
                raise ValueError("set graph name before create Node")

        properties_str = self._format_properties(self._properties)
        if self.typex:

            if self._properties:
                clause = ", ".join([f"{key} : '{value}'" for key, value in self._properties.items()])
                query = f"CREATE (:{self.typex} {{id: '{self.md5}', {clause}}})"
            else :
                query = f"CREATE (:{self.typex} {{id: '{self.md5}'}})"
        else :
            if self._properties:
                clause = ", ".join([f"{key} : '{value}'" for key, value in self._properties.items()])
                query = f"CREATE (:Node {{id: '{self.md5}', {clause} }})"
            else :
                query = f"CREATE (:Node {{id: '{self.md5}'}})"
        result = self._execute_query(query)
        number = extract_number(result[0][1].decode())
        return number > 0
        

    def load(self, md5:str, graph_name:str, create:bool = False)->bool:
        if create :
            self.md5 = md5
            self.graph_name = graph_name
            self.create()
            result =  check_node_exist(self.graph_name,self.md5)
            return result
        else: 
            if check_graph_exist(graph_name):
                self.graph_name= graph_name
            else :
                raise ValueError(f'{graph_name} is not an existing graph')
            if check_node_exist(graph_name, md5):
                self.md5 = md5
            else:
                raise ValueError(f' the node {md5} is not existing in the graph {graph_name}')
            if self.typex is None :
                self.typex = self.get_label()

            return True

        

    def update(self, properties:dict=None):
        if (self.graph_name and not check_graph_exist(self.graph_name)) or not self.graph_name:
            raise ValueError(f'load Node before')
        if (self.md5 and not check_node_exist(self.graph_name, self.md5)) or not self.md5:
            raise ValueError(f'load Node before')
        props = self.properties
        if properties :
            update_clause = ", ".join([f"n.{key} = '{value}'" for key, value in properties.items()])
            query = f"MATCH (n) WHERE n.id = '{self.md5}' SET  {update_clause}"
        else :
            update_clause = ", ".join([f"n.{key} = '{value}'" for key, value in self._properties.items()])
            query = f"MATCH (n) WHERE n.id = '{self.md5}' SET  {update_clause}"
        result = self._execute_query(query)
        check = extract_number(result[0][0].decode())
        return check == 1

    def load_properties(self)-> None:
        """
        add existant properties to the cache properties
        """
        self._properties = self._properties | self.properties

    def del_properties(self,name:str):
        query = f"MATCH (n:Node {{id: '{self.md5}'}}) REMOVE n.{name}"
        result = self._execute_query(query)
        return result


    def delete(self):
        if (self.graph_name and not check_graph_exist(self.graph_name)) or not self.graph_name:
            raise ValueError(f'load Node before')
        if (self.md5 and not check_node_exist(self.graph_name, self.md5)) or not self.md5:
            raise ValueError(f'load Node before')
        query = f"MATCH (n {{id: '{self.md5}'}}) DELETE n"
        self._execute_query(query)

    def get_label(self):
        query = f"MATCH (e {{id: '{self.md5}'}}) RETURN labels(e)"
        result = self._execute_query( query)
        #r = extract_string_between_brackets( result[1][0][0].decode() ) 
        if result and len(result) > 0:
            return extract_string_between_brackets( result[1][0][0].decode() ) 
        else :
            raise ValueError("start node doesn't exist") 
        
    def get_parents_nodes(
            self , 
            typex :str = None # le type d'edge
            )-> list['RedisNode']:
        """
        get all parents node from a child node
        """

        md5 = self.md5

        if typex :
            typex = f":{typex}"
        else :
            typex = ""

        query = f"MATCH (start)-[{typex}]->(end) WHERE end.id = '{md5}' RETURN start.id"
        result = self._execute_query(query)
        edges = []
        for record in result[1]:
            edges.append(RedisNode(md5= record[0].decode(), graph_name=self.graph_name, load=True))

        return edges
    
    def get_child_node(
            self,
            md5,
            label:str=None,
            ):
        if label :
            label = f":{label}"
        else:
            label = ""
        query = f"MATCH (start)-[:Slot]->(end) "\
                f"WHERE start.id = '{label}' and end.id = '{md5}' "\
                f"RETURN end.id"
        nodes = []
        result = self._execute_query(query)
        for record in result[1]:
            nodes.append(RedisNode(md5= record[0].decode(), graph_name=self.graph_name, load=True))
        if len(nodes)==1:
            return nodes[0]
        else :
            ValueError('There are more than one Node')

    def get_childs_nodes(
            self , 
            typex :str = None # le type d'edge
            )-> list['RedisNode']:
        """
        get all childs nodes from a parent nodes
        """

        md5 = self.md5

        if typex :
            typex = f":{typex}"
        else :
            typex = ""

        query = f"MATCH (start)-[{typex}]->(end) WHERE start.id = '{md5}' RETURN end.id"
        result = self._execute_query(query)
        edges = []
        for record in result[1]:
            edges.append(RedisNode(md5= record[0].decode(), graph_name=self.graph_name, load=True))
        return edges
    
    def get_node_child_edges(
            self , 
            typex :str = None # le type d'edge
            )-> list['RedisEdge']:
        """
        récupérer les edges d'un noeud
        """
        md5 = self.md5
        if typex :
            typex = f":{typex}"
        else :
            typex = ""
        #UPDATE @-> DONE retourn une liste de RedisEdges et donc recup l'id des edges  et les load succintement  DONE
        #query = f"MATCH (start)-[e]->() WHERE start.id = '{md5}' RETURN e"
        query = f"MATCH (start)-[e{typex}]->() WHERE start.id = '{md5}' RETURN e.id"
        result = self._execute_query(query)
        edges = []
        
        for record in result[1]:
            edge_md5=record[0]
            edges.append(RedisEdge(md5= record[0].decode(), graph_name=self.graph_name, load=True))
        return edges
    
    def get_node_parent_edges(
            self , 
            typex :str = None # le type d'edge
            )-> list['RedisEdge']:
        """
        récupérer les edges d'un noeud
        """
        md5 = self.md5
        if typex :
            typex = f":{typex}"
        else :
            typex = ""
        #UPDATE @-> DONE retourn une liste de RedisEdges et donc recup l'id des edges  et les load succintement  DONE
        query = f"MATCH ()-[e{typex}]->(end) WHERE end.id = '{md5}' RETURN e.id"
        result = self._execute_query(query)
        edges = []
        for record in result:
            edges.append(RedisEdge(md5= record[1][0][0].decode(), graph_name=self.graph_name, load=True))
        return edges
    
    def __eq__(self, other: 'RedisNode'):
        if check_node_exist(other.graph_name, other.md5):
            return self.graph_name == other.graph_name and self.md5 == other.md5
        else :
            return False
        
    def exist(self)->bool:
        return check_node_exist(self.graph_name, self.md5)

    def __getitem__(self, key):
        return self._properties[key]

    def __setitem__(self, key, value):
        self._properties[key] = value

    def __delitem__(self, key):
        del self._properties[key]

    def __contains__(self, key):
        return key in self._properties

    def __len__(self):
        return len(self._properties)

    def __str__(self):
        return f"Node {self.typex} (ID: {self.md5}, Properties: {self.properties})"

    def __repr__(self):
        return str(self)

    def _format_properties(self, properties):
        if properties is None:
            return "{}"
        else:
            return str(properties).replace("'", '"')


import redis

class RedisEdge:
    def __init__(self, 
                 
                 start_node_md5 : RedisNode|str=None, 
                 end_node_md5 : RedisNode|str=None, 
                 md5:str=None, 
                 graph_name:str= None, 
                 properties:dict={}, 
                 create : bool = False,
                 load :bool = False,
                 label :str = None
                 ):
        #self.graph_name = graph_name
        #self.md5 = md5
        self.graph_name = None
        if isinstance(start_node_md5, RedisNode):
            if not graph_name:
                #x = start_node_md5.graph_name
                self.graph_name = start_node_md5.graph_name
            start_node_md5 = start_node_md5.md5

        if isinstance(end_node_md5, RedisNode):
            if self.graph_name != end_node_md5.graph_name:
                raise ValueError("the graph name of start node and end node are different")
            end_node_md5 = end_node_md5.md5

        self._properties =properties

        if md5 :
            if create:
                if self.create(md5, graph_name, True):
                    pass
                else :
                    raise ValueError("Edge was not create")
            #self.load(edge_md5)
            elif load :
                self.load(md5, graph_name)
            elif graph_name:
                self.md5 =md5
                self.graph_name = graph_name
                self.start_node_id = start_node_md5
                self.end_node_id = end_node_md5
                self.label = label
            else:
                #pas besoin de graph_name car il a était set par le start Node
                self.md5 = md5
                self.start_node_id = start_node_md5
                self.end_node_id = end_node_md5
                self.label =label



        else :
            if  not self.graph_name :
                self.graph_name = graph_name

            self.start_node_id = start_node_md5
            self.end_node_id = end_node_md5
            self._properties = properties or {}
            self.label = label
            date = datetime.datetime.utcnow().strftime("%Y-%m-%d#%H:%M:%S.%f%Z")
            uid = uuid.uuid1()
            import hashlib
            hashid = hashlib.md5("{}-{}-{}".format(RedisEdge.__name__, date, uid).encode())
            self.md5 = hashid.hexdigest()
            if create:
                self.create()

        # if md5 :
        #     #RedisMetadata.__init__(self, md5, create=create)
        #     if create:
        #         if  self.load(md5, graph_name, True):
        #             pass 
        #         else :
        #             raise ValueError("Node was not create")
        #     elif (load or check_node_exist(graph_name, md5)) and graph_name :
        #         self.load(md5, graph_name)
        #     elif graph_name :
        #         self.md5= md5
        #         self.graph_name = graph_name
        #     else :
        #         self.md5= md5
        
        # else:
        #     date = datetime.datetime.utcnow().strftime("%Y-%m-%d#%H:%M:%S.%f%Z")
        #     uid = uuid.uuid1()
        #     typex = typex
        #     import hashlib
        #     hashid = hashlib.md5("{}-{}-{}".format(typex, self.date,self.uid).encode())
        #     self.md5 = hashid.hexdigest()
        #     self.graph_name = graph_name

    def __call__(self, 
                 graph_name= None,
                 md5 = None,
                 properties = None,
                 start_node_md5 = None,
                 end_node_md5 = None,
                 label = None 
                 ) -> Any:
        if graph_name:
            self.graph_name = graph_name

        if md5:
            self.md5 = md5

        if label:
            self.label = label

        if properties:
            self._properties = properties

        if  start_node_md5:
            self.start_node_id = start_node_md5

        if end_node_md5:
            self.end_node_id = end_node_md5

    @property
    def properties(self):
        if not self.md5 or not self.graph_name:
            raise ValueError("load node before try to get properties")
        query = f"MATCH ()-[e {{id: '{self.md5}'}}]-() RETURN e"
        #query = f"MATCH (n {{id: '{self.md5}'}}) RETURN n"
        result = self._execute_query(query)
        test = result[1][0][0][4][1]
        if result and len(result) > 0:
            result = {k.decode(): v.decode() for k,v in result[1][0][0][4][1] if k.decode()!= 'id'}
        
            return result
        else :
            return {}

    def create(self, 
               start_md5 = None , 
               end_md5 = None, 
               md5 = None, 
               graph_name = None, 
               properties = None):
        if not start_md5 :
            start_md5 = self.start_node_id
        if not end_md5:
            end_md5 = self.end_node_id

        if not self.md5 and md5:
            self.md5 = md5

        if not  self.graph_name and graph_name:
            self.graph_name = graph_name

        if self.graph_name is None:
            raise ValueError("you can't create Edge without graph_name")

        if self.md5 is None:
            raise ValueError("Edge doesn't have id")
        if start_md5 is None :
            raise ValueError(f' the start node is not set')
        if end_md5 is None: 
            raise ValueError(f' the end node is not set')
        
        # opour creer un Edge il faut deja des Nodes de crée 
        if not check_graph_exist(self.graph_name):
            raise ValueError(f"graph {self.graph_name} doesn't exist")
        if not check_node_exist(self.graph_name,self.start_node_id):
            raise ValueError(f"start node {self.start_node_id}  doesn't exist ")
        if not check_node_exist(self.graph_name,self.end_node_id):
            raise ValueError(f"end node {self.end_node_id} doesn't exist ")

        #properties_str = self._format_properties(self._properties)
        

        if self.label:
            if self._properties:
                clause = ", ".join([f"e.{key} = '{value}'" for key, value in self._properties.items()])
                query = f"MATCH (start), (end) WHERE start.id = '{start_md5}' AND end.id = '{end_md5}' " \
                            f"MERGE (start)-[e:{self.label} {{id : '{self.md5}' }}]->(end) " \
                            f"ON CREATE SET {clause} " \
                            f"RETURN e"
            else :
                query = f"MATCH (start), (end) WHERE start.id = '{start_md5}' AND end.id = '{end_md5}' " \
                            f"MERGE (start)-[e:{self.label} {{id : '{self.md5}' }}]->(end) " \
                            f"RETURN e"
        else:
            if self._properties:
                clause = ", ".join([f"e.{key} = '{value}'" for key, value in self._properties.items()])
                query = f"MATCH (start), (end) WHERE start.id = '{start_md5}' AND end.id = '{end_md5}' " \
                            f"MERGE (start)-[e:Edge {{id : '{self.md5}' }}]->(end) " \
                            f"ON CREATE SET {clause} " \
                            f"RETURN e"
            else :
                query = f"MATCH (start), (end) WHERE start.id = '{start_md5}' AND end.id = '{end_md5}' " \
                            f"MERGE (start)-[e:Edge {{id : '{self.md5}' }}]->(end) " \
                            f"RETURN e"

        result =self._execute_query(query)
        return extract_number(result[2][1].decode()) == 1

    def load(self, md5, graph_name):
        # if not check_node_exist(graph_name, self.start_node_id) :
        #     raise ValueError(f' the start node {self.start_node_id} is not existing in the graph {graph_name}')
        # if not check_node_exist(graph_name, self.end_node_id) :
        #     raise ValueError(f' the end node {self.end_node_id} is not existing in the graph {graph_name}') 
        if check_graph_exist(graph_name):
            self.graph_name= graph_name
        else :
            raise ValueError(f'{graph_name} is not an existing graph')
        if check_edge_exists(self.graph_name,md5):
            self.md5 = md5
        else:
            raise ValueError(f' the edge {md5} is not existing in the graph {graph_name}')
        self.start_node_id = self.get_start_md5()
        self.end_node_id = self.get_end_md5()
        self.label = self.get_label()
        return True
    
    def get_label(self)->str:
        query = f"MATCH ()-[e {{id: '{self.md5}'}}]->() RETURN type(e)"
        result = self._execute_query( query)
        if result and len(result) > 0:
            return result[1][0][0].decode()
        else :
            raise ValueError("start node doesn't exist") 

    def get_start_md5(self):
        query = f"MATCH (start)-[e {{id: '{self.md5}'}}]->() RETURN start.id"
        result = self._execute_query( query)
        if result and len(result) > 0:
            return result[1][0][0].decode()
        else :
            raise ValueError("start node doesn't exist")

    def get_end_md5(self):
        query = f"MATCH ()-[e {{id: '{self.md5}'}}]->(end) RETURN end.id"
        result = self._execute_query( query)
        if result and len(result) > 0:
            return result[1][0][0].decode()
        else :
            raise ValueError("start node doesn't exist")
        
    def get_start_node(self)->RedisNode:
        return RedisNode(graph_name =self.graph_name, md5=self.get_start_md5(), load=True)
    
    def get_end_node(self)->RedisNode:
        return RedisNode(graph_name= self.graph_name,md5= self.get_end_md5(), load= True)

    def delete(self, edge_md5=None, parent_md5=None, child_md5=None):
        if (edge_md5 is None and  self.md5 is None) or  \
        (
            (parent_md5 is None and self.start_node_id is None )or \
          (child_md5 is None and self.end_node_id is None)
          ) :
            raise ValueError("Please provide either edge_id or both parent_id and child_id")
        
        if not parent_md5:
            parent_md5 = self.start_node_id

        if not child_md5 :
            child_md5 = self.end_node_id

        if not edge_md5:
            edge_md5 = self.md5
        
        if edge_md5 is not None:
            query = f"MATCH ()-[e {{id: '{edge_md5}'}}]-() DELETE e"
        else:
            query = f"MATCH (parent)-[e]->(child) WHERE parent.id = '{parent_md5}' AND child.id = '{child_md5}' DELETE e"
        
        return self._execute_query( query)

    def _execute_query(self, query):
        if not self.graph_name:
            raise ValueError("Set") 
        conn = get_connect()
        result = conn.execute_command("GRAPH.QUERY", self.graph_name, query)
        conn.close()
        return result

    def update( self, properties, clean=False):
        #conn = get_connect()
        if (self.graph_name and not check_graph_exist(self.graph_name)) or not self.graph_name:
            raise ValueError(f'load Node before')
        if (self.md5 and not check_edge_exists(self.graph_name, self.md5)) or not self.md5:
            raise ValueError(f'load Edge before')


        if clean: # si on veut clean cad remplacer complètement les properties
            if properties:
                # si on a mis des properties dans les parametre de fonction
                clean_clause = self._format_properties(properties)
                query = f"MATCH ()-[e {{id: '{self.md5}'}}]-() SET e = {clean_clause}"
            else :
                # pas properties alors on utlise les cache properties
                if self._properties :
                    clean_clause = self._format_properties(self._properties)
                    query = f"MATCH ()-[e {{id: '{self.md5}'}}]-() SET e = {clean_clause}"
                else :
                    return 
        else:
            # si on veut juste mettre à jour les properties deja existantes
            if properties :
                update_clause = ", ".join([f"e.{key} = '{value}'" for key, value in properties.items()])
                query = f"MATCH ()-[e {{id: '{self.md5}'}}]-() SET  {update_clause}"
            else :
                update_clause = ", ".join([f"e.{key} = '{value}'" for key, value in self._properties.items()])
                query = f"MATCH ()-[e {{id: '{self.md5}'}}]-() SET  {update_clause}"
    
        result = self._execute_query(query)
        #conn.close()
        check = extract_number(result[0][0].decode())
        return check
    
    def __eq__(self, other: 'RedisEdge'):
        if check_edge_exists(other.graph_name, other.md5):
            return self.graph_name == other.graph_name and self.md5 == other.md5
        else :
            return False


    def __getitem__(self, key):
        return self.properties[key]

    def __setitem__(self, key, value):
        self.properties[key] = value

    def __delitem__(self, key):
        del self.properties[key]

    def __contains__(self, key):
        return key in self.properties

    def __len__(self):
        return len(self.properties)

    def __str__(self):
        return f"Edge(ID: {self.md5}, Start Node ID: {self.start_node_id}, End Node ID: {self.end_node_id}, Properties: {self.properties})"

    def __repr__(self):
        return str(self)

    def _format_properties(self, properties):
        if properties is None:
            return "{}"
        else:
            return str(properties).replace("'", '"')