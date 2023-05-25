import logging
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
import grpc
logger = logging.getLogger(__name__)


class ShardMasterService:
    def join(self, server: str):
        pass

    def leave(self, server: str):
        pass

    def query(self, key: int) -> str:
        pass

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
       self.num_servers=0
       self.servers={}
       self.max_key=100
       
    def join(self, server: str):
       
      self.num_servers = self.num_servers + 1
      keys_for_server=int(self.max_key / self.num_servers)
      new_servers={}
      # Crear nuevo diccionario para hacer overlap
      i=0
      for clave, address in self.servers.items():
        key1=i*keys_for_server
        key2=(i*keys_for_server)+(keys_for_server-1)
        new_servers[(key1,key2)]=address 
        i=i+1            
      # El ultimo valor del nuevo diccionario hay que completarlo con la nueva address
      i=self.num_servers-1
      key1=i*keys_for_server
      key2=(i*keys_for_server)+(keys_for_server-1)
      new_servers[(key1,key2)]=server
      # Comprobar overlap
      for keys1, val1 in self.servers.items():
        for keys2, val2 in new_servers.items():
        # Verificar si hay superposición entre las llaves de las posiciones actuales
           valor1_min, valor1_max = keys1
           valor2_min, valor2_max = keys2
           interseccion_min = max(valor1_min, valor2_min)
           interseccion_max = min(valor1_max, valor2_max)
           if interseccion_min <= interseccion_max:
             redistribute_request=RedistributeRequest(destination_server=server,lower_val=interseccion_min,upper_val=interseccion_max)
             channel = grpc.insecure_channel(server)
             stub = KVStoreStub(channel)
             stub.Redistribute(redistribute_request)
      self.servers = new_servers.copy()         
     
    def leave(self, server: str):
      if self.num_servers > 1:
        self.num_servers = self.num_servers - 1
        keys_for_server=int(self.max_key / self.num_servers)
        new_servers={}
        # Crear nuevo diccionario para hacer overlap
        i=0
        for clave, address in self.servers.items():
        # Si la direccion que me pasan es diferente a la que recorro entonces hay que añadirla al nuevo diccionario
          if address!=server:
            key1=i*keys_for_server
            key2=(i*keys_for_server)+(keys_for_server-1)
            new_servers[(key1,key2)]=address 
            i=i+1            
	# Comprobar overlap
        for keys1, val1 in self.servers.items():
          for keys2, val2 in new_servers.items():
          # Verificar si hay superposición entre las llaves de las posiciones actuales
            valor1_min, valor1_max = keys1
            valor2_min, valor2_max = keys2
            interseccion_min = max(valor1_min, valor2_min)
            interseccion_max = min(valor1_max, valor2_max)
            if interseccion_min <= interseccion_max:
              redistribute_request=RedistributeRequest(destination_server=server,lower_val=interseccion_min,upper_val=interseccion_max)
              channel = grpc.insecure_channel(server)
              stub = KVStoreStub(channel)
              stub.Redistribute(redistribute_request)
        self.servers = new_servers.copy()  
      else: 
        self.num_servers=0
        self.servers={}      
    def query(self, key: int) -> str:
    
          for (start,end), address in self.servers.items():
            if key >= start and key <= end:
              return address

class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        
       self.shard_master_service.join(request.server)
       return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        
       self.shard_master_service.leave(request.server)
       return google_dot_protobuf_dot_empty__pb2.Empty()
       
    def Query(self, request: QueryRequest, context) -> QueryResponse:
        
        address=self.shard_master_service.query(request.key)
        return QueryResponse(server=address)

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        """
        To fill with your code
        """

