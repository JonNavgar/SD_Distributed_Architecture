import time
import random
from typing import Dict, Union, List
import logging
import grpc
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub
import threading 
from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService:

    def __init__(self):
        pass

    def get(self, key: int) -> str:
        pass

    def l_pop(self, key: int) -> str:
        pass

    def r_pop(self, key: int) -> str:
        pass

    def put(self, key: int, value: str):
        pass

    def append(self, key: int, value: str):
        pass

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
    
        self.database={}
        self.lock = threading.Lock()
        
    def get(self, key: int) -> Union[str, None]:
    
          return self.database.get(key)
          
    def l_pop(self, key: int) -> Union[str, None]:
        
        self.lock.acquire()
        value=self.database.get(key)
        pop = value[0]
        val = value[1:]
        self.database[key]=val
        self.lock.release()
        return pop

    def r_pop(self, key: int) -> Union[str, None]:
        
        self.lock.acquire()
        value=self.database.get(key)
        pop = value[-1]
        val = value[:-1]
        self.database[key]=val
        self.lock.release()
        return pop



    def put(self, key: int, value: str):
    
        self.database[key]=value
        
    def append(self, key: int, value: str):
        
      self.lock.acquire()
      if key in self.database:
        self.database[key]= self.database[key] + value 
      else:
        self.database[key] = value
      self.lock.release()
       
    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
       
       transfer_request=Transfer_request()
       for clave, valor in database.items():
         if lower_val >= clave < upper_val:
           key_value=KeyValue(key=clave,value=valor)
           transfer_request.key_values.append(key_value)
       channel = grpc.insecure_channel(destination_server)
       stub = KVStoreStub(channel)
       stub.Transfer(transfer_request)
          
    def transfer(self, keys_values: List[KeyValue]):
       
       new_database={} 
       for pair in keys_values.pairs:
         new_database[pair.key]=pair.value
       self.database.update(new_database)
       

class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
        """
        """
    def put(self, key: int, value: str):
        """
        To fill with your code
        """

    def append(self, key: int, value: str):
        """
        To fill with your code
        """

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:

        key=request.key
        value=self.storage_service.get(key)
        if value is not None:
          respuesta=GetResponse(value=value)
        else:
          respuesta=GetResponse()
        return respuesta

    def LPop(self, request: GetRequest, context) -> GetResponse:
               
        key = request.key
        value=self.storage_service.get(key)
        if value is None:
          return GetResponse()
        elif len(value)==0:
          return GetResponse(value="")
        else:
          pop=self.storage_service.l_pop(key)
          return GetResponse(value=pop)
          
    def RPop(self, request: GetRequest, context) -> GetResponse:
        
        key = request.key
        value=self.storage_service.get(key)
        if value is None:
          return GetResponse()
        elif len(value)==0:
          return GetResponse(value="")
        else:
          pop=self.storage_service.r_pop(key)
          return GetResponse(value=pop)
          
    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
    
        key = request.key
        value = request.value
        self.storage_service.put(key,value)
        return google_dot_protobuf_dot_empty__pb2.Empty()
        
    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
    
        key = request.key
        value = request.value
        self.storage_service.append(key,value)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
       
       address = request.destination_server 
       lower_key = request.lower_val
       upper_key = request.upper_val
       self.storage_service.redistribute(address,lower_key,upper_key)
       return google_dot_protobuf_dot_empty__pb2.Empty()
       
    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        
       self.storage_service.transfer(request.keys_values)
       return google_dot_protobuf_dot_empty__pb2.Empty()        

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """
