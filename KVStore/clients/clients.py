from typing import Union, Dict
import grpc
import logging
import KVStore.protos.kv_store_pb2
from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None

class SimpleClient:
    def __init__(self, kvstore_address: str):

        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)

    def get(self, key: int) -> Union[str, None]:
          get_request=KVStore.protos.kv_store_pb2.GetRequest()
          get_request.key = key
          respuesta=self.stub.Get(get_request)
          resp=_get_return(respuesta)
       	  return resp

    def l_pop(self, key: int) -> Union[str, None]:
        
        lpop_request=KVStore.protos.kv_store_pb2.GetRequest()
        lpop_request.key = key
        respuesta=self.stub.LPop(lpop_request)
        resp=_get_return(respuesta)
        return resp

    def r_pop(self, key: int) -> Union[str, None]:
        
        rpop_request=KVStore.protos.kv_store_pb2.GetRequest()
        rpop_request.key = key
        respuesta=self.stub.RPop(rpop_request)
        resp=_get_return(respuesta)
        return resp
        
    def put(self, key: int, value: str):
    
        put_request=KVStore.protos.kv_store_pb2.PutRequest()
        put_request.key = key
        put_request.value = value
        self.stub.Put(put_request)
        
    def append(self, key: int, value: str):
    
        append_request=KVStore.protos.kv_store_pb2.AppendRequest()
        append_request.key = key
        append_request.value = value
        self.stub.Append(append_request)
        
    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)

    def address_config(self, key) -> Union[str, None]:
    
        query_request=KVStore.protos.kv_store_shardmaster_pb2.QueryRequest()
        query_request.key = key
        query_response=self.stub.Query(query_request)
        address=query_response.server
        client=SimpleClient(address)
        return client

    def get(self, key: int) -> Union[str, None]:
        
        client = self.address_config(key)
        return client.get(key)

    def l_pop(self, key: int) -> Union[str, None]:
        
        client = self.address_config(key)
        return client.l_pop(key)
        
    def r_pop(self, key: int) -> Union[str, None]:
    
        client = self.address_config(key)
        return client.r_pop(key)

    def put(self, key: int, value: str):
        
        client = self.address_config(key)
        return client.put(key,value)

    def append(self, key: int, value: str):
    
        client = self.address_config(key)
        return client.append(key,value)
        
class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """
    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def put(self, key: int, value: str):
        """
        To fill with your code
        """


    def append(self, key: int, value: str):
        """
        To fill with your code
        """

