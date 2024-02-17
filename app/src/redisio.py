import pdb
from .redisdata import RedisObject
import asyncio
from .buffer import BufferMultiQueue
import base64

EMPTY_RDB = base64.b64decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
class RedisIOHandler:
    def __init__(self, redis_server=None):
        self.parsed_input = None
        self.parsed_output = None
        self.redis_server = redis_server
        self.redis_server.redis_io_handler = self
        self.buffer = BufferMultiQueue() 
        self.session = {} # client_id:uuid -> dict  ->  dict['client_port']   

    def execute_command(self, client_id=None, input_redisobject=None, **kwargs):

        _is_master = self.redis_server.role =='master'
        _reply_null = lambda *args: RedisObject(obj="", typ="null_bulk_str")
        handler ={"ping": self.redis_server.command_ping,
                  "PING": self.redis_server.command_ping,
                  "echo": self.redis_server.command_echo, 
                  "ECHO": self.redis_server.command_echo, 
                  "set": self.redis_server.command_set, 
                  "SET": self.redis_server.command_set, 
                  "get": self.redis_server.command_get, 
                  "GET": self.redis_server.command_get, 
                  "keys": self.redis_server.command_keys, 
                  "KEYS": self.redis_server.command_keys, 
                  "config": self.redis_server.command_config, 
                  "CONFIG": self.redis_server.command_config, 
                  "info": self.redis_server.command_info, 
                  "INFO": self.redis_server.command_info, 
                  "REPLCONF": self.redis_server.command_replconf if _is_master else _reply_null, 
                  "replconf": self.redis_server.command_replconf if _is_master else _reply_null, 
                  "PSYNC": self.redis_server.command_psync if _is_master else _reply_null, 
                  "psync": self.redis_server.command_psync if _is_master else _reply_null, 
                 }

        if input_redisobject.typ == "str" or input_redisobject.typ == "bulk_str":
           try:
               _cmd = str(input_redisobject.obj)
               output_redisobject = handler[_cmd]()
               return output_redisobject
           except:
               print("unknown command: ", str(input_redisobject))
               self.parsed_output = RedisObject.from_string("")
               return self.parsed_output

        elif input_redisobject.typ == "list":
           try:
               _cmd = str(input_redisobject.obj[0].obj)
               output_redisobject = handler[_cmd](client_id=client_id, command=_cmd, *(input_redisobject.obj[1:]))
               return output_redisobject
           except:
               print("unknown command: ", str(input_redisobject))
               output_redisobject = RedisObject.from_string("")
               return output_redisobject
            

    async def process_buffer_commands(self, reader, writer, **kwargs):
        '''
        clear buffer and process each commands
        '''

        
        _port = self.session[kwargs['client_id']]['client_port']

        print(f"processing replica at port  {_port}") 
        # support buffering for master server only
        assert self.redis_server.role == "master"
        while (not self.buffer[_port].is_empty()):
            print(f"<process buffer commands to slave server at port {_port}>")
            if (self.buffer[_port].dequeue() == "send_empty_rdb"):
                _len = len(EMPTY_RDB)
                _empty_rdb_resp_encode = ('$' + str(_len) + "\r\n").encode() + EMPTY_RDB 
                print("rdb ", _empty_rdb_resp_encode.decode('latin-1'))
                writer.write(_empty_rdb_resp_encode)
                await writer.drain()

    
    def parse_input(self, input_string):
        '''
        redis RESP input_string -> self.parsed_input as a RedisObject
        '''
        return RedisObject.from_string(input_string)

    def parse_output(self, output_redisobject):
        '''
        RedisObject self.parsed_output -> output RESP string
        '''
        return str(output_redisobject)

