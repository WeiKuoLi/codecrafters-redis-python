import pdb
from app.src.redisdata import RedisObject
import asyncio
from app.src.buffer import BufferMultiQueue
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

    def execute_master_command(self, client_id=None, input_redisobject=None, **kwargs):
        print("executing commands from master")
        return self.execute_command(client_id=client_id, input_redisobject=input_redisobject, **kwargs)
    
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
                  "REPLCONF": self.redis_server.command_replconf , 
                  "replconf": self.redis_server.command_replconf , 
                  "PSYNC": self.redis_server.command_psync if _is_master else _reply_null, 
                  "psync": self.redis_server.command_psync if _is_master else _reply_null, 
                 }
        '''
        if input_redisobject.typ == "str" or input_redisobject.typ == "bulk_str":
            try:
                _cmd = str(input_redisobject.obj)
                output_redisobject = handler[_cmd]()
                return output_redisobject
            except:
                print("unknown command str: ", str(input_redisobject))
                output_redisobject = RedisObject.from_string("")
                return output_redisobject
        '''
        try:
            if(len(input_redisobject.obj) == 1): 
                _cmd = str(input_redisobject.obj[0].obj[0].obj)
                output_redisobject = handler[_cmd](client_id=client_id, command=_cmd, *(input_redisobject.obj[0].obj[1:]))
                print('one command: ',output_redisobject.__repr__())
                return output_redisobject

            output_redisobject = RedisObject(obj=[], typ='list')
            for _obj in input_redisobject.obj:
                _cmd = str(_obj.obj[0].obj)
                output_redisobject.obj.append( handler[_cmd](client_id=client_id, command=_cmd, *(_obj.obj[1:])))

            print('many commands: ',output_redisobject.__repr__())
            return output_redisobject

        except:
            print("unknown command list: ", str(input_redisobject))
            output_redisobject = RedisObject.from_string("")
            return output_redisobject
            

    async def process_buffer_commands(self, reader, writer, client_id=None,  **kwargs):
        '''
        clear buffer and process each commands
        '''
        #print("CLEARING BUFFER")
        
        if(client_id is None):
            print("client_id is required")
            return

        _port = self.session[client_id]['client_port']

        #print(f"processing replica at port  {_port}") 
        # support buffering for master server only
        assert self.redis_server.role == "master"
        
        while (not self.buffer[_port].is_empty()):
            #print(f"{str(self.buffer[_port])}")
            #print(f"<process buffer commands to slave server at port {_port}>")
            oldest_command_redisobject = self.buffer[_port].dequeue() #dequeue
            #print(f"<process buffer command {(oldest_command_redisobject).__repr__()}")
            if (oldest_command_redisobject.obj == "send_empty_rdb"):
                _len = len(EMPTY_RDB)
                _empty_rdb_resp_encode = ('$' + str(_len) + "\r\n").encode() + EMPTY_RDB 
               # print("rdb ", _empty_rdb_resp_encode.decode('latin-1'))
                writer.write(_empty_rdb_resp_encode)
                await writer.drain()
            else:# //list
                _resp_string = str(oldest_command_redisobject)
                #print(f"send {oldest_command_redisobject.__repr__()} to {_port}")
                
                writer.write(_resp_string.encode())
                await writer.drain()
                #print("DELAY BUFFER CLEARING .1s FOR TESTING")
                await asyncio.sleep(.1)
                
        #print("END CLEARING BUFFER")
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

