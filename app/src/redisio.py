from .redisdata import RedisObject
import asyncio
from .buffer import BufferMultiQueue
import base64

EMPTY_RDB = base64.b64decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
EMPTY_RDB_STRING = EMPTY_RDB.decode() #'utf-8')
class RedisIOHandler:
    def __init__(self, redis_server=None):
        self.parsed_input = None
        self.parsed_output = None
        self.redis_server = redis_server
        self.redis_server.redis_io_handler = self
        self.buffer = BufferMultiQueue() 
    

    def execute_command(self):
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

        if self.parsed_input.typ == "str" or self.parsed_input.typ == "bulk_str":
           try:
               _cmd = str(self.parsed_input.obj)
               self.parsed_output = handler[_cmd]()
           except:
               self.parsed_output = RedisObject.from_string("")
        elif self.parsed_input.typ == "list":
           try:
               _cmd = str(self.parsed_input.obj[0].obj)
               self.parsed_output = handler[_cmd](*(self.parsed_input.obj[1:]))
           except:
               self.parsed_output = RedisObject.from_string("")
            

    async def process_buffer_commands(self, reader, writer):
        '''
        clear buffer and process each commands
        '''
        # Access the transport object associated with the writer
        _transport = writer.get_extra_info('transport')

        # Get the socket associated with the transport
        _socket = _transport.get_extra_info('socket')

        # Get the port of the socket
        _port = _socket.getsockname()[1]
        print(f"writer port is {_port}") 
        # support buffering for master server only
        assert self.redis_server.role == "master"
        while (not self.buffer[_port].is_empty()):
            print(f"<process buffer commands to slave server at port {_port}>")
            if (self.buffer[_port].dequeue() == "send_empty_rdb"):
                _len = len(EMPTY_RDB)
                _empty_rdb_resp = '$' + str(_len) + '\r\n' + EMPTY_RDB
                writer.write(_empty_rdb_resp.encode())
                await writer.drain()

    
    def parse_input(self, input_string):
        '''
        redis RESP input_string -> self.parsed_input as a RedisObject
        '''
        self.parsed_input = RedisObject.from_string(input_string)
    
    def parse_output(self):
        '''
        RedisObject self.parsed_output -> output RESP string
        '''
        return str(self.parsed_output)

