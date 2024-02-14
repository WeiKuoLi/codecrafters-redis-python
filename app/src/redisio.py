from .redisdata import RedisObject
import asyncio

class RedisIOHandler:
    def __init__(self, redis_server=None):
        self.parsed_input = None
        self.parsed_output = None
        self.redis_server = redis_server


    def execute_command(self):
        handler ={"ping": self.redis_server.command_ping,
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
            


    
    def parse_input(self, input_string):
        # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
        # "*1\r\n$4\r\nping\r\n"
        # "+PONG\r\n"
        self.parsed_input = RedisObject.from_string(input_string)
    def parse_output(self):
        return str(self.parsed_output)

