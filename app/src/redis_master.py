from .redis_base import RedisServer
from .redisdata import RedisObject
from .buffer import BufferQueue
import asyncio

class RedisServerMaster(RedisServer):
    '''
    Master Redis server
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.role="master"
        self.replid="8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.repl_offset=0
        self.slave_port = []

    def command_info(self, *args, **kwargs):
        if args[0].obj != "replication":
            return RedisObject.from_string("")
        _info = f"role:{self.role}\n"
        _info += f"master_replid:{self.replid}\n"
        _info += f"master_repl_offset:{self.repl_offset}\n"
        return RedisObject(obj=_info, typ="bulk_str") 
   
    def command_replconf(self, *args, **kwargs):
        if(args[0].obj == 'listening-port'):
            slave_port = str(args[1].obj)
            self.slave_port.append(slave_port)
            session_client_id = kwargs['client_id']
            self.redis_io_handler.session[session_client_id]['client_port'] = str(args[1].obj)

            self.redis_io_handler.buffer[slave_port] = BufferQueue()
            
        print(f"slave port is {self.slave_port}")
        return RedisObject("OK")

    def command_psync(self, *args, **kwargs):
        session_client_id = kwargs['client_id']
        slave_port = self.redis_io_handler.session[session_client_id]['client_port']

        print("buffer: ", str(self.redis_io_handler.buffer))
        print("redisiohandler: ", self.redis_io_handler)
        print("buffer: ", str(self.redis_io_handler.buffer[slave_port]))
        self.redis_io_handler.buffer[slave_port].enqueue("send_empty_rdb")
        print(f"buffer[{self.slave_port}]: ", str(self.redis_io_handler.buffer[slave_port]))
        return RedisObject(obj=f"FULLRESYNC {self.replid} {str(self.repl_offset)}", typ="str")
