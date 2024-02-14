from .redis import RedisServer
from .redisdata import RedisObject
import asyncio
class RedisServerMaster(RedisServer):
    '''
    Base class for my Redis server
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.role="master"
        self.replid="8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.repl_offset=0


    def command_info(self, *args):
        if args[0].obj != "replication":
            return RedisObject.from_string("")
        _info = f"role:{self.role}\n"
        _info += f"master_replid:{self.replid}\n"
        _info += f"master_repl_offset:{self.repl_offset}\n"
        return RedisObject(obj=_info, typ="bulk_str") 
   
    def command_replconf(self, *args):
        pass
        return RedisObject("OK")

    def command_psync(self, *args):
        return RedisObject([
            RedisObject(obj="FULLRESYNC", typ="str"),
            RedisObject(obj=self.replid, typ="str"),
            RedisObject(obj=str(self.repl_offset), typ="str")])
