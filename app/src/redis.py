from .redisdata import RedisObject
import asyncio
class RedisServer:
    '''
    Base class for my Redis server
    '''
    def __init__(self, port_number=6379, *args, **kwargs):
        self.redis = {}
        self.rdb_dir = "" 
        self.rdb_dbfilename = ""
        self.port_number = port_number
        self.role = "master"

    async def delete_key(self, _key, millisecond):
        assert isinstance(_key, str)
        await asyncio.sleep(millisecond / 1000)
        del self.redis[_key]

    def command_ping(self, *args):
        return RedisObject.from_string("+PONG\r\n")
    
    def command_echo(self, *args):
        try: 
            return args[0]
        except:
            return RedisObject.from_string("")
    
    def command_set(self, *args):
        try:
            _key = args[0].obj
            assert isinstance(_key, str)
            _value = args[1]
            self.redis[_key] = _value
            print(f"set {_key}: {_value} ")
            if(len(args)>3 and args[2].obj =="px"):
                _ps = int(args[3].obj)
                asyncio.create_task(self.delete_key(_key, _ps))
            return RedisObject(obj="OK", typ="str")
        except:
            return RedisObject.from_string("")
    
    def command_get(self, *args):
        try:
            _key = args[0].obj
            assert isinstance(_key, str)
            print(f"get {_key}")
            return self.redis[_key] 
        except:
            return RedisObject.from_string("")
    
    def command_config(self, *args): 
        try:
            assert(args[0].obj == 'get' or args[0].obj =='GET')
            _config_key =  args[1]
            if _config_key.obj == "dir":
                _config_val = RedisObject(obj=self.rdb_dir, typ="bulk_str") 
            elif _config_key.obj == "dbfilename":
                _config_val = RedisObject(obj=self.rdb_dbfilename, typ="bulk_str")
            _output = RedisObject(obj=[], typ="list")
            _output.obj.append(_config_key)
            _output.obj.append(_config_val)
            return _output
        except:
            return RedisObject.from_string("")
    
    def command_keys(self, *args):
        try:
            _output = RedisObject(obj=[])
            for _key in self.redis:
                _output.obj.append(RedisObject(obj=_key, typ="bulk_str"))
            if(len(_output.obj) ==0 ):
                return RedisObject(obj="",typ="null_bulk_str")
            return _output
        except:
            return RedisObject.from_string("")
    
    def command_info(self, *args):
        if args[0].obj != "replication":
            return RedisObject.from_string("")
        _info = f"role:{self.role}"
        return RedisObject(obj=_info, typ="bulk_str") 
    
    def command_replconf(self, *args):
        pass
        return RedisObject("OK")
