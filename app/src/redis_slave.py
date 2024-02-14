from .redis import RedisServer
from .redisdata import RedisObject
import asyncio
class RedisServerSlave(RedisServer):
    '''
    Base class for my Redis server
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.role="slave"
