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
        self.master_host = kwargs['master_host']
        self.master_port = kwargs['master_port']
        assert self.master_host is not None
        assert self.master_port is not None

    async def hand_shake(self):
        await self.ping_server()

    async def ping_server(self):
        try:
            print("open connection")
            # Open a connection to the server
            reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
            print("send ping")
            # Send a ping message
            writer.write(b"+ping\r\n")
            await writer.drain()
            print("wait response")
            # Read the response
            response = await reader.readline()
            response_obj = RedisObject.from_string(response.decode())
            print("Response From Server:", response_obj)
            assert response_obj.obj == "PONG"
        
            # Close the connection
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print("Error:", e)
