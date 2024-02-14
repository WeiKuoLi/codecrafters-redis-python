from .redis import RedisServer
from .redisdata import RedisObject
import asyncio
class RedisServerSlave(RedisServer):
    '''
    Slave Redis server
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.role="slave"
        self.master_host = kwargs['master_host']
        self.master_port = kwargs['master_port']

        assert self.master_host is not None
        assert self.master_port is not None

        self.master_replid = "?"
        self.master_repl_offset = -1
    async def hand_shake(self):
        try:
            print("open connection")
            # Open a connection to the server
            reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        
            await self.ping_master(reader, writer)
            await self.replconf_master(reader, writer)
            await self.psync_master(reader, writer)
            # Close the connection
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print("Error:", e)

    async def ping_master(self, reader, writer):
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
    
    async def replconf_master(self, reader, writer):
            print("send replconf")
            
            _message_1 = RedisObject([])
            _message_1.obj.append(RedisObject.from_string("REPLCONF"))
            _message_1.obj.append(RedisObject.from_string("listening-port"))
            _message_1.obj.append(RedisObject.from_string(f"{self.port_number}"))
            
            _message_2 = RedisObject([])
            _message_2.obj.append(RedisObject.from_string("REPLCONF"))
            _message_2.obj.append(RedisObject.from_string("capa"))
            _message_2.obj.append(RedisObject.from_string("psync2"))
            for _message in [_message_1, _message_2]:
                # Send a ping message
                writer.write(str(_message).encode())
                await writer.drain()
                print("wait response")
                # Read the response
                response = await reader.readline()
                response_obj = RedisObject.from_string(response.decode())
                print("Response From Server:", response_obj)
                assert response_obj.obj == "OK"

    async def psync_master(self, reader, writer):
            print("send psync")
            
            _message = RedisObject([])
            _message.obj.append(RedisObject.from_string("PSYNC"))
            _message.obj.append(RedisObject.from_string(f"{self.master_replid}"))
            _message.obj.append(RedisObject.from_string(f"{self.master_repl_offset}"))
            
            # Send a ping message
            writer.write(str(_message).encode())
            await writer.drain()
            print("wait response")
            # Read the response
            response = await reader.readline()
            response_obj = RedisObject.from_string(response.decode())
            print("Response From Server:", response_obj)

