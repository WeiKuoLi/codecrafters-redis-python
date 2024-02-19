from app.src.redis_base import RedisServer
from app.src.redisdata import RedisObject
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
    
    async def hand_shake(self, reader, writer):
        try:
            print("open connection")
            # Open a connection to the server
            # reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        
            await self.ping_master(reader, writer)
            await self.replconf_master(reader, writer)
            await self.psync_master(reader, writer)
            # Close the connection
           # print('testing at redis_slave.py 30')
          #  for i in range(5):
           #     response = await reader.readline()
           #     if not response:
           #         break
           #     response_obj = RedisObject.from_string(response.decode('latin-1'))
                
           #     print("Response From Server to slave:", response_obj.__repr__())
           #     _message="+OK\r\n"
           #     writer.write(str(_message).encode())
           #     await writer.drain()
            
           # writer.close()
           # await writer.wait_closed()
        except Exception as e:
            print("Error:", e)

    async def ping_master(self, reader, writer):
            print("send ping")
            # Send a ping message
            writer.write(b"*1\r\n+ping\r\n")
            await writer.drain()
            print("wait response")
            # Read the response
            response = await reader.readline()
            response_obj = RedisObject.from_string(response.decode())
            print("Response From Server:", response_obj.__repr__())
            assert response_obj.obj == "PONG"
    
    async def replconf_master(self, reader, writer):
            print("send replconf to master")
            
            _message_1 = RedisObject([])
            _message_1.obj.append(RedisObject(obj="REPLCONF", typ="bulk_str"))
            _message_1.obj.append(RedisObject(obj="listening-port", typ="bulk_str"))
            _message_1.obj.append(RedisObject(obj=f"{self.port_number}", typ="bulk_str"))
            
            _message_2 = RedisObject([])
            _message_2.obj.append(RedisObject(obj="REPLCONF", typ="bulk_str"))
            _message_2.obj.append(RedisObject(obj="capa", typ="bulk_str"))
            _message_2.obj.append(RedisObject(obj="psync2", typ="bulk_str"))
            for _message in [_message_1, _message_2]:
                # Send a ping message
                writer.write(str(_message).encode())
                await writer.drain()
                print("wait response")
                # Read the response
                response = await reader.readline()
                response_obj = RedisObject.from_string(response.decode())
                print("Response From Server:", response_obj.__repr__())
                assert response_obj.obj == "OK"

    async def psync_master(self, reader, writer):
            print("send psync")
            
            _message = RedisObject([])
            _message.obj.append(RedisObject(obj="PSYNC", typ="bulk_str"))
            _message.obj.append(RedisObject(obj=f"{self.master_replid}", typ="bulk_str"))
            _message.obj.append(RedisObject(obj=f"{self.master_repl_offset}", typ="bulk_str"))
            
            # Send a ping message
            writer.write(str(_message).encode())
            await writer.drain()
            print("wait response")
            # Read the response
            response = await reader.readline()
            response_obj = RedisObject.from_string(response.decode())
            print("Response From Server:", response_obj.__repr__())
            ''' 
                # receive rdb
                response = None
                while (not response):
                    response = await reader.readline()
                response_obj = RedisObject.from_string(response.decode('latin-1'))
                #!!get RDB here 
                print(f"Receive rdb{len(response)} From Server:", response_obj.__repr__())
            '''
   
    def command_replconf(self, *args, **kwargs):
        if(args[0].obj == 'GETACK' or args[0].obj == 'getack'):
            _reply = RedisObject(obj=[], typ='lst')
            _reply.obj.append(RedisObject(obj="REPLCONF",typ="bulk_str"))
            _reply.obj.append(RedisObject(obj="ACK",typ="bulk_str"))
            _reply.obj.append(RedisObject(obj="0",typ="bulk_str"))
            return _reply
        return RedisObject(obj="", typ="null_bulk_str") 
