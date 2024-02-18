import uuid
from  app.src.redisdata import RedisObject
from  app.src.redisio import RedisIOHandler
from  app.src.redis_master import RedisServerMaster
from  app.src.redis_slave import RedisServerSlave
from app.src.buffer import BufferQueue, BufferMultiQueue
a = RedisObject(obj=[], typ="list")
b = RedisObject(obj=[], typ="list")
a1 = RedisObject(obj='SET', typ='bulk_str')
a2 = RedisObject(obj='key10293', typ='bulk_str')
a3 = RedisObject(obj='valuevalue', typ='bulk_str')
a.obj.append(a1)
a.obj.append(a2)
a.obj.append(a3)
print(str(a))

rds_svr = RedisServerMaster()
r = RedisIOHandler(redis_server=rds_svr)
assert r.redis_server==rds_svr
assert rds_svr.redis_io_handler==r

assert str(a)=="*3\r\n$3\r\nSET\r\n$8\r\nkey10293\r\n$10\r\nvaluevalue\r\n"
assert r.parse_input(str(a)) == a
assert r.parse_output(a) == str(a)
client_id = str(uuid.uuid4()) 

r.session[client_id] = { "reader":'reader',
                                     "writer": 'writer',
                                     "client_ip":None,
                                     "client_port":"6380"}
r.buffer["6380"]=BufferQueue()

assert r.execute_command(input_redisobject=a) == r.parse_input("+OK\r\n")
assert rds_svr.redis['key10293'].obj == 'valuevalue'
print('bufferQ  ', (r.buffer["6380"]).__repr__())
print('bufferQ peek  ', (r.buffer["6380"].peek()).__repr__())
print('bufferQ size  ', (r.buffer["6380"].size()))

x = r.buffer["6380"].dequeue()
assert r.execute_command(input_redisobject=x) == r.parse_input("+OK\r\n")

rds_svr  = RedisServerSlave(port_number=6380, 
                            master_host="localhost", 
                            master_port=6379)
r = RedisIOHandler(redis_server=rds_svr)
assert r.redis_server==rds_svr
assert rds_svr.redis_io_handler==r

assert str(a)=="*3\r\n$3\r\nSET\r\n$8\r\nkey10293\r\n$10\r\nvaluevalue\r\n"
assert r.parse_input(str(a)) == a
assert r.parse_output(a) == str(a)
assert r.execute_command(input_redisobject=a) == r.parse_input("+OK\r\n")
assert rds_svr.redis['key10293'].obj == 'valuevalue'

buffer = BufferMultiQueue()
buffer[6379]=BufferQueue()
buffer[6399]=BufferQueue()
for k, v in buffer.items():
    buffer[k].enqueue(a)
    v.dequeue()
    buffer[k].enqueue(a)
    v.enqueue(a)

assert buffer[6399].peek() == a
assert str(buffer[6379].peek()) == str(a)
for k in buffer:
    assert buffer[k].size()==2


print( str(buffer))
