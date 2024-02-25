import unittest
import uuid
from app.src.redisdata import RedisObject
from app.src.redisio import RedisIOHandler
from app.src.redis_master import RedisServerMaster
from app.src.redis_slave import RedisServerSlave
from app.src.buffer import BufferQueue, BufferMultiQueue

class TestRedisIOHandler(unittest.TestCase):
    def setUp(self):
        self.a = RedisObject(obj=[], typ="list")
        self.b = RedisObject(obj=[], typ="list")
        self.a1 = RedisObject(obj='SET', typ='bulk_str')
        self.a2 = RedisObject(obj='key10293', typ='bulk_str')
        self.a3 = RedisObject(obj='valuevalue', typ='bulk_str')
        self.a.obj.append(self.a1)
        self.a.obj.append(self.a2)
        self.a.obj.append(self.a3)
        self.rds_svr = RedisServerMaster()
        self.r = RedisIOHandler(redis_server=self.rds_svr)
        self.client_id = str(uuid.uuid4())
        self.r.session[self.client_id] = {
            "reader": 'reader',
            "writer": 'writer',
            "client_ip": None,
            "client_port": "6380"
        }
        self.r.buffer["6380"] = BufferQueue()
        self.buffer = BufferMultiQueue()
        self.buffer[6379] = BufferQueue()
        self.buffer[6399] = BufferQueue()

    def test_redis_server_master(self):
        self.assertEqual(self.r.redis_server, self.rds_svr)
        self.assertEqual(self.rds_svr.redis_io_handler, self.r)

    def test_redis_object_str(self):
        expected_str = "*3\r\n$3\r\nSET\r\n$8\r\nkey10293\r\n$10\r\nvaluevalue\r\n"
        self.assertEqual(str(self.a), expected_str)

    def test_parse_input_output(self):
        self.assertEqual(self.r.parse_input(str(self.a)), self.a)
        self.assertEqual(self.r.parse_output(self.a), str(self.a))

    def test_execute_command(self):
        expected_response = self.r.parse_input("+OK\r\n")
        self.assertEqual(self.r.execute_command(input_redisobject=self.a), expected_response)
        self.assertEqual(self.rds_svr.redis['key10293'].obj, 'valuevalue')

    def test_buffer_operations(self):
        self.assertEqual((self.r.buffer["6380"]).size(), 0)
        self.r.buffer["6380"].enqueue(self.a)
        self.assertEqual((self.r.buffer["6380"].size()), 1)
        dequeued_item = self.r.buffer["6380"].dequeue()
        self.assertEqual(dequeued_item, self.a)

    def test_redis_server_slave(self):
        rds_svr_slave = RedisServerSlave(port_number=6380, master_host="localhost", master_port=6379)
        r_slave = RedisIOHandler(redis_server=rds_svr_slave)
        self.assertEqual(r_slave.redis_server, rds_svr_slave)
        self.assertEqual(rds_svr_slave.redis_io_handler, r_slave)

    def test_buffer_multi_queue(self):
        for k, v in self.buffer.items():
            self.buffer[k].enqueue(self.a)
            self.assertEqual(v.size(), 1)
            v.dequeue()
            self.assertEqual(v.size(), 0)
            self.buffer[k].enqueue(self.a)
            v.enqueue(self.a)
        self.assertEqual(self.buffer[6399].peek(), self.a)
        self.assertEqual(str(self.buffer[6379].peek()), str(self.a))
        for k in self.buffer:
            self.assertEqual((self.buffer[k]).size(), 2)

if __name__ == '__main__':
    unittest.main()

