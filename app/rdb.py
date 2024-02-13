import os
import asyncio
from .src.redis import RedisObject
def read_bits(file_path, num_bits=1):
    with open(file_path, 'rb') as file:
        byte = file.read(1)
        while byte:
            for i in range(8):
                yield (byte[0] >> (7 - i)) & 1
            byte = file.read(1)

def bits_to_bytes(bits):
    # Pad the bits to ensure the total length is a multiple of 8
    bits = [0] * ((8 - len(bits) % 8) % 8) + bits

    # Group bits into bytes (8 bits per byte)
    bytes_list = [bits[i:i+8] for i in range(0, len(bits), 8)]

    # Convert each byte to an integer and then to a byte
    byte_array = bytearray([int(''.join(map(str, byte)), 2) for byte in bytes_list])

    return byte_array


def check_header(bit_iterator):
    #check REDIS header
    _bits = [next(bit_iterator) for _ in range(8*5)]
    _bytes = bits_to_bytes(_bits)
    assert _bytes =="REDIS".encode('latin-1')
    for _ in range(8*4):
        next(bit_iterator)
    return True

def read_auxilary_field(bit_iterator):
    '''
    _bits = [next(bit_iterator) for _ in range(8*1)]
    _bytes = bits_to_bytes(_bits)
    assert _bytes.hex() =='fa'
    '''
    _meta_data_key = read_string_encoding(bit_iterator)
    _meta_data_value = read_string_encoding(bit_iterator)
    return _meta_data_key, _meta_data_value

def read_resizedb_field(bit_iterator):

    _hash_table_size = read_length_encoding(bit_iterator)
    _expire_hash_table_size = read_length_encoding(bit_iterator)
    return _hash_table_size, _expire_hash_table_size

def read_value_type(bit_iterator):
    _bits = [next(bit_iterator) for _ in range(8*1)]
    _bytes = bits_to_bytes(_bits)
    _int = int.from_bytes(_bytes, byteorder='little')
    if(_int !=0):
        raise ValueError("only string encoding supported")
def read_unsigned_int(bit_iterator):
    _bits = [next(bit_iterator) for _ in range(8*4)]
    _bytes = bits_to_bytes(_bits)
    _int = int.from_bytes(_bytes, byteorder='little')
    return _int

def read_unsigned_long(bit_iterator):
    _bits = [next(bit_iterator) for _ in range(8*8)]
    _bytes = bits_to_bytes(_bits)
    _int = int.from_bytes(_bytes, byteorder='little')
    return _int

def read_block_type(bit_iterator):
    _bits = [next(bit_iterator) for _ in range(8*1)]
    _bytes = bits_to_bytes(_bits)
    op_codes = ['ff', 'fe', 'fd', 'fc', 'fb', 'fa']
    if _bytes.hex() in op_codes:
        return _bytes.hex(), -1
    else:
        _val_typ = int.from_bytes(_bytes, byteorder='little')
        # 'we' stands for without expire
        return  'we', _val_typ

def read_block(bit_iterator):
    _block_typ, _val_typ = read_block_type(bit_iterator)
    _expire_time = -1
    _data = None
    if(_block_typ == 'fa'):
        _key, _value = read_auxilary_field(bit_iterator)
        _data = (_block_typ, _expire_time, _val_typ, _key, _value)  
    elif(_block_typ == 'fe'):
        _db_number = read_length_encoding(bit_iterator)
        _data = (_block_typ, _db_number)
    elif(_block_typ == 'fb'):
        _hash_table_size, _expire_hash_table_size = read_resizedb_field(bit_iterator)
        _data = (_block_typ, _hash_table_size, _expire_hash_table_size )
    elif(_block_typ == 'ff'):
        _check_sum = None # not implemented
        _data = (_block_typ, _check_sum)
    elif(_block_typ == 'fd'):
        _expire_time = read_unsigned_int(bit_iterator) 
        _val_typ = read_value_type(bit_iterator)
        assert _val_typ == 0
        _key = read_string_encoding(bit_iterator)
        _value = read_string_encoding(bit_iterator)
        _data = (_block_typ, _expire_time, _val_typ, _key, _value)
    elif(_block_typ == 'fc'):
        _expire_time = read_unsigned_long(bit_iterator)
        _val_typ = read_value_type(bit_iterator)
        assert _val_typ == 0
        _key = read_string_encoding(bit_iterator)
        _value = read_string_encoding(bit_iterator)
        _data = (_block_typ, _expire_time, _val_typ, _key, _value)
    else:
        assert _block_typ == 'we'
        assert _val_typ == 0
        _key = read_string_encoding(bit_iterator)
        _value = read_string_encoding(bit_iterator)
        _data = (_block_typ, _expire_time, _val_typ, _key, _value)
    return _data

'''
def read_key_value_block(bit_iterator):
    _block_typ, _val_typ = read_key_value_type(bit_iterator)
    _expire_time = -1
    if(_block_typ == 'fd'):
        _expire_time = read_unsigned_int(bit_iterator) 
        _val_typ = read_value_type(bit_iterator)
        assert _val_typ == 0
        _key = read_string_encoding(bit_iterator)
        _value = read_string_encoding(bit_iterator)
    elif(_block_typ == 'fc'):
        _expire_time = read_unsigned_long(bit_iterator)
        _val_typ = read_value_type(bit_iterator)
        assert _val_typ == 0
        _key = read_string_encoding(bit_iterator)
        _value = read_string_encoding(bit_iterator)
    else:
        assert _block_typ == 'we'
        assert _val_typ == 0
        _key = read_string_encoding(bit_iterator)
        _value = read_string_encoding(bit_iterator)
    return _block_typ, _expire_time, _val_typ, _key, _value
'''



def read_length_encoding(bit_iterator):
    _bits = [next(bit_iterator) for _ in range(2)]
    if (_bits == [0, 0]):
        # 6 bits length
        _bits = [next(bit_iterator) for _ in range(6)]
        _bytes = bits_to_bytes(_bits)
        _int = int.from_bytes(_bytes, byteorder='little')
        return _int

    elif(_bits == [0, 1]):
        # 14 bits length
        _bits = [next(bit_iterator) for _ in range(14)]
        _bytes = bits_to_bytes(_bits)
        _int = int.from_bytes(_bytes, byteorder='little')
        return _int

    elif(_bits == [1, 0]):
        # 6 bits discard 4 bytes length
        _discard = [next(bit_iterator) for _ in range(6)]
        _bits = [next(bit_iterator) for _ in range(8 * 4)]
        _bytes = bits_to_bytes(_bits)
        _int = int.from_bytes(_bytes, byteorder='little')
        return _int

    elif(_bits == [1, 1]):
        print("length encoding 11 mode not supported, may have unexpected behavior")
        _bits = [next(bit_iterator) for _ in range(6)]
        _bytes = bits_to_bytes(_bits)
        _format = int.from_bytes(_bytes, byteorder='little')
        if(_format<3):
            _map = {0:1, 1:2, 2:4} # _format -> x byte integer
            return _map[_format]
        else:
            raise ValueError("LZF compressed string not supported")
        return None

def read_string_encoding(bit_iterator):
    #support length prefixed string only
    _len = read_length_encoding(bit_iterator)
    _bits = [next(bit_iterator) for _ in range(8*_len)]
    _bytes = bits_to_bytes(_bits)
    return _bytes.decode('latin-1')

def read_rdb_file(file_path='dump0.rdb'):
    # create bit iterator for RDB file
    bit_iterator = read_bits(file_path)

    check_header(bit_iterator)
    while True:

        block_typ, block_data = "", [] 
        block_typ, *block_data = read_block(bit_iterator) 
        if(block_typ == 'ff'):
            break
        print(f"{block_typ} block,{block_data}\n\n")
        #block_typ, expire_time, val_typ, key, value =


    print("End of file reached")

async def import_rdb_file(redis_handler, file_path=None):
    try:
        if(file_path is None):
           file_path = os.path.join(redis_handler.rdb_dir, redis_handler.rdb_dbfilename) 
        bit_iterator = read_bits(file_path)

        check_header(bit_iterator)
        while True:

            block_typ, block_data = "", [] 
            block_typ, *block_data = read_block(bit_iterator) 
            if(block_typ == 'ff'):
                break
            elif(block_typ == 'we'):
                _expire_time, _val_typ, _key, _value = block_data
                assert _val_typ == 0
                redis_handler.redis[str(_key)] = RedisObject(str(_value))
            elif(block_typ == 'fc'):
                _expire_time, _val_typ, _key, _value = block_data
                #assert _val_typ == 0
                redis_handler.redis[str(_key)] = RedisObject(str(_value))
                asyncio.create_task(self.delete_key(str(_key), _expire_time))
            elif(block_typ == 'fd'):
                _expire_time, _val_typ, _key, _value = block_data
                #assert _val_typ == 0
                redis_handler.redis[RedisObject(str(_key))] = RedisObject(str(_value))
                asyncio.create_task(self.delete_key(str(_key), _expire_time * 1000))
                
            #print(f"{block_typ} block,{block_data}\n\n")
            #block_typ, expire_time, val_typ, key, value =
    except:
        print("cannot find RDB file, proceed as if non exist")
    print("End loading phase")
if __name__ == "__main__":
    read_rdb_file('rdb_data/dump2.rdb')
