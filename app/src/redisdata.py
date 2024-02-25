class RedisObject:
    '''
    The primary data structure in this project, 
    for example:
        input commands/output messages/ keys/ values 
    are all RedisObjects  
    '''
    def __init__(self, obj, typ=None):
        self.obj = obj
        if (typ is None):
            if isinstance(obj, int):
                self.typ = "int"
            elif isinstance(obj, str):
                self.typ = "str"
            elif isinstance(obj, list):
                self.typ = "list"
            elif isinstance(obj, RedisObject):
                self.typ = "RedisObject"
        else:
            assert typ in ["int", "str", "bulk_str", "null_bulk_str", "list", "RedisObject"]
            self.typ = typ

    def __repr__(self):
        return (f"< Object {self.obj}, Type: {self.typ} >")
    
    def __str__(self):
        '''
        return its resp string
        '''
        if self.typ =="bulk_str":
            return f"${len(self.obj)}\r\n{self.obj}\r\n"
        elif self.typ == "str":
            return f"+{self.obj}\r\n"
        elif self.typ == "list":
            _len = len(self.obj)
            _str = f"*{_len}\r\n"
            for node_obj in self.obj:
                _str += str(node_obj)
            return _str
        elif self.typ == "null_bulk_str":
            return "$-1\r\n"
        return "$-1\r\n"
    
    def __hash__(self):
        return hash((self.typ, self.obj))

    def __eq__(self, other):
        return isinstance(other, RedisObject) and self.obj == other.obj and self.typ == other.typ
    
    def copy(self):
        return RedisObject(obj=self.obj, typ=self.typ)
    
    @classmethod
    def from_string(cls, string):
        '''
        parse the string and return an instance 
        '''
        return cls.recursive_parse_string(string)[0]
        
   
    @classmethod
    def simple_parse_string(cls, string):
        resp_list = []
        _len = len(string)
        _i, _j = 0, 0
        _arr_num = 0
        _num = 0
        if ((_len == 0) or string == "\r\n"):
            return cls(obj="", typ="null_bulk_str")

        while(_i < _len and _j < _len):
            if(string[_i] == '*'):
                # array
                _j = _i + 1
                while(string[_j] != '\n'):
                    _j = _j + 1
                _arr_num = int(string[_i+1:_j-1])
                resp_list.append(cls(obj=[], typ='list'))
                #print(f"an array of{_arr_num}")
                _j = _j + 1
                _i = _j
            elif (string[_i] == '+'):
                # simple string
                _j = _i + 1
                while(string[_j] != '\n'):
                    _j = _j + 1
                _obj =cls(obj=string[_i+1:_j-1],typ='str')
                #print(f"a string = {_obj}")
                if(_arr_num == 0):
                    resp_list.append(cls(obj=[_obj],typ='list'))
                else:
                    resp_list[-1].obj.append(_obj)
                    _arr_num -= 1
                _j = _j + 1
                _i = _j
            elif (string[_i] == '$'):
                # bulk string
                _j = _i + 1
                while(string[_j] != '\n'):
                    _j = _j + 1
                _num = int(string[_i+1:_j-1])
                _j = _j + 1
                _i = _j
                
                _obj=cls(obj=string[_i:_i+_num],typ='bulk_str')
                #print(f"a bulk_str={_obj}")
                if(_arr_num == 0):
                    resp_list.append(cls(obj=[_obj],typ='list'))
                else:
                    resp_list[-1].obj.append(_obj)
                    _arr_num -= 1

                _i = _i + _num + 2
                _j = _i
            else:
                _i = _i + 1
                _j = _i
        
        return cls(obj=resp_list,typ='list')                                 


    
    @classmethod
    def recursive_parse_string(cls, string):
        '''
        returns an instance of the first object and its remaining unparsed string
        '''
        if ((len(string)==0 ) or string == "\r\n"):
            return cls(obj="", typ="null_bulk_str"), ""

        head = string.split("\r\n")[0]
        assert len(string.split("\r\n")) > 0

        if (head[0] == '+'):
            # simple string
            _str = head[1:]
            _string_remove_head = string[len(head) + 2:]
            return cls(obj=_str, typ="str"), _string_remove_head
        elif (head[0] == '$'):
            # bulk string
            _str_len = int(head[1:])
            _string_remove_head = string[len(head) + 2:]
            _str = _string_remove_head[:_str_len]
            return cls(obj=_str, typ="bulk_str" ), _string_remove_head[_str_len + 2 :]
        elif (head[0] == '*'):
            _lst = []
            _arr_len = int(head[1:])
            _string_remove_head = string[len(head) + 2:]
            #_string_remove_head = "\r\n".join(_string.split("\r\n")[1:])+ "\r\n"
            for i in range(_arr_len):
                _node, _str = cls.recursive_parse_string(_string_remove_head)
                _lst.append(_node)
                _string_remove_head = _str
            return cls(obj=_lst, typ="list"), _string_remove_head
        return cls(obj="", typ="null_bulk_str"), ""

