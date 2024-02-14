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
    
    @class_method
    def from_string(cls, string):
        '''
        parse the string and return an instance 
        '''
        _obj, _ = cls.recursive_parse_string(string)
        return _obj

    @class_method
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
            _string_remove_head = "\r\n".join(string.split("\r\n")[1:]) + "\r\n"
            return cls(obj=_str, typ="str"), _string_remove_head
        elif (head[0] == '$'):
            # bulk string
            _str_len = int(head[1:])
            _string_remove_head = string[len(head) + 2:]
            _str = _string_remove_head[:_str_len]
            return RedisObject(obj=_str, typ="bulk_str" ), _string_remove_head[_str_len + 2 :]
        elif (head[0] == '*'):
            _lst = []
            _arr_len = int(head[1:])
            _string_remove_head = string[len(head) + 2:]
            #_string_remove_head = "\r\n".join(_string.split("\r\n")[1:])+ "\r\n"
            for i in range(_arr_len):
                _node, _str = cls.recursive_parse_string(_string_remove_head)
                _lst.append(_node)
                _string_remove_head = _str
            return RedisObject(obj=_lst, typ="list"), _string_remove_head
        return cls(obj="", typ="null_bulk_str"), ""

