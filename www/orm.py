# -*- coding: utf-8 -*-
import asyncio,logging

import aiomysql

__pool={}

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

@asyncio.coroutine
def create_pool(loop,**kw):
    ' 创建全局连接池，**kw 关键字参数集，用于传递host port user password db等的数据库连接参数 '
    logging.info('create database connection pool...')
    global __pool
    __pool=yield from aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['database'],
        charset=kw.get('charset','utf8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop#需要传递一个事件循环实例，若无特别声明，默认使用asyncio.get_event_loop()
    )

@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:#从连接池中获取一个连接，使用完后自动释放
        cur=yield from conn.corsor(aiomysql.DictCursor)
        # 执行SQL，mysql的占位符是%s，和python一样，为了coding的便利，先用SQL的占位符？写SQL语句，最后执行时在转换过来
        yield from cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs=yield from cur.fetchmany(size) #只读取size条记录
        else:
            rs=yield from cur.fetchall() #返回的rs是一个list，每个元素是一个dict，一个dict代表一行记录
        yield from cur.close()
        logging.info('rows returned:%s' % len(rs))
        return rs
@asyncio.coroutine
def execute(sql,args):
    ' 实现SQL语句：INSERT、UPDATE、DELETE。传入参数分别为SQL语句、SQL语句中占位符对应的参数集、默认打开MySQL的自动提交事务 '
    log(sql)
    with(yield from __pool) as conn:
        try:
            cur=yield from conn.cursor()
            yield from cur.execute(sql.replace('?','%s'),args)
            affected=cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    ' 按参数个数制作占位符字符串，用于生成SQL语句 '
    L = []
    for n in range(num): #SQL的占位符是？，num是多少就插入多少个占位符
        L.append('?')
    return ', '.join(L) #将L拼接成字符串返回，例如num=3时："?, ?, ?"



class Field(object):
    ' 定义一个数据类型的基类，用于衍生 各种在ORM中 对应 数据库的数据类型 的类 '
    def __init__(self, name, column_type, primary_key, default):
        ' 传入参数对应列名、数据类型、主键、默认值 '
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        ' print(Field_object)时，返回类名Field，数据类型，列名 '
        return '<%s,%s:%s>' % (self.__class__.__name__,self.column_type,self.name)

class StringField(Field):

    def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
        super().__init__(name,ddl,primary_key,default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        #排除Model类本身
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        #获取table名称：
        tableName=attrs.get('__table__',None) or name  #获取表名，默认为None，或为类名
        #获取所有的Field和主键名：
        mappings = dict()
        fields=[]
        primaryKey=None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping:%s ==> %s' % (k, v))
                mappings[k] = v#存储列名和数据类型
                if v.primary_key:
                    #找到主键：
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k )
                    primaryKey = k
                else:
                    fields.append(k) #存储非主键的列名
        if not primaryKey: #整个表不存在主键时抛出异常
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys(): #过滤掉列，只剩方法
            attrs.pop(k)
        escaped_fields=list(map(lambda f: '`%s`'% f,fields)) #给非主键列加``（可执行命令）区别于''（字符串效果）
        attrs['__mappings__'] = mappings #保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey#主键属性名
        attrs['__fields__'] = fields #除主键外的属性名
        #构造默认的SELECT,INSERT,UPDATE和DELETE语句：
        attrs['__select__']='select `%s` ,%s from `%s` ' % (primaryKey,','.join(escaped_fields),tableName)
        attrs['__insert__']='insert into `%s` (%s,`%s`) values(%s)' %(tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
        attrs['__update__']='update `%s` set %s where `%s` = ? ' %(tableName, ','.join(map(lambda f:'`%s`= ?' % (mappings.get(f).name or f),fields)),primaryKey)
        attrs['__delete__']='delete from `%s` where `%s` = ?' % (tableName,primaryKey)
        return type.__new__(cls,name,bases,attrs)

class Model(dict, metaclass=ModelMetaclass):
    ' 定义一个对应 数据库数据类型 的模板类。通过继承，获得dict的特性和元类的类与数据库的映射关系 '
    # 由模板类衍生其他类时，这个模板类没重新定义__new__()方法，因此会使用父类ModelMetaclass的__new__()来生成衍生类，从而实现ORM
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
    ' __getattr__、__settattr__实现属性动态绑定和获取 '
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        ' 返回属性值，空则返回默认值 '
        value=getattr(self,key,None)
        if value is None:
            field=self.__mappings__[key]#查取属性对应的列的数量类型默认值
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod #添加类方法，对应查表，默认查整个表，可通过where limit设置查找条件
    @asyncio.coroutine
    def find(cls,pk):
        'find object by primary key.'
        rs=yield  from select('%s where `%s`= ?' % (cls.__select__,cls.__primary_key__),[pk],1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @asyncio.coroutine
    def save(self):
        ' 实例方法，映射插入记录 '
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__,args)
        if rows !=1:
            logging.warn('failed to insert record: affected rows : %s' % rows)