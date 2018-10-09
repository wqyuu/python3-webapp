import asyncio
import www.orm as orm
from www.models import User,Blog,Comment

def test(loop):
    #创建连接池
    yield from orm.create_pool(loop,user='root',password='3306',database='awesome')
    #创建对象
    u=User(name='Test231',email='test123@wxample',passwd='123456',image='about:blank')
    #调用保存方法
    yield from u.save()

#for x in test():
#    pass
loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close()