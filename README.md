Chain-PyMySQL
----

Easy to use PyMySQL.

对 PyMySQL 进行封装，增加链式操作，方便快捷进行 CURD 操作  
> 注：支持断线自动重连

<br>

文档目录
----

+ [一、安装说明（INSTALLATION）](#一安装说明installation)
+ [二、连接数据库（CONNECTION）](#二连接数据库connection)
    + [2.1 连接数据库](#21-连接数据库)
    + [2.2 切换数据库连接](#22-切换数据库连接)
    + [2.3 使用同连接的其他数据库](#23-使用同连接的其他数据库)
+ [三、增删改查（CURD）](#三增删改查curd)
    + [3.1 增](#31-增)
    + [3.2 删](#32-删)
    + [3.3 改](#33-改)
    + [3.4 查](#34-查)
        + [3.4.1 字符串条件](#341-字符串条件)
        + [3.4.2 字典条件查询](#342-字典条件查询)
        + [3.4.3 IN 查询](#343-in-查询)
        + [3.4.4 LIKE 模糊查询](#344-like-模糊查询)
        + [3.4.5 BETWEEN 查询](#345-between-查询)
        + [3.4.6 追加查询 and_where](#346-追加查询-and_where)
        + [3.4.7 OR 查询 or_where](#347-or-查询-or_where)
        + [3.4.8 其他操作符](#348-其他操作符)
+ [四、查询构建器（QUERY BUILDER）](#四查询构建器query builder)
    + [4.1 选择字段 select](#41-选择字段-select)
    + [4.2 联表查询 join](#42-联表查询-join)
    + [4.3 分组及排序 group_by order_by](#43-分组及排序-group_by-order_by)
    + [4.4 结果筛选 having](#44-结果筛选-having)
    + [4.5 分页查询 skip limit](#45-分页查询-skip-limit)
+ [五、执行原生SQL（RAW SQL）](#五执行原生sqlraw sql)
    + [5.1 执行原生SQL示例](#51-执行原生sql示例)
    + [5.2 使用助手函数来拼接SQL（防注入）](#52-使用助手函数来拼接sql防注入)
+ [六、返回值（RETURNED VALUE）](#六返回值returned value)
    + [6.1 统计 count](#61-统计-count)
    + [6.2 多行 all](#62-多行-all)
    + [6.3 单行 one](#63-单行-one)
    + [6.4 单个值 scalar](#64-单个值-scalar)
    + [6.5 一列 column](#65-一列-column)
    + [6.6 结果索引（一维）](#66-结果索引一维)
    + [6.7 结果索引（二维）](#67-结果索引二维)
    + [6.8 获取SQL](#68-获取sql)
    + [6.9 获取插入的ID](#69-获取插入的id)
    + [6.10 获取影响的行数](#610-获取影响的行数)
+ [七、事务支持（TRANSACTION）](#七事务支持transaction)
    + [7.1 上下文管理器](#71-上下文管理器)
    + [7.2 装饰器](#72-装饰器)
+ [八、单元测试（UNITTEST）](#八单元测试unittest)
+ [九、防注入（INJECTION）](#九防注入injection)
+ [十、内置异常（EXCEPTIONS）](#十内置异常exceptions)
+ [十一、请我喝奶茶（DONATION）](#十一请我喝奶茶donation)

<br>

一、安装说明（INSTALLATION）
----

使用 PIP 安装 或 直接下载源码

* 全自动安装：`easy_install chain-pymysql` 或者 `pip install chain-pymysql` / `pip3 install chain-pymysql`
* 半自动安装：先下载 https://pypi.org/project/chain-pymysql/#files ，解压后运行 `python setup.py install`
* 手动安装：将 chain-pymysql 目录放置于当前目录或者 site-packages 目录
* 通过 `from chain_pymysql import imysql` 来引用

<br>

二、连接数据库（CONNECTION）
----

#### 2.1 连接数据库

Method: `imysql.connect(options: dict, name='default')`

```python
from chain_pymysql import imysql

imysql.connect({
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'root',
    'database': 'test'
})
```
注: options 参数请参考 pymysql 官方文档: https://pymysql.readthedocs.io/en/latest/modules/connections.html

#### 2.2 切换数据库连接

支持连接多个数据库，使用 switch 方法来动态（或永久）切换数据库连接

Method: `imysql.switch(name: str, db_name=None, inplace=False)`

switch 方法使用示例

+ 临时切换数据库连接 `imysql.switch('db2')`
+ 永久切换数据库连接：`imysql.switch('db2', inplace=True)`
+ 切换连接的时候同时切换数据库：`imysql.switch('db2', 'test_member')` 或 `imysql.switch('db2.test_member')`

```python
from chain_pymysql import imysql

imysql.connect(default_config, name='default') # 第一个添加的数据库为默认数据库
imysql.connect(other_config, name='other')

# 使用默认数据库连接查询
imysql.table('table1').limit(10).all()
# 临时使用其他数据库连接来查询（只有本次查询是使用db2，其他查询还是db1）
imysql.switch('other').table('table1').limit(10).all()
# 永久切换数据库连接
imysql.switch('other', inplace=True).table('table1').limit(10).all()
# 这时再查询，默认就是 db2 数据库连接了
imysql.table('table1').limit(10).all() # 这里查询的是 db2 连接里的 table1
```


#### 2.3 使用同连接的其他数据库

Method: `table(table: str, alias='')`
> table 按 db.table 格式传参即可

```python
members = imysql.table('test_member.member').select('member_code').order_by('member_code desc').limit(10).column()
print(members)
orders = imysql.table('test_order.order').select('order_code, total_amount').where({
    'member_code': ('in', members, False)
}).order_by('order_code desc').all(fetch=True)
print(orders)
```

<br>

三、增删改查（CURD）
----

#### 3.1 增

```python
# 插入一行
insert_id = imysql.table('table1').insert_one({'id': 1, 'name': '张三'})

# 插入多行
effected_rows = imysql.table('table1').insert_many([
    {'id': 2, 'name': '李四'},
    {'id': 3, 'name': '王五'},
    {'id': 4, 'name': '赵六'}
])
```

程序默认会做数据验证，如果表单数据需要提交代码，则可以把 verify 设置为 False，取消数据验证

```python
insert_id = imysql.table('table1').insert_one({'name': '张三<script>alert(1)</script>'}, verify=False)
```

#### 3.2 删

```python
# 删除记录
effected_rows = imysql.table('table1').delete({'id': 1})

# 限制删除的行数
effected_rows = imysql.table('table1').delete('id>1', limit=1)
```

#### 3.3 改

```python
# 修改一行记录
effected_rows = imysql.table('table1').update_one({'id': 3}, {'name': '王六'})
# 修改多行记录
effected_rows = imysql.table('table1').update_many('id IN (3,4)', {'name': '匿名'})
effected_rows = imysql.table('table1').update_many({'id': ['IN', (3, 4)]}, {'name': '匿名'})
```

#### 3.4 查

注：fetch=True 返回 list，fetch=False（默认）返回 cursor，可用于迭代

##### 3.4.1 字符串条件
```python
results = imysql.table('table1').where('id IN (3,4)').all(fetch=True)
```

##### 3.4.2 字典条件查询
```python
results = imysql.table('table1').where({'id': 3}).all(fetch=True)
```

##### 3.4.3 IN 查询
```python
results = imysql.table('table1').where({'id': ['in', (3, 4)]}).all(fetch=True)
```

##### 3.4.4 LIKE 模糊查询
```python
results = imysql.table('table1').where({'name': ('like', '张%')}).all(fetch=True)
```

##### 3.4.5 BETWEEN 查询
```python
results = imysql.table('table1').where({'id': ['between', (3, 4)]}).all(fetch=True)
```

##### 3.4.6 追加查询 and_where
```python
# 特殊情况下使用 and_where
results = imysql.table('table1').where({'id': 3}).and_where({'name': '张三'}).all(fetch=True)

# 一般情况下，扩充字典条件即可
condition = {'id': 3}
if True:
    condition['name'] = '张三'

results = imysql.table('table1').where(condition).all(fetch=True)
```

##### 3.4.7 OR 查询 or_where
```python
results = imysql.table('table1').where({'id': 3}).or_where({'id': 4}).all(fetch=True)
```

##### 3.4.8 其他操作符
```python
# 大于等于
results = imysql.table('table1').where({'id': ['>=', 3]}).all(fetch=True)
# 小于
results = imysql.table('table1').where({'id': ['<', 4]}).all(fetch=True)
# 不等于
results = imysql.table('table1').where({'id': ['<>', 3]}).all(fetch=True)
# 不为空
results = imysql.table('table1').where({'id': ['<>', '']}).all(fetch=True)
# NULL
results = imysql.table('table1').where({'name': ['is', None]}).all(fetch=True)
# NOT NULL
results = imysql.table('table1').where({'name': ['is not', None]}).all(fetch=True)
# NOT IN
results = imysql.table('table1').where({'id': ['not in', (3, 4)]}).all(fetch=True)
# NOT LIKE
results = imysql.table('table1').where({'name': ['not like', '张%']}).all(fetch=True)
```

<br>

四、查询构建器（QUERY BUILDER）
----

##### 4.1 选择字段 select
```python
one = imysql.table('table1').select('id').where('id=3').one()
```

##### 4.2 联表查询 join
```python
# 默认 LEFT JOIN
results = (
    imysql.table('table1', alias='t1')
    .select('t1.id,t1.name,t2.age')
    .join('table2', alias='t2', on='t1.id=t2.id')
    .all(fetch=True)
)

# RIGHT JOIN
results = (
    imysql.table('table1 t1')
    .select(['t1.id', 't1.name', 't2.age'])
    .join('table2 t2', on='t1.id=t2.id', how='right')
    .all(fetch=True)
)

# INNER JOIN
results = (
    imysql.table('table1 t1')
    .join('table2 t2', on='t1.id=t2.id', how='inner')
    .all(fetch=True)
)

# ON 也可以使用字典，并且支持多条件
results = (
    imysql.table('table1 t1')
    .select('t1.id,t1.name,t2.age')
    .join('table2 t2', how='inner', on={
        # 第三个参数是不要使用引号的意思
        't1.id': ['=', 't2.id', False],
        't2.age': 20,
    })
    .all(fetch=True)
)
```

##### 4.3 分组及排序 group_by order_by
```python
results = (
    imysql.table('table2')
    .select('age, count(*) as num')

    # 可以使用字符串或list
    .group_by('age')
    # .group_by(['age'])

    # 可以使用字符串或list
    # .order_by('age asc, num desc')
    # .order_by(['age asc', 'num desc'])
    # .order_by(['age', 'num'], ascending=True)
    .order_by(['age', 'num'], ascending=[True, False])

    .all(fetch=True)
)
```

##### 4.4 结果筛选 having
```python
results = (
    imysql.table('table2')
    .select('age, count(*) as num')
    .group_by('age')
    .having('num > 1')
    .order_by('num desc')
    .all(fetch=True)
)
```

##### 4.5 分页查询 skip limit
`results = imysql.table('table1').order_by('id asc').skip(1).limit(3).all(fetch=True)`

<br>

五、执行原生SQL（RAW SQL）
----

> 注：imysql.execute 返回原生 pymysql.cursors.DictCursor 对象，后继操作须自己处理

##### 5.1 执行原生SQL示例

```python
sql = 'SELECT * FROM table1 WHERE id=%s'
# 解析多行
results = imysql.execute(sql, (3,)).fetchall()
# 解析单行
one = imysql.execute(sql, (3,)).fetchone()

# 如果sql类型为insert、replace、update或delete，需要自己提交事务
try:
    cursor = imysql.execute('DELETE FROM table1 WHERE name IN ("赵六", "陈七")')
    cursor.connection.commit()
except Exception as e:
    cursor.connection.rollback()

# 切换数据库来执行SQL
imysql.switch('other').execute(sql)
```

##### 5.2 使用助手函数来拼接SQL（防注入）
> gen_condition、gen_order_by、gen_limit 等

```python
condition = dict()
for k, v in request.GET.items():
    if k in ['id', 'name']:
        condition[f't1.{k}'] = v
    elif k in ['age']:
        condition[f't2.{k}'] = v

where = imysql.gen_condition(condition)
order_by = imysql.gen_order_by(request.GET.get('order'), request.GET.get('asc') == '1')
page = int(request.GET.get('page', 1))
size = int(request.GET.get('size', 10))
limit = imysql.gen_limit(skip=(page-1)*size, limit=size)

sql = f'SELECT t1.`name`, avg(t2.age) AS age FROM table1 t1 LEFT JOIN table2 t2 ON t1.id = t2.id WHERE {where} GROUP BY t1.`name` {order_by}{limit}'
results = imysql.execute(sql, fetch=True)
for item in results:
    print(item)
```

<br>

六、返回值（RETURNED VALUE）
----

##### 6.1 统计 count
```python
count = imysql.table('table1').count()
```

##### 6.2 多行 all
```python
results = imysql.table('table1').limit(10).all(fetch=True)
```

##### 6.3 单行 one
```python
one = imysql.table('table1').where('id=3').one()
```

##### 6.4 单个值 scalar
```python
name = imysql.table('table1').select('name').where('id=3').scalar()
```

##### 6.5 一列 column
```python
column = imysql.table('table1').select('name').limit(3).column()
```

##### 6.6 结果索引（一维）
```python
result = imysql.table('table1').limit(10).index(key='id', value='name')
```

##### 6.7 结果索引（二维）
```python
result = imysql.table('table1').limit(10).index(key='id')
```

##### 6.8 获取SQL
```python
# 方法一：执行前获取
sql = imysql.table('table1').select('id,name').limit(2).get_raw_sql()

# 方法二：执行后获取
sql = imysql.get_last_sql()
```
注：多线程高并发情况下，方法二可能不太准确

##### 6.9 获取插入的ID
```python
# 方法一：insert_one 方法返回值
id1 = imysql.table('table1').insert_one({'name': '周八'})
# 方法二：get_insert_id
id2 = imysql.get_insert_id()
```
注：多线程高并发情况下，方法二可能不太准确

##### 6.10 获取影响的行数
```python
# 方法一：函数返回值
result = imysql.table('table1').insert_many([
    {'name': '吴九'},
    {'name': '郑十'},
])
# 方法二：get_effected_rows
effected_rows = imysql.get_effected_rows()
```
注1：insert_many、update_one、update_many、delete 等函数返回的都是影响行数
注2：多线程高并发情况下，方法二可能不太准确

<br>

七、事务支持（TRANSACTION）
----

+ insert_one、insert_many、update_one、update_many、delete 等操作默认会自动提交事务
+ 如果希望多个操作完成之后再提交事务，上下文管理器 或 装饰器
+ **推荐使用上下文管理器（更灵活，可切换数据库连接），装饰器只才使用默认数据库连接**

<br>

##### 7.1 上下文管理器
```python
from chain_pymysql import imysql, transaction

with transaction.atomic():
    imysql.table('table1').delete({'name': '匿名'})
    # 下面这句会报错，事务回滚
    imysql.table('table1').insert_one({'name': '匿名', 'age': 18})
```

+ 捕获异常
    + 方法一：包裹整个上下文管理器，无需自己处理事务回滚
    ```python
    from chain_pymysql import imysql, transaction
    
    try:
        with transaction.atomic():
            imysql.table('table1').delete({'name': '匿名'})
            # 下面这句会报错，事务回滚
            imysql.table('table1').insert_one({'name': '匿名', 'age': 18})
    except Exception as e:
        print(e)
    ```

    + 方法二：在上下文内部捕获异常，自己处理事务回滚
    ```python
    from chain_pymysql import imysql, transaction
    
    with transaction.atomic() as atomic:
        imysql.table('table1').delete({'name': '匿名'})
        try:
            imysql.table('table1').insert_one({'name': '匿名', 'age': 18})
            # 不抛出异常的情况下，上下文管理器会自动提交事务，不用手动处理
            # atomic.commit()
        except Exception as e:
            # 捕获了异常，需要自己手动回滚事务
            atomic.rollback()
    ```

+ 切换数据库连接
```python
other = imysql.switch('other')
with transaction.atomic(other.conn) as atomic:
    other.table('table1').insert_one({'name': '李四'})
    other.table('table1').insert_one({'name': '王五'})
    atomic.rollback()
```

+ 多个数据库链接
>不同连接的事务不能放一个上下文管理器里，要分开写  

    ```python
    default = imysql.switch('default')
    with transaction.atomic(default.conn):
        default.table('table1').insert_one({'name': '吴九'})
        default.table('table1').insert_one({'name': '郑十'})
    
    other = imysql.switch('other')
    with transaction.atomic(other.conn):
        other.table('table1').insert_one({'name': '李四'})
        other.table('table1').insert_one({'name': '王五'})
    ```

##### 7.2 装饰器
```python
@transaction.atomic
def some_operation():
    imysql.table('table1').delete({'name': '匿名'})
    # 下面这句会报错，事务回滚
    imysql.table('table1').insert_one({'name': '匿名', 'age': 18})

some_operation()
```

+ 捕获异常  
    + 方法一：包裹被装饰的函数，无需自己处理事务回滚
    ```python
    @transaction.atomic
    def some_operation():
        imysql.table('table1').delete({'name': '匿名'})
        # 下面这句会报错，事务回滚
        imysql.table('table1').insert_one({'name': '匿名', 'age': 18})

    try:
        some_operation()
    except Exception as e:
        print(e)
    ```

    + 方法二：在被装饰的函数内捕获异常，自己处理事务回滚
    ```python
    @transaction.atomic
    def some_operation():
        imysql.table('table1').delete({'name': '匿名'})
        try:
            imysql.table('table1').insert_one({'name': '匿名', 'age': 18})
            # 不抛出异常的情况下，装饰器会自动提交事务，不用手动处理
            # imysql.default_conn.commit()
        except Exception as e:
            # 捕获了异常，需要自己手动回滚事务
            imysql.default_conn.rollback()

    some_operation()
    ```

+ 切换数据库连接
> **使用装饰器无法灵活切换数据库连接，如需切换数据库连接，需修改全局默认连接**  

    ```python
    # 切换默认数据库连接为 other
    imysql.switch('other', inplace=True)
    
    @transaction.atomic
    def some_operation():
        # 这里使用的是 other 连接里默认数据库的 table1 表
        imysql.table('table1').delete({'name': '匿名'})
    
    some_operation()
    ```

<br>

八、单元测试（UNITTEST）
----

详情请看 test.py 文件
> https://github.com/Tiacx/chain-pymysql/test.py

<br>

九、防注入（INJECTION）
----

经过 sqlmap 测试，安全防注入
> 注1：不保证所有情况下都防注入，建议接收用户提交的数据时做数据类型及格式的验证  
> 注2：如需自己拼接sql，建议使用助手函数，详情请看“5.2 使用助手函数来拼接SQL（防注入）”  

<br>

十、内置异常（EXCEPTIONS）
----

所有内置都提供两个一样的属性及方法
> 即：code、message，get_code(), get_message()

<br>

错误代码：

| 错误码 | 说明 |
|  ----  | ---- |
| 400 | 参数错误 |
| 403 | 存在SQL注入 |

<br>

> 运行时异常：RuntimeError

```python
import chain_pymysql.exceptions

try:
    imysql.table('table1').skip(0).limit(-1).all()
except chain_pymysql.exceptions.RuntimeError as e:
    assert e.code == 400
    print(e.message)

try:
    imysql.table('table1').where({
        'id': ['=', "3' UNION ALL SELECT NULL,NULL,NULL,NULL,NULL,NULL-- zxmL"]
    }).all(fetch=True)
except chain_pymysql.exceptions.RuntimeError as e:
    assert e.get_code() == 403
    print(e.get_message())
```

<br>

十一、请我喝奶茶（DONATION）
----

> 如果你觉得这个工具对你有帮助或启发，也可以请我喝☕️

![支付宝](alipay.jpg)
