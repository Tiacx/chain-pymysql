import unittest
from chain_pymysql import imysql, transaction


class TestCase(unittest.TestCase):

    def test_0_0(self):
        # 创建一个默认连接
        imysql.connect({
            'host': '127.0.0.1',
            'user': 'root',
            'password': 'root',
            'database': 'test'
        })

        ''' 清空数据 '''
        imysql.execute('TRUNCATE TABLE table1')
        imysql.execute('TRUNCATE TABLE table2')

    def test_0_1(self):
        ''' 插入一行记录 '''
        
        insert_id = imysql.table('table1').insert_one({'id': 1, 'name': '张三'})
        self.assertEqual(insert_id, 1)

    def test_0_2(self):
        ''' 插入多行记录 '''
        effected_rows = imysql.table('table1').insert_many([
            {'id': 2, 'name': '李四'},
            {'id': 3, 'name': '王五'},
            {'id': 4, 'name': '赵六'}
        ])
        self.assertEqual(effected_rows, 3)

    def test_0_3(self):
        ''' 删除记录 '''
        effected_rows = imysql.table('table1').delete({'id': 1})
        self.assertEqual(effected_rows, 1)

        # 限制删除的行数
        effected_rows = imysql.table('table1').delete('id>1', limit=1)
        self.assertEqual(effected_rows, 1)

    def test_0_4(self):
        ''' 修改一行记录 '''
        effected_rows = imysql.table('table1').update_one({'id': 3}, {'name': '王六'})
        self.assertEqual(effected_rows, 1)

    def test_0_5(self):
        ''' 修改多行记录 '''
        effected_rows = imysql.table('table1').update_many('id IN (3,4)', {'name': '匿名'})
        self.assertEqual(effected_rows, 2)
        effected_rows = imysql.table('table1').update_many('id=3', {'name': '张三'})
        self.assertEqual(effected_rows, 1)

    def test_0_6(self):
        ''' 字符串条件查询 '''

        # 注：fetch=True 返回 list，fetch=False（默认）返回 cursor，可用于迭代
        results = imysql.table('table1').where('id IN (3,4)').all(fetch=True)
        self.assertEqual(len(results), 2)

    def test_0_7(self):
        ''' 字典条件查询 '''
        results = imysql.table('table1').where({'id': 3}).all(fetch=True)
        self.assertEqual(len(results), 1)

    def test_0_9(self):
        ''' IN 查询 '''
        results = imysql.table('table1').where({'id': ['in', (3, 4)]}).all(fetch=True)
        self.assertEqual(len(results), 2)

    def test_1_0(self):
        ''' LIKE 模糊查询 '''
        results = imysql.table('table1').where({'name': ('like', '张%')}).all(fetch=True)
        self.assertEqual(len(results), 1)

    def test_1_1(self):
        ''' BETWEEN 查询 '''
        results = imysql.table('table1').where({'id': ['between', (3, 4)]}).all(fetch=True)
        self.assertEqual(len(results), 2)

    def test_1_2(self):
        ''' 条件操作符 '''

        # 大于等于
        count = imysql.table('table1').where({'id': ['>=', 3]}).count()
        self.assertEqual(count, 2)
        # 小于
        count = imysql.table('table1').where({'id': ['<', 4]}).count()
        self.assertEqual(count, 1)
        # 不等于
        count = imysql.table('table1').where({'id': ['<>', 3]}).count()
        self.assertEqual(count, 1)
        # 不为空
        count = imysql.table('table1').where({'id': ['<>', '']}).count()
        self.assertEqual(count, 2)
        # NULL
        count = imysql.table('table1').where({'name': ['is', None]}).count()
        self.assertEqual(count, 0)
        # NOT NULL
        count = imysql.table('table1').where({'name': ['is not', None]}).count()
        self.assertEqual(count, 2)
        # NOT IN
        count = imysql.table('table1').where({'id': ['not in', (3, 4)]}).count()
        self.assertEqual(count, 0)
        # NOT LIKE
        count = imysql.table('table1').where({'name': ['not like', '张%']}).count()
        self.assertEqual(count, 1)

    def test_1_3(self):
        ''' 追加查询 '''

        # 特殊情况下使用 and_where
        results = imysql.table('table1').where({'id': 3}).and_where({'name': '张三'}).all(fetch=True)
        self.assertEqual(len(results), 1)

        # 一般情况下，扩充字典条件即可
        condition = {'id': 3}
        if True:
            condition['name'] = '张三'
        
        results = imysql.table('table1').where(condition).all(fetch=True)
        self.assertEqual(len(results), 1)

    def test_1_4(self):
        ''' OR 查询 '''
        results = imysql.table('table1').where({'id': 3}).or_where({'id': 4}).all(fetch=True)
        self.assertEqual(len(results), 2)

    def test_1_5(self):
        ''' 统计 '''
        count = imysql.table('table1').count()
        self.assertEqual(count, 2)

    def test_1_6(self):
        ''' 其他 '''

        # 查询单行
        one = imysql.table('table1').where('id=3').one()
        self.assertEqual(one.get('id'), 3)

        # 查询单个值
        name = imysql.table('table1').select('name').where('id=3').scalar()
        self.assertEqual(name, '张三')

        # 查询一列
        column = imysql.table('table1').select('name').limit(3).column()
        self.assertTrue(type(column) is list and type(column[0]) is str)

        # 查询结果索引（一维）
        result = imysql.table('table1').limit(10).index(key='id', value='name')
        self.assertEqual(result.get(3), '张三')
        self.assertEqual(result.get(4), '匿名')

        # 查询结果索引（二维）
        result = imysql.table('table1').limit(10).index(key='id')
        self.assertEqual(result.get(3).get('name'), '张三')

    def test_1_7(self):
        ''' 选择字段 '''
        one = imysql.table('table1').select('id').where('id=3').one()
        self.assertEqual(one.get('id'), 3)
        self.assertTrue(one.get('name') is None)

    def test_1_8(self):
        ''' 联表查询 '''

        # 先给table2插入一条记录
        imysql.table('table2').insert_many([
            {'id': 1, 'age': 18},
            {'id': 3, 'age': 18},
            {'id': 5, 'age': 20},
        ])

        # 默认 LEFT JOIN
        results = (
            imysql.table('table1', alias='t1')
            .select('t1.id,t1.name,t2.age')
            .join('table2', alias='t2', on='t1.id=t2.id')
            .all(fetch=True)
        )
        self.assertEqual(len(results), 2)

        # RIGHT JOIN
        results = (
            imysql.table('table1 t1')
            .select(['t1.id', 't1.name', 't2.age'])
            .join('table2 t2', on='t1.id=t2.id', how='right')
            .all(fetch=True)
        )
        self.assertEqual(len(results), 3)

        # INNER JOIN
        results = (
            imysql.table('table1 t1')
            .join('table2 t2', on='t1.id=t2.id', how='inner')
            .all(fetch=True)
        )
        self.assertEqual(len(results), 1)

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
        self.assertEqual(len(results), 0)

    def test_1_9(self):
        ''' 分组及排序 '''

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
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].get('age'), 18)

    def test_2_0(self):
        ''' 结果筛选 '''

        results = (
            imysql.table('table2')
            .select('age, count(*) as num')
            .group_by('age')
            .having('num > 1')
            .order_by('num desc')
            .all(fetch=True)
        )
        self.assertEqual(len(results), 1)

    def test_2_1(self):
        ''' 分页查询 '''
        
        ''' 插入多行记录 '''
        imysql.table('table1').insert_many([
            {'name': '李四'},
            {'name': '王五'},
            {'name': '赵六'},
            {'name': '陈七'},
        ])

        # 分页查询（skip limit）
        results1 = imysql.table('table1').order_by('id asc').skip(0).limit(3).all(fetch=True)
        self.assertEqual(len(results1), 3)

        results2 = imysql.table('table1').order_by('id asc').skip(1).limit(3).all(fetch=True)
        self.assertEqual(len(results2), 3)
        self.assertEqual(results2[0].get('id'), results1[1].get('id'))

    def test_2_2(self):
        ''' 执行原生SQL '''
        
        # 注：imysql.execute 返回原生 pymysql.cursors.DictCursor 对象，后继操作须自己处理

        sql = 'SELECT * FROM table1 WHERE id=%s'
        
        # 解析多行
        results = imysql.execute(sql, (3,)).all(fetch=True)
        self.assertEqual(len(results), 1)
        # 解析单行
        one = imysql.execute(sql, (3,)).one()
        self.assertEqual(one.get('id'), 3)
        # 其他操作请看 test_1_6
        names = imysql.execute(sql, (3,)).index('id', 'name')
        self.assertEqual(names.get(3), '张三')
        
        # 切换数据库来执行SQL，详情请看：test_2_7
        # imysql.switch('other').execute(sql)
        
        # 使用助手函数来拼接SQL（防注入）
        # gen_condition 用于 where on having 等子句的条件处理
        # gen_order_by 用于排序
        # gen_limit 用于分页
        
        '''
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
        '''

    def test_2_3(self):
        ''' 获取 sql '''

        # 方法一：执行前获取
        sql1 = imysql.table('table1').select('id,name').limit(2).get_raw_sql()
        results = imysql.execute(sql1, fetch=True)
        self.assertEqual(len(results), 2)

        # 方法二：执行后获取
        sql2 = imysql.get_last_sql()
        self.assertEqual(sql2, sql1)

        # 注：多线程高并发情况下，方法二可能不太准确

    def test_2_4(self):
        ''' 获取插入的ID '''

        # 方法一：insert_one 方法返回值
        id1 = imysql.table('table1').insert_one({'name': '周八'})
        # 方法二：get_insert_id
        id2 = imysql.get_insert_id()
        self.assertEqual(id1, id2)

        # 注：多线程高并发情况下，方法二可能不太准确

    def tset_2_5(self):
        ''' 获取影响的行数 '''

        # 方法一：函数返回值
        result = imysql.table('table1').insert_many([
            {'name': '吴九'},
            {'name': '郑十'},
        ])
        # 方法二：get_effected_rows
        effected_rows = imysql.get_effected_rows()
        self.assertEqual(result, effected_rows)

        # 注1：insert_many、update_one、update_many、delete等函数返回的都是影响行数
        # 注2：多线程高并发情况下，方法二可能不太准确

    def test_2_6(self):
        ''' 多连接及切换连接 '''

        '''
        注：要先新建一个 test2 数据库，并新建 table1 数据表

        CREATE DATABASE test2 charset = utf8mb4;

        CREATE TABLE `test2`.`table1` (
          `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
          `name` varchar(20) CHARACTER SET utf8mb4 DEFAULT '' COMMENT '姓名',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        '''
        
        # 添加一个连接
        imysql.connect({
            'host': '127.0.0.1',
            'user': 'root',
            'password': 'root',
            'database': 'test2'
        }, name='other')

        imysql.switch('other').execute('TRUNCATE TABLE table1')

        # 临时切换other库
        name = imysql.switch('other').table('table1').insert_one({'name': '张三'})
        name = imysql.switch('other').table('table1').select('name').where('id=1').scalar()
        self.assertEqual(name, '张三')

        # 临时switch后，会自动恢复default数据库
        _id = imysql.table('table1').select('id').where({'name': name}).scalar()
        self.assertTrue(_id != 1)
        results = imysql.execute('SELECT COUNT(*) AS ct FROM table1', fetch=True)
        self.assertTrue(results[0].get('ct') > 1)

        # 永久切换other
        imysql.switch('other', inplace=True)
        
        count = imysql.table('table1').count()
        self.assertEqual(count, 1)

        # other 库没有 table2
        import pymysql.err
        with self.assertRaises(pymysql.err.ProgrammingError):
            imysql.table('table2').count()

    def test_2_7(self):
        imysql.switch('default', inplace=True)

    def test_2_8(self):
        ''' 事务操作 '''

        # 注：insert_one、insert_many、update_one、update_many、delete 等操作默认会自动提交事务
        
        # 如果希望多个操作完成之后再提交事务，可以上下文管理器或装饰器
        
        import pymysql.err
        
        # 上下文使用方法：
        with self.assertRaises(pymysql.err.OperationalError):
            with transaction.atomic():
                imysql.table('table1').delete({'name': '匿名'})
                # 下面这句会报错，事务回滚
                imysql.table('table1').insert_one({'name': '匿名', 'age': 18})

        # 事务回滚了，匿名用户应该还在
        _id = imysql.table('table1').select('id').where({'name': '匿名'}).scalar()
        self.assertTrue(_id > 0)

        # 捕捉异常
        # 方法一：包裹整个上下文管理器，无需自己处理事务回滚
        try:
            with transaction.atomic():
                imysql.table('table1').delete({'name': '匿名'})
                # 下面这句会报错，事务回滚
                imysql.table('table1').insert_one({'name': '匿名', 'age': 18})
        except Exception as e:
            self.assertEqual(str(e), "(1054, \"Unknown column 'age' in 'field list'\")")

        # 方法二：在上下文内部捕捉异常，自己处理事务回滚
        with transaction.atomic() as atomic:
            imysql.table('table1').delete({'name': '匿名'})
            try:
                imysql.table('table1').insert_one({'name': '匿名', 'age': 18})
                # 不抛出异常的情况下，上下文管理器会自动提交事务，不用手动处理
                # atomic.commit()
            except Exception as e:
                # 捕捉了异常，需要自己手动回滚事务
                self.assertEqual(str(e), "(1054, \"Unknown column 'age' in 'field list'\")")
                atomic.rollback()

        # 切换数据库连接
        other = imysql.switch('other')
        with transaction.atomic(other.conn) as atomic:
            other.table('table1').insert_one({'name': '李四'})
            other.table('table1').insert_one({'name': '王五'})
            self.assertEqual(other.table('table1').count(), 3)
            atomic.rollback()
            self.assertEqual(other.table('table1').count(), 1)

        # 多个数据库链接
        default = imysql.switch('default')
        with transaction.atomic(default.conn):
            default.table('table1').insert_one({'name': '吴九'})
            default.table('table1').insert_one({'name': '郑十'})
        
        other = imysql.switch('other')
        with transaction.atomic(other.conn):
            other.table('table1').insert_one({'name': '李四'})
            other.table('table1').insert_one({'name': '王五'})

    def test_2_9(self):
        ''' 事务操作 '''

        # 注：insert_one、insert_many、update_one、update_many、delete 等操作默认会自动提交事务
        
        # 如果希望多个操作完成之后再提交事务，可以上下文管理器或装饰器
        
        import pymysql.err
        
        @transaction.atomic
        def some_operation():
            imysql.table('table1').delete({'name': '匿名'})
            # 下面这句会报错，事务回滚
            imysql.table('table1').insert_one({'name': '匿名', 'age': 18})

        with self.assertRaises(pymysql.err.OperationalError):
            some_operation()

        @transaction.atomic
        def some_operation():
            imysql.table('table1').delete({'name': '匿名'})
            try:
                imysql.table('table1').insert_one({'name': '匿名', 'age': 18})
                # 不抛出异常的情况下，装饰器会自动提交事务，不用手动处理
                # imysql.default_conn.commit()
            except Exception:
                # 捕获了异常，需要自己手动回滚事务
                imysql.default_conn.rollback()

        some_operation()

        # 事务回滚了，匿名用户应该还在
        _id = imysql.table('table1').select('id').where({'name': '匿名'}).scalar()
        self.assertTrue(_id > 0)

        # 切换默认数据库连接为 other
        imysql.switch('other', inplace=True)
        
        @transaction.atomic
        def some_operation():
            # 这里使用的是 other 连接里默认数据库的 table1 表
            effected_rows = imysql.table('table1').delete({'name': '匿名'})
            self.assertEqual(effected_rows, 0)
        
        some_operation()

        # 切换默认数据库连接为 default
        imysql.switch('default', inplace=True)

    def test_3_0(self):
        ''' 事务嵌套 '''

        with transaction.atomic() as atomic:
            with transaction.atomic():
                imysql.table('table1').delete({'name': '匿名'})

            # 事务回滚
            atomic.rollback()

        @transaction.atomic
        def some_operation_1():
            imysql.table('table1').delete({'name': '匿名'})

        @transaction.atomic
        def some_operation_2():
            some_operation_1()
            # 事务回滚
            imysql.default_conn.rollback()

        some_operation_2()

        @transaction.atomic
        def some_operation_3():
            with transaction.atomic():
                imysql.table('table1').delete({'name': '匿名'})

            # 事务回滚
            imysql.default_conn.rollback()

        some_operation_3()

        # 事务嵌套时，所有操作只能被最外层的事务提交，里面的（自动提交）事务无效
        # 所以匿名用户应该还在
        _id = imysql.table('table1').select('id').where({'name': '匿名'}).scalar()
        self.assertTrue(_id > 0)

    def test_3_1(self):
        ''' 内置异常 '''

        import chain_pymysql.exceptions

        with self.assertRaises(chain_pymysql.exceptions.RuntimeError):
            imysql.connect({}, name='default')
        
        with self.assertRaises(chain_pymysql.exceptions.RuntimeError):
            imysql.switch('db1')
        
        with self.assertRaises(chain_pymysql.exceptions.RuntimeError):
            imysql.table('')
        
        with self.assertRaises(chain_pymysql.exceptions.RuntimeError):
            imysql.table('table1').skip(0).limit(-1).all()
        
        with self.assertRaises(chain_pymysql.exceptions.RuntimeError):
            imysql.table('table1').skip(0).limit(-1).all()
        
        try:
            imysql.table('table1').where({
                'id': ['=', "3' UNION ALL SELECT NULL,NULL,NULL,NULL,NULL,NULL-- zxmL"]
            }).all(fetch=True)
        except chain_pymysql.exceptions.RuntimeError as e:
            self.assertEqual(e.code, 403)
            self.assertEqual(e.get_code(), 403)


if __name__ == '__main__':
    unittest.main()
