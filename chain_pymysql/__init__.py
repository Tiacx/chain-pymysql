# chain-pymysql: Easy to use pymysql.

# @link https://github.com/Tiacx/chain-pymysql
# @copyright Copyright (c) 2022 Tiac
# @license MIT
# @author Tiac
# @since 1.0.0

import re
import json
import pymysql
import pymysql.connections
from pymysql import converters, FIELD_TYPE
from pymysql.converters import escape_string
from . import exceptions

# 连接集合
connections = dict()
# 全局游标
global_cursor = None
# 缓存信息
cache_data = dict()


# 事务处理
class transaction:

    level = 0

    def __init__(self, conn=None):
        # 游标
        if conn is None:
            self.conn = global_cursor.connection
        else:
            self.conn = conn

    @classmethod
    def atomic(cls, conn_or_func=None):
        ''' 原子性事务 '''

        # 上下文管理器
        if conn_or_func is None or isinstance(conn_or_func, pymysql.connections.Connection):
            return cls(conn=conn_or_func)

        # 装饰器
        def wrapper(*args, **kwargs):
            cls.adjust_level(1)
            
            try:
                result = conn_or_func(*args, **kwargs)
                cls.adjust_level(-1)
                if cls.get_level() == 0:
                    global_cursor.connection.commit()
                return result
            except Exception as e:
                cls.adjust_level(-1)
                global_cursor.connection.rollback()
                raise e

        return wrapper

    def __enter__(self):
        self.__class__.adjust_level(1)

        return self.conn

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.__class__.adjust_level(-1)

        if self.__class__.get_level() == 0:
            if exc_type is None:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                # 回滚后，插入ID和影响行数都改为0
                cache_data['effected_rows'] = 0
                cache_data['last_insert_id'] = 0
                return False

    @classmethod
    def get_level(cls):
        return cls.level

    @classmethod
    def adjust_level(cls, val: int):
        cls.level += val


# 查询构建器
class imysql:

    def __init__(self, cursor=None):
        # 数据
        self.data = dict()
        # 原生SQL
        self.raw_sql = ''
        # 上一个SQL
        self.last_sql = ''
        # 游标
        if cursor is not None:
            self.cursor = cursor
        else:
            self.cursor = global_cursor

        # 连接
        self.conn = self.cursor.connection
        
        # 断线重连
        self.conn.ping(reconnect=True)

    @classmethod
    def connect(cls, options: dict, name='default'):
        ''' 连接 MySql

        :param https://pymysql.readthedocs.io/en/latest/modules/connections.html
        '''

        if name in connections:
            raise exceptions.RuntimeError((400, '请忽重复连接【%s】' % name))

        global global_cursor

        if 'charset' not in options:
            options['charset'] = 'utf8'

        if 'cursorclass' not in options:
            options['cursorclass'] = pymysql.cursors.DictCursor

        if 'conv' not in options:
            conv = converters.conversions
            conv[FIELD_TYPE.NEWDECIMAL] = float
            conv[FIELD_TYPE.DATE] = str
            conv[FIELD_TYPE.TIMESTAMP] = str
            conv[FIELD_TYPE.DATETIME] = str
            conv[FIELD_TYPE.TIME] = str
            options['conv'] = conv

        # 连接
        conn = pymysql.connect(**options)
        # 保存连接
        connections[name] = conn
        
        # 全局游标
        if global_cursor is None:
            global_cursor = conn.cursor()
        
        return conn
    
    @classmethod
    def switch(cls, name: str, db_name=None, inplace=False):
        ''' 切换数据库连接

        :param name: 连接实例名称，例如：prod1、prod2 或 prod1.prod_member
        :param db_name: 数据库名称，例如：prod_member
        :param inplace: 是否永久生效，False：临时生效，执行完SQL后恢复默认连接，True：永久生效，执行完不恢复默认连接
        :return cursor
        '''

        global global_cursor
        
        if name.find('.') > -1:
            name, db_name = name.split('.')

        # 如果连接不存在，则报错
        if name not in connections:
            raise exceptions.RuntimeError((400, '【%s】连接不存在，请先连接'))
        
        # 连接
        conn = connections[name]
        # 断线重连
        conn.ping(reconnect=True)
        # 游标
        cursor = conn.cursor()

        # 切换到同连接的其他数据库
        if db_name is not None:
            conn.select_db(db_name)
        
        # 全局游标
        if inplace is True:
            global_cursor = cursor
            return cls
        else:
            # 实例化
            instance = cls(cursor=cursor)
            # 动态修改 table 方法
            instance.table = instance._table
            # 动态修改 execute 方法
            instance.execute = instance._execute
            return instance

    @classmethod
    def table(cls, table: str, alias=''):
        ''' 设置表（静态调用） '''
        instance = cls()
        instance.data['table'] = instance.gen_table(table, alias)
        return instance

    def _table(self, table: str, alias=''):
        ''' 设置表（实例化后调用） '''
        self.data['table'] = self.__class__.gen_table(table, alias)
        return self

    @classmethod
    def execute(cls, sql: str, args=None, fetch=False):
        ''' 执行原生SQL

        :param sql
        :param args: sql 参数
        :return imysql 或 result
        '''

        instance = cls()
        return instance._execute(sql=sql, args=args, fetch=fetch)

    def _execute(self, sql: str, args=None, fetch=False):
        ''' 执行原生SQL

        :param sql
        :param args: sql 参数
        :return imysql 或 result
        '''
        
        sql = global_cursor.mogrify(sql, args)

        operation = sql.split(' ')[0].strip().lower()
        
        if operation in ['insert', 'replace', 'update', 'delete', 'truncate', 'create', 'drop', 'alter']:
            with transaction.atomic(self.conn):
                self.cursor.execute(sql)

                # 记录SQL信息
                cache_data['last_sql'] = sql
                cache_data['last_operation'] = operation
                cache_data['last_insert_id'] = global_cursor.connection.insert_id()
                cache_data['effected_rows'] = global_cursor.rowcount

                return cache_data['last_insert_id'] if operation == 'insert' else cache_data['effected_rows']
        else:
            self.raw_sql = sql
        
            # 记录SQL信息
            cache_data['last_operation'] = 'select'
            cache_data['last_sql'] = sql
            
            if fetch is False:
                return self
            else:
                return self.all(fetch=True)

    @classmethod
    def gen_table(cls, table: str, alias=''):
        ''' 处理表名

        :param table: 数据表 或 数据库.数据表
        :param alias: 数据表别名
        :return sql
        '''

        if table == '':
            raise exceptions.RuntimeError((400, 'table 不能为空'))

        if table.find(' ') > -1:
            table, alias = table.split(' ')

        if table.find('.') == -1:
            table = f'`{table}`'
        else:
            table = '`{}`'.format(table.replace('.', '`.`'))

        if alias != '':
            table += f' `{alias}`'

        return table

    def gen_setter(self, data: dict):
        ''' 处理 SQL SET 部分

        :param data: 更新的内容
        :return sql
        '''

        s = ''
        for k, v in data.items():
            s += "`{}`='{}',".format(k, v)

        s = s[0:-1]
        return s

    def gen_fields(self, data: 'list|dict'):
        ''' 处理 insert_many fields 部分

        :param data: 更新的内容
        :return sql
        '''

        if type(data) is dict:
            data = [data]

        fields = '('
        for field in data[0].keys():
            fields += f'`{field}`,'

        fields = fields[0:-1] + ')'
        return fields

    def gen_values(self, data: 'list|dict'):
        ''' 处理 insert_many values 部分

        :param data: 更新的内容
        :return sql
        '''

        if type(data) is dict:
            data = [data]

        values = []
        for row in data:
            value = tuple(x for x in row.values())
            values.append(value)
        return values

    @classmethod
    def check_validity(cls, data: 'str|dict|list'):
        ''' 检查字符串合法性

        :param data: 需要验证的内容
        :return bool
        '''

        if type(data) is str:
            s = data
        else:
            s = json.dumps(data)

        if s == '':
            return True
        
        # 过滤规则
        filter_rule = "\\<.+javascript:window\\[.{1}\\\\x|<.*=(&#\\d+?;?)+?>|<.*data=data:text\\/html.*>|\\b(alert\\(|confirm\\(|expression\\(|prompt\\(|benchmark\s*?\\(\d+?|sleep\s*?\\([\d\.]+?\\)|load_file\s*?\\()|<[^>]*?\\b(onerror|onmousemove|onload|onclick|onmouseover)\\b|\\b(and|or)\\b\\s*?([\\(\\)'\"\\d]+?=[\\(\\)'\"\\d]+?|[\\(\\)'\"a-zA-Z]+?=[\\(\\)'\"a-zA-Z]+?|>|<|\s+?[\\w]+?\\s+?\\bin\\b\\s*?\(|\\blike\\b\\s+?[\"'])|\\/\\*.+?\\*\\/|<\\s*script\\b|\\bEXEC\\b|UNION.+?SELECT(\\(.+\\)|\\s+?.+?)|UPDATE(\\(.+\\)|\\s+?.+?)SET|INSERT\\s+INTO.+?VALUES|(SELECT|DELETE)(\\(.+\\)|\\s+?|\\s+?.+?\\s+?)FROM(\\(.+\\)|\\s+?.+?)|(CREATE|ALTER|DROP|TRUNCATE)\\s+(TABLE|DATABASE)|(EXTRACTVALUE|UPDATEXML)(\\(.+\\)|\\s+?.+?)"
        
        return re.search(filter_rule, s, re.I) is None

    @classmethod
    def gen_condition(cls, condition: 'str|dict'):
        ''' 处理条件

        :param condition: 条件，字符串或字典
        :return sql
        '''

        if cls.check_validity(condition) is False:
            raise exceptions.RuntimeError((403, '筛选条件中包含非法参数'))

        if type(condition) is str:
            return condition
        
        s = ''
        if type(condition) is dict:
            for key in condition:
                operate = '='
                value = condition[key]
                if value is None:
                    continue
                elif type(value) is str:
                    value = f"'{escape_string(value)}'"
                elif type(value) is list or type(value) is tuple:
                    operate = value[0].upper()
                    quote = value[2] if len(value) >= 3 else True
                    value = value[1]
                    
                    if type(value) is str:
                        value = escape_string(value)
                        if quote is True:
                            value = "'{}'".format(escape_string(value))
                    elif hasattr(value, '__iter__'):
                        if quote is True:
                            value = [escape_string(str(x)) for x in value]
                        else:
                            value = [int(x) if str(x).isnumeric() else 0 for x in value]
                        
                        if operate in ['IN', 'NOT IN']:
                            value = [str(x) for x in value]
                            if quote is True:
                                value = "('{}')".format("','".join(value))
                            else:
                                value = "({})".format(','.join(value))
                        elif operate in ['BETWEEN', 'NOT BETWEEN']:
                            if quote is True:
                                value = f"'{value[0]}' AND '{value[1]}'"
                            else:
                                value = f"{value[0]} AND {value[1]}"

                if key.find('.') > -1:
                    key = key.replace('.', '`.`')

                if value is None:
                    value = 'NULL'
                
                s += f' AND `{key}` {operate} {value}'
            
            s = s[5:]
        
        return s

    @classmethod
    def gen_order_by(cls, by: 'str|list|tuple', ascending: 'bool|list' = True):
        ''' 处理排序

        :param by: 排序字段，可传 str 或 list
        :param ascending: 是否升序，可传 bool 或 list，默认 True
        :return sql
        '''

        if cls.check_validity(by) is False:
            raise exceptions.RuntimeError((403, '排序条件中包含非法参数'))

        if type(by) is str:
            if by.find(',') > -1 or by.find(' ') > -1:
                return f' ORDER BY {by}'
            else:
                flag = 'ASC' if ascending else 'DESC'
                return f' ORDER BY {by} {flag}'
        elif hasattr(by, '__iter__'):
            s = ' ORDER BY'
            for i, x in enumerate(by):
                if x.find(' ') > -1:
                    s += f' {x},'
                else:
                    if type(ascending) is list:
                        flag = 'ASC' if ascending[i] else 'DESC'
                    else:
                        flag = 'ASC' if ascending else 'DESC'
                    s += f' {x} {flag},'
            
            return s[0:-1]
        else:
            return ''

    @classmethod
    def gen_limit(cls, skip: int, limit: int):
        ''' 处理分页

        :param skip: 忽略行数
        :param limit: 分页大小
        :return sql
        '''

        s = ' LIMIT '
        
        if skip > 0:
            s += f'{skip},'
        
        if limit > 0:
            s += f'{limit}'
            return s
        else:
            return ''

    def select(self, fields: 'str|list|tuple'):
        ''' 选择字段

        :param fields: 字段，str 或 iterable
        :return self
        '''

        if self.__class__.check_validity(fields) is False:
            raise exceptions.RuntimeError((403, '字段中包含非法字符'))

        if type(fields) is str:
            self.data['fields'] = fields
        else:
            fields = '`,`'.join([x.replace('.', '`.`') for x in fields])
            self.data['fields'] = '`%s`' % fields
        
        return self

    def join(self, table: str, on: 'str|dict', alias='', how='left'):
        ''' 联表查询

        :param table: 表名 或 数据库.表名
        :param on: 关联条件，str 或 dict
        :param alias: 数据表别名
        :param how: 关联方式 left right inner 或 outer
        :return self
        '''

        if self.data.get('join') is None:
            self.data['join'] = []
        
        table = self.__class__.gen_table(table, alias)
        
        s = f' {how.upper()} JOIN {table} ON '
        s += self.__class__.gen_condition(on)

        self.data['join'].append(s)

        return self

    def where(self, condition: 'str|dict'):
        ''' 筛选条件

        :param condition: 筛选条件，str 或 dict
        :return self
        '''

        self.data['where'] = self.__class__.gen_condition(condition)
        return self

    def and_where(self, condition: 'str|dict'):
        ''' 追加筛选条件

        :param condition: 筛选条件，str 或 dict
        :return self
        '''

        if self.data.get('where'):
            self.data['where'] = '(%s) AND (%s)' % (self.data.get('where'), self.__class__.gen_condition(condition))
        else:
            self.data['where'] = self.__class__.gen_condition(condition)
        
        return self

    def or_where(self, condition: 'str|dict'):
        ''' 追加筛选条件

        :param condition: 筛选条件，str 或 dict
        :return self
        '''

        if self.data.get('where'):
            self.data['where'] = '(%s) OR (%s)' % (self.data.get('where'), self.__class__.gen_condition(condition))
        else:
            self.data['where'] = self.__class__.gen_condition(condition)
        
        return self

    def group_by(self, group_by: 'str|list|tuple'):
        ''' 分组

        :param group_by: 筛选条件，str 或 iterable
        :return self
        '''

        if self.__class__.check_validity(group_by) is False:
            raise exceptions.RuntimeError((403, '分组条件中包含非法参数'))

        if type(group_by) is str:
            self.data['group_by'] = group_by
        else:
            self.data['group_by'] = ','.join(group_by)
        
        return self

    def having(self, condition: 'str|dict'):
        ''' 结果集筛选

        :param condition: 筛选条件，str 或 dict
        :return self
        '''

        self.data['having'] = self.__class__.gen_condition(condition)
        return self

    def order_by(self, by: 'str|list', ascending: 'bool|list' = True):
        ''' 处理排序

        :param by: 排序字段，可传 str 或 list
        :param ascending: 是否升序，可传 bool 或 list，默认 True
        :return sql
        '''

        self.data['order_by'] = self.__class__.gen_order_by(by, ascending)
        
        return self

    def skip(self, num: int):
        ''' 忽略行数

        :param num: 忽略行数
        :return self
        '''

        if num < 0:
            raise exceptions.RuntimeError((400, 'skip: num 须大于等于0'))
        
        self.data['skip'] = num
        return self

    def limit(self, num: int):
        ''' 分页

        :param num: 分页大小
        :return self
        '''

        if num < 0:
            raise exceptions.RuntimeError((400, 'limit: num 须大于等于0'))
        
        self.data['limit'] = num
        
        return self

    def get_raw_sql(self, wrapper=''):
        ''' 获取原生 SQL '''

        if self.raw_sql != '':
            return self.raw_sql

        table = self.data.get('table')
        fields = self.data.get('fields', '*')
        
        sql = f'SELECT {fields} FROM {table}'

        if self.data.get('join'):
            sql += ''.join(self.data.get('join'))

        if self.data.get('where'):
            sql += ' WHERE ' + self.data.get('where')

        if self.data.get('group_by'):
            sql += ' GROUP BY ' + self.data.get('group_by')

        if self.data.get('having'):
            sql += ' HAVING ' + self.data.get('having')

        if self.data.get('order_by'):
            sql += self.data.get('order_by')

        skip = self.data.get('skip', 0)
        limit = self.data.get('limit', 0)
        sql += self.__class__.gen_limit(skip, limit)

        if wrapper != '':
            sql = wrapper % sql
        
        # 记录SQL信息
        cache_data['last_operation'] = 'select'
        cache_data['last_sql'] = sql

        self.raw_sql = sql

        return sql

    def reset_data(self):
        ''' 重置数据 '''

        self.data = dict()
        self.raw_sql = ''

    def all(self, fetch=False):
        ''' 查询多行

        :param fetch: fetch结果，默认 False
        :return cursor 或 result
        '''

        sql = self.get_raw_sql()
        self.cursor.execute(sql)
        self.reset_data()
        
        if fetch is True:
            return self.cursor.fetchall()
        else:
            return self.cursor

    def index(self, key: str, value=None):
        ''' 查询结果用key索引

        :param key: 索引字段
        :param value: 如果指定value字段，则返回一维dict，否则返回二维dict
        :return cursor 或 result
        '''

        result = dict()
        
        for item in self.all():
            if value is None:
                result[item.get(key)] = item
            else:
                result[item.get(key)] = item.get(value)

        return result

    def one(self):
        ''' 查询一行 '''

        self.skip(num=0)
        self.limit(num=1)
        
        sql = self.get_raw_sql()
        self.cursor.execute(sql)
        self.reset_data()
        
        return self.cursor.fetchone()

    def scalar(self):
        ''' 查询一个值 '''

        one = self.one()
        if type(one) is dict:
            return list(one.values())[0]
        else:
            return ''

    def column(self):
        ''' 查询一列 '''

        results = self.all(fetch=True)
        if len(results) > 0:
            key = list(results[0].keys())[0]
            return [item.get(key) for item in results]
        else:
            return ''

    def count(self):
        ''' 统计 '''
        
        wrapper = ''
        
        if self.data.get('limit'):
            wrapper = 'SELECT COUNT(*) AS ct FROM (%s) t'
        else:
            self.data['fields'] = 'count(*) as ct'
        
        sql = self.get_raw_sql(wrapper)
        self.cursor.execute(sql)
        self.reset_data()
        one = self.cursor.fetchone()
        return one.get('ct') if one else False

    def insert_many(self, data: list, return_insert_id=False, verify=True):
        ''' 批量插入数据

        :param data: 插入的数据，dict 或 list
        :param return_insert_id: 返回插入的ID，默认 False
        :param verify: 是否验证数据合法性，默认 True
        :return 影响行数，当 return_insert_id = True 时，返回 insert_id
        '''
        
        if verify is True and self.__class__.check_validity(data) is False:
            raise exceptions.RuntimeError((403, '插入内容中包含非法字符'))

        # 开启事务处理
        with transaction.atomic(self.conn):
            
            table = self.data.get('table')
            fields = self.gen_fields(data)
            values = self.gen_values(data)
            types = ['%s' for x in values[0]]
            placeholder = "(%s)" % (','.join(types))

            sql = f'INSERT INTO {table} {fields} VALUES {placeholder}'
            
            effected_rows = self.cursor.executemany(sql, values)
            insert_id = self.conn.insert_id()
            # 记录SQL信息
            cache_data['last_operation'] = 'insert'
            cache_data['last_sql'] = sql
            cache_data['effected_rows'] = effected_rows
            cache_data['last_insert_id'] = insert_id
            # 返回插入的ID或影响的行数
            return insert_id if return_insert_id else effected_rows

    def insert_one(self, data: dict, verify=True):
        ''' 批量插入数据

        :param data: 插入的数据，dict 或 list
        :param verify: 是否验证数据合法性，默认 True
        :return insert_id
        '''
        return self.insert_many([data], return_insert_id=True, verify=verify)

    def update_many(self, condition: 'str|dict', data: dict, limit=0, verify=True):
        ''' 更新多行

        :param condition: 筛选条件，str 或 dict
        :param data: 更新的数据，字典类型
        :param limit: 限制更新的数量，默认 0，不限制
        :param verify: 是否验证数据合法性，默认 True
        :return 影响行数
        '''

        if verify is True and self.__class__.check_validity(data) is False:
            raise exceptions.RuntimeError((403, '更新内容中包含非法字符'))

        # 开启事务处理
        with transaction.atomic(self.conn):
            
            sql = "UPDATE {table} SET {setter} WHERE {where}{limit}".format(
                table=self.data.get('table'),
                setter=self.gen_setter(data),
                where=self.__class__.gen_condition(condition),
                limit=" LIMIT " + str(limit) if limit > 0 else ''
            )

            effected_rows = self.cursor.execute(sql)

            # 记录SQL信息
            cache_data['last_operation'] = 'update'
            cache_data['last_sql'] = sql
            cache_data['effected_rows'] = effected_rows
            # 返回影响的行情
            return effected_rows

    def update_one(self, condition: 'str|dict', data: dict, verify=True):
        ''' 更新一行

        :param condition: 筛选条件，str 或 dict
        :param data: 更新的数据，字典类型
        :param verify: 是否验证数据合法性，默认 True
        :return 影响行数
        '''
        return self.update_many(condition, data, limit=1, verify=verify)

    def delete(self, condition: 'str|dict', limit=0):
        ''' 删除数据

        :param condition: 筛选条件，str 或 dict
        :param limit: 限制删除的数量，默认 0，不限制
        :return 影响行数
        '''

        # 开启事务处理
        with transaction.atomic(self.conn):
            
            sql = "DELETE FROM {table} WHERE {where}{limit}".format(
                table=self.data.get('table'),
                where=self.__class__.gen_condition(condition),
                limit=" LIMIT " + str(limit) if limit > 0 else ''
            )
            
            effected_rows = self.cursor.execute(sql)

            # 记录SQL信息
            cache_data['last_operation'] = 'delete'
            cache_data['last_sql'] = sql
            cache_data['effected_rows'] = effected_rows
            # 返回影响的行数
            return effected_rows

    @staticmethod
    def get_last_sql():
        return cache_data.get('last_sql', '')

    @staticmethod
    def get_insert_id():
        return cache_data.get('last_insert_id', 0)

    @staticmethod
    def get_effected_rows():
        return cache_data.get('effected_rows', 0)

    @classmethod
    @property
    def default_conn(cls):
        return global_cursor.connection
