from pyparsing import pyparsing_unicode as ppu
from pyparsing import Word, alphas, alphanums, printables, Group, ZeroOrMore, OneOrMore, CaselessKeyword


class DqlParse(object):

    def parse(self, dql: str):
        dql = dql.replace("`", '')
        word = Word(alphanums + '_')
        table = ZeroOrMore(word + '.') + word
        table_alias = word
        normal_field = ZeroOrMore(table_alias + '.') + word
        field_alias = CaselessKeyword('AS') + Word(ppu.Chinese.alphanums + '"' + "'" + '_' + '(' + ')' + '（' + '）')
        function_field = ZeroOrMore(word) + '(' + Word(printables + '\n' + ' ' + ppu.Chinese.alphanums, excludeChars=')') + ')'
        function_field.set_parse_action(''.join)
        string_field = Word(printables, exclude_chars=',')
        field = normal_field ^ function_field ^ string_field
        from_label = CaselessKeyword('FROM')
        join_label = Word(alphas) + CaselessKeyword('JOIN')
        on_condition = OneOrMore(ZeroOrMore('AND') + Group(field + '=' + field))
        join_expr = join_label + table + table_alias + CaselessKeyword('ON') + on_condition
        where_label = CaselessKeyword('WHERE')
        where_condition = field + Word(alphas + '>' + '<' + '=', exclude_chars='B') + Word(ppu.Chinese.alphanums + printables + ' ', exclude_chars='AO')
        where_condition.set_parse_action(lambda x: [i.strip() for i in x])

        parser = (
            CaselessKeyword('SELECT')
            + Group(OneOrMore(Group(field + ZeroOrMore(field_alias)) + ZeroOrMore(','), stop_on=from_label)).set_results_name('columns')
            + from_label
            + Group(table + table_alias).set_results_name('table')
            + Group(OneOrMore(Group(join_expr), stop_on=where_label)).set_results_name('joins')
            + where_label
            + Group(OneOrMore(ZeroOrMore('AND') + Group(where_condition))).set_results_name('conditions')
            + ZeroOrMore(Word(printables + ' ')).set_results_name('other')
        )

        return parser.parse_string(dql).as_dict()

    def split_tables_joins(self, result: dict):
        name, _, database, _, table, table_alias = result.get('table')
        tables = dict()
        joins = dict()
        tables[name] = [(database, table, table_alias)]
        for item in result.get('joins'):
            if item[7] == '.':
                name, database, table, table_alias = item[2], item[4], f'{item[6]}.{item[8]}', item[9]
            else:
                name, database, table, table_alias = item[2], item[4], item[6], item[7]
            if name not in tables:
                tables[name] = []
            if name not in joins:
                joins[name] = []
            tables[name].append((database, table, table_alias))
            joins[name].append(item)

        return tables, joins

    def split_columns(self, mapping: dict, result: dict):
        columns = {name: [] for name in mapping.values()}
        columns_alias = []

        word = Word(alphanums + '_')
        field_expr = word + '.' + word

        for item in result.get('columns'):
            if type(item) is not list:
                continue

            if len(item) == 2 or item[1] == '.':
                name = mapping.get(item[0])
            else:
                matches = field_expr.search_string(item[0])
                name = mapping.get(matches[0][0])
                for x in matches[1:]:
                    if mapping.get(x[0]) != name:
                        fields = ', '.join(''.join(x) for x in matches)
                        raise AssertionError(f'【{fields}】字段结合跨链接异常')

            if len(item) >= 3:
                item[-2] = f' {item[-2]} '
            columns[name].append(''.join(item))
            columns_alias.append(item[-1].strip("'\""))
        return columns, columns_alias

    def split_conditions(self, mapping: dict, result: dict):
        conditions = {name: [] for name in mapping.values()}

        word = Word(alphanums + '_')
        field_expr = word + '.' + word

        for item in result.get('conditions'):
            if type(item) is not list:
                continue

            if item[1] == '.':
                name = mapping.get(item[0])
            else:
                matches = field_expr.search_string(item[0])
                name = mapping.get(matches[0][0])
                for x in matches[1:]:
                    assert mapping.get(x[0]) == name, f'【{x[0]}.{x[2]}】字段结合跨链接异常'

            if len(item) > 3 and item[3].upper() in ['IS', 'IN', 'LIKE', 'NOT']:
                conditions[name].append(''.join(item[0:3]) + ' ' + ' '.join(item[3:]))
            else:
                item[-2] = f' {item[-2]} '
                conditions[name].append(''.join(item))

        return conditions

    def gen_bridging(self, bridging: dict, join_item: list, columns: dict, alias_mapping: dict):
        field_mapping = dict()
        for name, items in columns.items():
            for column in items:
                t = column.split(' AS ')
                if len(t) == 1:
                    field_mapping[t[0]] = t[0].split('.')[-1]
                else:
                    field_mapping[t[0]] = t[-1].strip("'\"")
        
        flag = False

        result = []
        for i, x in enumerate(join_item.copy()):
            if type(x) is list:
                pos = x.index('=')
                left = x[0:pos]
                right = x[pos+1:]
                left_link = alias_mapping.get(left[0])
                rigth_link = alias_mapping.get(right[0])
                # 同一数据库连接不需要处理
                if left_link == rigth_link or rigth_link is None:
                    continue

                left_alias = field_mapping.get(''.join(left))
                if left_alias is None:
                    _name = alias_mapping.get(left[0])
                    left_alias = left[2]
                    columns[_name].append(''.join(left))
                
                right = ''.join(right)
                result.append([left_alias, right])
                columns[name].append(f"{right} AS '{left_alias}'")

                if name in bridging:
                    join_item = join_item[0:i-1]

        if len(result) > 0:
            if name not in bridging:
                bridging[name] = (join_item[0], result)
                flag = True
            else:
                bridging[name][1].extend(result)

        return flag, join_item

    def split_sql(self, dql: str):
        result = self.parse(dql)
        tables, joins = self.split_tables_joins(result)
        mapping = dict()
        sql_map = dict()
        i = 0
        for name, itemList in tables.items():
            for t in itemList:
                mapping[t[2]] = name
            
            sql_map[name] = dict()
            if i == 0:
                sql_map[name]['other'] = ' '.join(result.get('other', []))
            else:
                sql_map[name]['other'] = ''
            i = i + 1
        columns, columns_alias = self.split_columns(mapping, result)
        conditions = self.split_conditions(mapping, result)

        bridging = dict()

        for name, itemList in tables.items():
            sql_map[name]['columns'] = columns[name]
            sql_map[name]['from'] = f'FROM {itemList[0][0]}.{itemList[0][1]} {itemList[0][2]}'
            sql_map[name]['joins'] = ''
            
            if len(joins.get(name, [])) > 0:
                for join_item in joins.get(name, []):
                    flag, join_item = self.gen_bridging(bridging, join_item, columns, mapping)
                    if flag is True:
                        continue
                    
                    for x in join_item:
                        if type(x) is list:
                            sql_map[name]['joins'] += ' '
                            sql_map[name]['joins'] += ''.join(x).replace('=', ' = ')
                        elif x != '.':
                            sql_map[name]['joins'] += f' {x}'
                        else:
                            sql_map[name]['joins'] += x

                    sql_map[name]['joins'] = sql_map[name]['joins'].replace(f'{name}.', '')

            if len(conditions.get(name, [])) > 0:
                sql_map[name]['conditions'] = ' AND '.join(conditions.get(name, []))
            else:
                sql_map[name]['conditions'] = '1'

        sql_list = dict()
        for name, item in sql_map.items():
            fields = ', '.join(columns.get(name))
            sql_list[name] = f"SELECT {fields} {item['from']}{item['joins']} WHERE {item['conditions']} {item['other']}"
        
        return sql_list, bridging, columns_alias
