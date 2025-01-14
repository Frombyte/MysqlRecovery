import re
import sqlite3

class SQLConverter:
    def __init__(self, sqlite_db_path):
        self.sqlite_db_path = sqlite_db_path

    @staticmethod
    def mysql_to_sqlite_type(mysql_type):
        """转换MySQL的列类型为SQLite的列类型"""
        type_mappings = {
            'bigint': 'INTEGER',  # 大整数
            'int': 'INTEGER',  # 标准整数
            'mediumint': 'INTEGER',  # 中等大小整数
            'smallint': 'INTEGER',  # 小整数
            'tinyint': 'INTEGER',  # 很小的整数
            'varchar': 'TEXT',  # 可变长度字符串
            'char': 'TEXT',  # 定长字符串
            'text': 'TEXT',  # 长文本数据
            'mediumtext': 'TEXT',  # 中等长度文本
            'longtext': 'TEXT',  # 极大文本数据
            'datetime': 'TEXT',  # 日期和时间
            'date': 'TEXT',  # 日期
            'timestamp': 'TEXT',  # 时间戳
            'time': 'TEXT',  # 时间
            'year': 'INTEGER',  # 年份
            'float': 'REAL',  # 单精度浮点数
            'double': 'REAL',  # 双精度浮点数
            'decimal': 'REAL',  # 定点数
            'numeric': 'REAL',  # 精确数值，可以是定点数也可以是浮点数，但在SQLite中通常用REAL表示
            'blob': 'BLOB',  # 二进制大对象
            'tinyblob': 'BLOB',  # 很小的BLOB
            'mediumblob': 'BLOB',  # 中等大小的BLOB
            'longblob': 'BLOB',  # 极大的BLOB
            'enum': 'TEXT',  # 枚举，表示字符串对象的一个列表
            'set': 'TEXT',  # 集合，与ENUM类似，但每个值可以包含零个或多个值
            'bit': 'INTEGER',  # 位字段类型
            'binary': 'BLOB',  # 定长二进制字符串
            'varbinary': 'BLOB',  # 可变长度二进制字符串
            'json': 'TEXT'  # JSON类型
        }

        for mysql, sqlite in type_mappings.items():
            if mysql_type.startswith(mysql):
                return sqlite
        return 'TEXT'  # 默认转换为文本

    def convert_sql_file_to_db(self, mysql_file_obj):
        mysql_file_obj.seek(0)
        conn = sqlite3.connect(self.sqlite_db_path)
        cursor = conn.cursor()

        create_table_sql = ""
        for line in mysql_file_obj:
            line = line.strip()

            # 跳过不需要的行
            if line.startswith('SET') or line.startswith('DROP') or line.startswith('ENGINE='):
                continue

            # 开始新的CREATE TABLE语句
            if line.startswith('CREATE TABLE'):
                if create_table_sql:  # 如果已有未处理的create语句，先执行它
                    cursor.execute(create_table_sql)
                table_name = re.search(r'`(\w+)`', line).group(1)
                create_table_sql = f'CREATE TABLE "{table_name}" (\n'

            # 转换列定义
            elif line.startswith('`'):
                parts = line.split()
                column_name = parts[0].strip('`')
                mysql_type = parts[1]
                sqlite_type = self.mysql_to_sqlite_type(mysql_type)
                nullable = 'NOT NULL' if 'NOT NULL' in line else ''

                # 检测主键
                primary_key = 'PRIMARY KEY' if 'AUTO_INCREMENT' in line else ''
                create_table_sql += f'  "{column_name}" {sqlite_type} {nullable} {primary_key},\n'

            # 处理表尾部分
            elif line.startswith('PRIMARY KEY'):
                pass  # 在之前列定义中已处理主键

            elif line.startswith(')'):
                if create_table_sql and create_table_sql.endswith(',\n'):
                    create_table_sql = create_table_sql[:-2] + '\n'  # 去掉最后一个逗号
                create_table_sql += ');'
                cursor.execute(create_table_sql)
                create_table_sql = ""

        # 提交更改并关闭数据库连接
        conn.commit()
        conn.close()


