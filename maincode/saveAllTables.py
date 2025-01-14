import os
import re

from PyQt6.QtCore import Qt, pyqtSignal, QThread

from maincode.mysql_v4 import MysqlV4
from maincode.mysql_v8 import MysqlV8


class SaveAllTables(QThread):
    finished = pyqtSignal()
    showMessage = pyqtSignal(str)

    def __init__(self, center_widget, my_c,is_recout_all,parent=None):
        super(SaveAllTables, self).__init__(parent)
        self.center_widget = center_widget
        self.my_c = my_c
        self.is_recout_all = is_recout_all
        self.dirName = ""

    def run(self):
        # 在这里执行耗时的操作
        all_tables = {}
        for i in range(self.center_widget.treeWidget.topLevelItemCount()):
            db_item = self.center_widget.treeWidget.topLevelItem(i)
            db_name = db_item.text(0)
            all_tables[db_name] = [db_item.child(j).text(0) for j in range(db_item.childCount())]

        #过滤大字段表
        self.large_fields_tablename = self.extract_table_names_with_large_fields(self.my_c['table_struct_str'])
        self.large_fields_tablename = set(self.large_fields_tablename)
        for key,value in all_tables.items():
            all_tables[key] = [item for item in value if item not in self.large_fields_tablename]

        if all_tables != {'没有连接到数据库或数据库为空': []}:
            # dirName = QFileDialog.getExistingDirectory(self, "选择一个保存目录")
            self.showMessage.emit("正在保存中，请稍候...")
            for db_name in all_tables.keys():
                folder_name = db_name.replace('.db', '')
                folder_path = os.path.join(self.dirName, folder_name)
                # 判断保存文件夹内是否已经存在了某些表，如果存在就不保存
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
                files_name = os.listdir(folder_path)
                recort_tablenames = []
                for table in all_tables[db_name]:
                    expected_filename = f"{table}_data.sql"
                    if expected_filename not in files_name:
                        recort_tablenames.append(table)
                # if not os.path.exists(folder_path):
                # os.makedirs(folder_path)
                struct_str = self.my_c['table_struct_str']
                if self.my_c['mysql_v'] == 5:
                    processor = MysqlV4()
                elif self.my_c['mysql_v'] == 8:
                    processor = MysqlV8()
                row_dir = folder_path
                page_file_dir = fr"{self.my_c['dir']}"
                # if folder_name.split('_')[-1] == 'del':
                #     is_del = True
                #     processor.process(struct_str, row_dir, page_file_dir, is_del, all_tables[db_name], None)
                # if folder_name.split('_')[-1] == 'data':
                #     is_del = False
                if self.is_recout_all:
                    is_del = True
                else:
                    is_del = False
                processor.process(struct_str, row_dir, page_file_dir, is_del, recort_tablenames, None)
                # else:

                self.insertStructure(folder_path,recort_tablenames)
            self.showMessage.emit("保存完成！！！")
        else:
            self.showMessage.emit("未选择任何数据库或表")
        self.finished.emit()

    @staticmethod
    def extract_table_names_with_large_fields(sql_content):
        sql_content = sql_content.getvalue()
        # 定义大字段数据类型的正则表达式模式
        large_field_pattern = re.compile(
            r'\b(' + '|'.join(['TEXT', 'BLOB', 'LONGTEXT', 'JSON', 'mediumtext', 'longblob', 'tinytext']) + r')\b',
            re.IGNORECASE)

        # 存储含有大字段的表名
        tables_with_large_fields = []

        # 当前解析的表名
        current_table_name = None
        # 是否当前表包含大字段
        has_large_field = False

        # 按行处理SQL内容
        for line in sql_content.split('\n'):
            # 简单检查是否为建表语句的开始
            if line.strip().lower().startswith('create table'):
                # 提取表名，这里假设表名紧跟`CREATE TABLE`
                current_table_name = re.search(r'`([^`]+)`', line).group(1)
                has_large_field = False  # 重置标记
            elif large_field_pattern.search(line):
                # 如果行中匹配到大字段类型的正则表达式，则标记当前表包含大字段
                has_large_field = True
            elif line.strip().lower().startswith(');') or 'engine=' in re.sub(r'\s+', '', line.lower()):
                # 建表语句结束，这里通过移除所有空格后检查'engine='来增强对不同格式的支持
                if current_table_name and has_large_field:
                    # 如果当前表包含大字段，则加入列表
                    tables_with_large_fields.append(current_table_name)
                    current_table_name = None  # 重置当前表名为None，准备下一个表的检查

        return tables_with_large_fields

    def insertStructure(self, row_dir, table_list):
        self.my_c['table_struct_str'].seek(0)
        structure_content = self.my_c['table_struct_str'].read()
        tables = re.split(r'CREATE TABLE ', structure_content, flags=re.IGNORECASE)
        table_structure = {}
        for table in tables[1:]:
            table_name = re.search(r'`(\w+)`', table)
            if table_name:
                table_name = table_name.group(1)
                table_structure[table_name] = 'CREATE TABLE ' + table

        for filename in os.listdir(row_dir):
            if filename.endswith('_data.sql'):
                # 提取表名
                table_name_match = re.match(r'(.+)_data\.sql$', filename)
                if table_name_match:
                    base_table_name = table_name_match.group(1)
                    if base_table_name in table_structure and base_table_name in table_list:
                        file_path = os.path.join(row_dir, filename)
                        with open(file_path, 'r+', encoding='utf-8') as f:
                            data_content = f.readlines()
                            f.seek(0)
                            f.write(
                                f"--Structure for table: `{base_table_name}`\n\n{table_structure[base_table_name]}\n\n--Data for table: `{base_table_name}`\n\n")
                            for line in data_content:
                                f.write(line)

    # def insertStructure(self, row_dir):
    #     self.my_c['table_struct_str'].seek(0)
    #     structure_content = self.my_c['table_struct_str'].read()
    #     tables = re.split(r'CREATE TABLE ', structure_content, flags=re.IGNORECASE)
    #     table_structure = {}
    #     for table in tables[1:]:
    #         table_name = re.search(r'`(\w+)`', table)
    #         if table_name:
    #             table_name = table_name.group(1)
    #             table_structure[table_name] = 'CREATE TABLE ' + table
    #
    #     for filename in os.listdir(row_dir):
    #         if filename.endswith('.sql'):
    #             table_name = filename.rsplit('_', 1)[0]
    #             if table_name in table_structure:
    #                 file_path = os.path.join(row_dir, filename)
    #                 # # 使用'a'模式追加内容，避免一次性读取大文件
    #                 # with open(file_path, 'a', encoding='utf-8') as f:
    #                 #     f.write(
    #                 #         f"\n\n--Structure for table: `{table_name}`\n\n{table_structure[table_name]}\n\n--Data for table: `{table_name}`\n\n")
    #                 # 如果数据文件很大，考虑先追加结构，然后再追加数据
    #                 with open(file_path, 'r+', encoding='utf-8') as f:
    #                     data_content = f.readlines()
    #                     f.seek(0)
    #                     f.write(
    #                         f"--Structure for table: `{table_name}`\n\n{table_structure[table_name]}\n\n--Data for table: `{table_name}`\n\n")
    #                     for line in data_content:
    #                         f.write(line)
