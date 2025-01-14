import sqlite3
import os
import sys
import re
import tempfile

from PyQt6 import QtCore
from PyQt6.QtCore import Qt, pyqtSlot, pyqtSignal, QUrl
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import (QWidget, QTreeWidget, QTreeWidgetItem, QHBoxLayout, QVBoxLayout, QLabel,
                             QPushButton, QLineEdit, QGridLayout, QTableWidget, QTableWidgetItem,
                             QHeaderView, QApplication, QSplitter, QMessageBox, QComboBox, QDialog)

from maincode.mysql_v4 import MysqlV4
from maincode.zhongjianclass import Singleton
from maincode.mysql_v8 import MysqlV8


class AppWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.treeWidget = None
        self.db_connections = {}  # 用于存储数据库连接

        # 当前页、每页大小和总页数用于分页
        self.current_page = 1
        self.page_size = 20  # 每页行数
        self.total_pages = 0  # 总页数
        self.initUI()

    def post_init_setup(self, sqlite_db_paths):
        # 接受一个SQLite数据库路径列表
        if sqlite_db_paths:
            for db_path in sqlite_db_paths:
                self.connect_to_sqlite(db_path)
            if self.db_connections:
                self.load_sqlite_dbs_and_tables()
            else:
                self.show_empty_data_indicator()
        else:
            self.show_empty_data_indicator()

    def show_empty_data_indicator(self):
        # 清除treeWidget和tableWidget
        self.treeWidget.clear()
        self.tableWidget.setRowCount(0)
        self.tableWidget.setColumnCount(0)
        # 可选：在treeWidget中添加一个提示项
        item = QTreeWidgetItem(["没有连接到数据库或数据库为空"])
        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsSelectable)  # 设置为不可选择
        self.treeWidget.addTopLevelItem(item)
        # 可选：在tableWidget中添加提示信息
        self.tableWidget.setColumnCount(1)  # 设置单列
        self.tableWidget.setHorizontalHeaderLabels(["提示信息"])
        self.tableWidget.setRowCount(1)  # 设置单行
        self.tableWidget.setItem(0, 0, QTableWidgetItem("无可显示的数据"))
        self.tableWidget.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)  # 调整列宽


    def connect_to_sqlite(self, db_path):
        try:
            connection = sqlite3.connect(db_path)
            db_name = os.path.basename(db_path)  # 使用文件名作为数据库名
            self.db_connections[db_name] = connection
        except sqlite3.Error as e:
            print(f"连接到SQLite数据库时出错：{e}")

    def initUI(self):
        # 创建主布局
        layout = QHBoxLayout()

        # 创建QSplitter，设置为水平分割
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 使用QTreeWidget代替QListWidget
        self.treeWidget = QTreeWidget()
        self.treeWidget.setHeaderLabels(["数据库和表"])
        self.treeWidget.setColumnCount(1)

        # 表格和分页控件的垂直布局
        tableLayoutWidget = QWidget()  # 新建一个QWidget作为布局的容器
        tableLayout = QVBoxLayout(tableLayoutWidget)  # 将布局应用到容器QWidget上
        self.tableWidget = QTableWidget()  # 创建表格控件
        tableLayout.addWidget(self.tableWidget)

        # 分页控件
        self.setup_pagination_controls()

        # 将分页布局添加到表格布局中
        tableLayout.addLayout(self.paginationLayout)

        # 将树视图和表格布局容器添加到splitter
        splitter.addWidget(self.treeWidget)
        splitter.addWidget(tableLayoutWidget)
        # 设置splitter的初始比例
        splitter.setStretchFactor(0, 2)  # 树视图初始比例
        splitter.setStretchFactor(1, 3)  # 表格视图初始比例

        # 将splitter添加到主布局中
        layout.addWidget(splitter)
        self.setLayout(layout)

        # 连接事件
        self.treeWidget.itemClicked.connect(self.on_item_selected)
        self.prevButton.clicked.connect(self.load_previous_page)
        self.nextButton.clicked.connect(self.load_next_page)
        self.firstPageButton.clicked.connect(self.load_first_page)
        self.lastPageButton.clicked.connect(self.load_last_page)
        self.gotoButton.clicked.connect(self.goto_page)
        self.pageSizeComboBox.currentTextChanged.connect(self.on_page_size_change)

    def setup_pagination_controls(self):
        # 分页控件
        self.pageLabel = QLabel("第 1 页 / 共 0 页")
        self.firstPageButton = QPushButton("第一页")
        self.prevButton = QPushButton("上一页")
        self.nextButton = QPushButton("下一页")
        self.lastPageButton = QPushButton("最后一页")
        self.gotoLineEdit = QLineEdit()  # 跳转到某页
        self.gotoButton = QPushButton("跳转到")

        # 新增：每页显示数量的下拉框
        self.pageSizeComboBox = QComboBox()
        self.pageSizeComboBox.setEditable(True)  # 允许用户输入
        # 添加预定义的选项
        self.pageSizeComboBox.addItems(["5条/页", "10条/页", "15条/页", "20条/页", "30条/页"])
        self.pageSizeComboBox.setCurrentText(str(self.page_size))  # 设置当前显示为默认的每页行数
        # 连接信号


        # 分页布局
        self.paginationLayout = QGridLayout()
        self.paginationLayout.addWidget(self.firstPageButton, 0, 0)
        self.paginationLayout.addWidget(self.prevButton, 0, 1)
        self.paginationLayout.addWidget(self.nextButton, 0, 2)
        self.paginationLayout.addWidget(self.lastPageButton, 0, 3)
        self.paginationLayout.addWidget(self.pageSizeComboBox, 0, 4)
        self.paginationLayout.addWidget(self.pageLabel, 0, 5)
        self.paginationLayout.addWidget(self.gotoLineEdit, 0, 6)
        self.paginationLayout.addWidget(self.gotoButton, 0, 7)


    def load_sqlite_dbs_and_tables(self):
        # 清除任何现有项
        self.treeWidget.clear()
        for db_name, connection in self.db_connections.items():
            db_item = QTreeWidgetItem(self.treeWidget, [db_name])
            db_item.setFlags(db_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            db_item.setCheckState(0, Qt.CheckState.Unchecked)
            cursor = connection.cursor()
            # SQLite将表信息存储在sqlite_master中
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            for table_name in tables:
                table_name_str = table_name[0]
                table_item = QTreeWidgetItem(db_item, [table_name_str])
                # 如果需要，为表项添加复选框
                table_item.setFlags(table_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                table_item.setCheckState(0, Qt.CheckState.Unchecked)  # 默认未选中

    def on_page_size_change(self):
        # 获取当前下拉框的文本
        current_text = self.pageSizeComboBox.currentText()

        # 尝试解析“X条/页”格式的选项
        try:
            # 如果用户选择的是预定义选项，如"10条/页"
            if "条/页" in current_text:
                new_page_size = int(current_text.split("条/页")[0])
            else:
                # 否则，假设用户输入了一个数字
                new_page_size = int(current_text)

            # 检查新的每页条目数是否有效
            if new_page_size > 0:
                self.page_size = new_page_size
                # if self.
                self.calculate_total_pages()
                self.load_first_page()  # 重新加载第一页
            else:
                self.show_message_box("请输入有效的每页条目数")
        except ValueError:
            self.show_message_box("请输入有效的数字")
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

    def show_hyperlink_dialog(self):
        # 创建一个对话框
        dialog = QDialog()
        dialog.setWindowTitle("提示信息")
        dialog.setFixedSize(400, 200)

        # 创建垂直布局
        layout = QVBoxLayout()

        # 创建一个带超链接的标签
        label = QLabel('当前不支持大字段表解析，如有需要请联系：<a href="https://www.frombyte.com">北亚数据恢复中心</a>')
        label.setOpenExternalLinks(False)  # 禁用自动打开链接
        label.linkActivated.connect(lambda link: QDesktopServices.openUrl(QUrl(link)))  # 手动处理链接点击
        label.setTextFormat(Qt.TextFormat.RichText)  # 启用富文本格式
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # 创建一个关闭按钮
        close_button = QPushButton("关闭")
        close_button.clicked.connect(dialog.close)

        # 将组件添加到布局中
        layout.addWidget(label)
        layout.addWidget(close_button)

        # 设置对话框的布局
        dialog.setLayout(layout)

        # 显示对话框
        dialog.exec()

    def find_table_schema(self,all_table_schema,table_name):
        in_table = False
        all_table_schema = all_table_schema.getvalue()
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', encoding='utf-8')
        found = False
        for line in all_table_schema.split('\n'):
            # 检查是否是目标表的开始
            if re.match(f"CREATE TABLE `\\b{table_name}\\b`", line, re.IGNORECASE):
                in_table = True
                found = True
            # 如果在目标表内部，收集表结构信息
            if in_table:
                temp_file.write(line+'\n')
                # 如果遇到了语句结束的分号，说明表结构定义结束
                if ';' in line:
                    break
        temp_file.seek(0)
        # aaaadsaf = temp_file.read()

        if not found:
            # 如果没有找到表结构，关闭并删除临时文件
            temp_file.close()
            os.unlink(temp_file.name)
            return None
        return temp_file

    def on_item_selected(self, item, column):
        # 检查是否点击了数据库项
        if item.parent() is None:  # 点击了数据库
            # 根据数据库项的选中状态更新所有表的选中状态
            self.update_tables_check_state(item, item.checkState(0))
            print(f"数据库 {item.text(0)} 状态改变")
        else:  # 点击了表
            db_name = item.parent().text(0)
            table_name = item.text(0)
            # 获取表结构，ibd数据等信息
            singleton = Singleton()
            self.mysql_con = singleton.my_conn
            # 根据表结构，判断那些表里有大字段
            large_fields_tablename = self.extract_table_names_with_large_fields(self.mysql_con['table_struct_str'])
            # singleton.large_fields_tablename = large_fields_tablename
            if table_name in large_fields_tablename :
                self.show_hyperlink_dialog()
            else:
                self.current_page = 1
                self.table_name = table_name
                sqlite_conn = sqlite3.connect(fr'../tempdata/{db_name}')
                sqlite_cursor = sqlite_conn.cursor()
                # 检查表中是否有数据
                sqlite_cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} LIMIT 1);")
                if sqlite_cursor.fetchone()[0]:
                    sqlite_conn.close()
                    self.db_connection = self.db_connections[db_name]
                    self.calculate_total_pages()
                    self.load_table_data()

                else:
                    sqlite_conn.close()
                   #在此处点击了哪个表，向哪个表里插入数据。然后再读取
                    if self.mysql_con['mysql_v'] == 8:
                        processor = MysqlV8()
                    elif self.mysql_con['mysql_v'] == 5:
                        processor = MysqlV4()
                    else:
                        processor = None

                    # 获取该表的表结构
                    struct_str = self.find_table_schema(self.mysql_con['table_struct_str'],table_name)
                    row_dir = None
                    page_file_dir = fr"{self.mysql_con['dir']}"
                    # if  db_name.split('_')[-1].split('.')[0] == 'data':     # 提取最后一个'_'后的内容，去掉扩展名
                    is_del = True
                    processor.process(struct_str, row_dir, page_file_dir, is_del, None,rf"../tempdata/{self.mysql_con['filename']}_data.db")
                    # elif db_name.split('_')[-1].split('.')[0] == 'del':
                    #     is_del = True
                    #     processor.process(struct_str, row_dir, page_file_dir, is_del, None,rf"../tempdata/{self.mysql_con['filename']}_del.db")

                    struct_str.close()
                    self.db_connection = self.db_connections[db_name]
                    self.calculate_total_pages()
                    self.load_table_data()


    def update_tables_check_state(self, db_item, checkState):
        # 遍历数据库下的所有表
        child_count = db_item.childCount()
        for i in range(child_count):
            table_item = db_item.child(i)
            # 确保该项可以被选中
            table_item.setFlags(table_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            # 根据数据库项的选中状态更新表项的选中状态
            table_item.setCheckState(0, checkState)

    def calculate_total_pages(self):

        cursor = self.db_connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            total_rows = cursor.fetchone()[0]
            self.total_pages = (total_rows + self.page_size - 1) // self.page_size
        except sqlite3.Error as e:
            print(f"计算总页数时出错: {e}")
            self.total_pages = 0

    def load_table_data(self):
        offset = (self.current_page - 1) * self.page_size
        query = f"SELECT * FROM {self.table_name} LIMIT {self.page_size} OFFSET {offset}"
        cursor = self.db_connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            # 获取列名
            col_names = [description[0] for description in cursor.description]
            col_count = len(col_names)
            self.tableWidget.setColumnCount(col_count)
            self.tableWidget.setRowCount(len(rows))
            self.tableWidget.setHorizontalHeaderLabels(col_names)
            for row_num, row_data in enumerate(rows):
                for col_num, col_data in enumerate(row_data):
                    self.tableWidget.setItem(row_num, col_num, QTableWidgetItem(str(col_data)))
            self.update_page_label()
        except sqlite3.Error as e:
            print(f"加载表数据时出错: {e}")
            self.tableWidget.setRowCount(0)
            self.tableWidget.setColumnCount(0)

    def load_previous_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.load_table_data()
        else:
            self.show_message_box("已经是第一页了")

    def load_next_page(self):
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.load_table_data()
        else:
            self.show_message_box("没有更多数据了")

    def load_first_page(self):
        if self.total_pages > 0:
            self.current_page = 1
            self.load_table_data()
        else:
            self.show_message_box("没有可显示的数据")

    def load_last_page(self):
        if self.total_pages > 0:
            self.current_page = self.total_pages
            self.load_table_data()
        else:
            self.show_message_box("没有可显示的数据")

    def goto_page(self):
        try:
            page = int(self.gotoLineEdit.text())
            if 0 < page <= self.total_pages:
                self.current_page = page
                self.load_table_data()
            else:
                self.show_message_box("请输入有效的页码")
        except ValueError:
            self.show_message_box("请输入有效的页码")

    def show_message_box(self, message):
        msgBox = QMessageBox()
        msgBox.setIcon(QMessageBox.Icon.Information)
        msgBox.setText(message)
        msgBox.setWindowTitle("提示")
        msgBox.setStandardButtons(QMessageBox.StandardButton.Ok)
        msgBox.exec()

    def update_page_label(self):
        self.pageLabel.setText(f"第 {self.current_page} 页 / 共 {self.total_pages} 页")

    @pyqtSlot()
    def close_all_connections(self):
        for connection in self.db_connections.values():
            connection.close()
        self.db_connections.clear()

