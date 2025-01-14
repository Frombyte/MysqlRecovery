import os.path
import sys
from PyQt6.QtWidgets import QGridLayout, QDialog, QLineEdit, QPushButton, QLabel, QFileDialog, QMessageBox, \
    QProgressDialog
from PyQt6.QtCore import Qt, pyqtSignal, QThread

from maincode.get_sdi_info import IbdToSql
from maincode.createSqlite import SQLConverter
from maincode.mysql_v8 import MysqlV8
from maincode.zhongjianclass import Singleton


class Worker(QThread):
    finished = pyqtSignal()

    def __init__(self, func):
        super().__init__()
        self.func = func

    def run(self):
        self.func()
        self.finished.emit()

class PopupDialog(QDialog):
    # 定义一个信号，将会携带一个字典数据
    data_signal = pyqtSignal(dict)
    close_dialog = pyqtSignal()
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("恢复MySQL8.X，请输入信息")
        self.setGeometry(0, 0, 600, 100)
        self.mysql_config = {}
        self.setStyleSheet("""
               QDialog { background-color: #f0f0f0; }
               QPushButton { background-color: #c1c1c1; color: black; border-radius: 5px; padding: 5px 15px; }
               QPushButton:hover { background-color: #0053a4; }
               QLineEdit { border: 1px solid #ccc; border-radius: 5px; padding: 5px; }
               QPushButton:disabled { background-color: #e0e0e0; color: #a0a0a0; }  # 禁用状态的样式   
               QLabel { color: #333; }
               QLineEdit::placeholder { color: #aaa; }
           """)
        if parent:
            self.center(parent)
        layout = QGridLayout()

        # 初始化UI组件
        self.init_ui(layout)

        # 设置布局
        self.setLayout(layout)

        # 初始化连接
        self.init_connections()



    def init_ui(self, layout):
        # 表结构文件
        self.structLineEdit = QLineEdit()
        self.structLineEdit.setPlaceholderText("如果有，优先修复表结构文件中的表")
        browseStructButton = QPushButton("...")
        browseStructButton.clicked.connect(self.browse_structure_file)
        layout.addWidget(QLabel("表结构文件:"), 0, 0)
        layout.addWidget(self.structLineEdit, 0, 1)
        layout.addWidget(browseStructButton, 0, 2)

        # 库文件目录
        self.dirLineEdit = QLineEdit()
        self.dirLineEdit.setPlaceholderText("包含.ibd文件")
        browseDirButton = QPushButton("...")
        browseDirButton.clicked.connect(self.browse_directory)
        layout.addWidget(QLabel("库文件目录:"), 1, 0)
        layout.addWidget(self.dirLineEdit, 1, 1)
        layout.addWidget(browseDirButton, 1, 2)
        # 确认按钮
        self.okButton = QPushButton("确定")
        self.okButton.clicked.connect(self.on_ok_clicked)
        layout.addWidget(self.okButton, 3, 1, 1, 2, Qt.AlignmentFlag.AlignVCenter)

        # 默认禁用确定按钮
        self.okButton.setEnabled(False)

    def init_connections(self):
        # 绑定文本变化信号以检查输入
        self.dirLineEdit.textChanged.connect(self.check_inputs)
    def check_inputs(self):
        # 检查必要的文本框是否都不为空
        is_all_filled = all(
            [self.dirLineEdit.text()])
        self.okButton.setEnabled(is_all_filled)

    def browse_directory(self):
        dir_path = QFileDialog.getExistingDirectory(self, "选择目录")
        if dir_path:
            self.dirLineEdit.setText(dir_path)

    def browse_structure_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "选择文件", "", "所有文件 (*)")
        if file_path:
            self.structLineEdit.setText(file_path)

    def center(self, parent):
        # 计算主窗口的中心点
        parentGeom = parent.frameGeometry()
        parentCenter = parentGeom.center()

        # 设置弹窗的几何形状，以便其中心与主窗口的中心对齐
        selfGeom = self.frameGeometry()
        selfGeom.moveCenter(parentCenter)
        self.move(selfGeom.topLeft().x(), selfGeom.topLeft().y())

    def conn(self, con):
        for connection in con.values():
            connection.close()
        con.clear()

    def on_ok_clicked(self):

        self.mysql_config['dir'] = self.dirLineEdit.text()
        self.mysql_config['struct'] = self.structLineEdit.text()
        self.mysql_config['mysql_v'] = 8
        self.mysql_config['filename'] = os.path.basename(self.mysql_config['dir'])
        for key, value in self.mysql_config.items():
            print(f"{key}: {value}")

        if any(os.scandir(r'../tempdata')):
            extracted_names = set()
            for filename in os.listdir(r'../tempdata'):
                if filename.endswith('.db'):
                    name_parts = filename.rsplit('.', 1)[0].rsplit('_', 1)
                    if len(name_parts) > 1:
                        extracted_names.add(name_parts[0])
                    else:
                        extracted_names.add(name_parts[0])
            extracted_names = list(extracted_names)
            for db_name in extracted_names:
                if db_name == self.mysql_config['filename']:
                    singleton = Singleton()
                    self.conn(singleton.d_co)
                    os.remove(rf"../tempdata/{db_name}_data.db")
                    # os.remove(rf"../tempdata/{db_name}_del.db")

        # 初始化进度对话框
        progressDialog = QProgressDialog("正在恢复中...", "取消", 0, 0, self)
        progressDialog.setCancelButton(None)  # 移除取消按钮
        progressDialog.setModal(True)
        progressDialog.setWindowTitle("正在恢复")
        progressDialog.show()

        def long_running():
            print("=============1=============")
            if self.mysql_config['struct'] == '':
                converter = IbdToSql()
                struct_str = converter.get_sdi_info(self.mysql_config['dir'])
                self.mysql_config['table_struct_str'] = struct_str
                singleton = Singleton()
                singleton.my_conn = self.mysql_config
                aaaa = struct_str.getvalue()
                sqlconverter_data = SQLConverter(rf"../tempdata/{self.mysql_config['filename']}_data.db")
                sqlconverter_data.convert_sql_file_to_db(struct_str)
                # sqlconverter_del = SQLConverter(rf"../tempdata/{self.mysql_config['filename']}_del.db")
                # sqlconverter_del.convert_sql_file_to_db(struct_str)
                # processor = MysqlV8()
                # row_dir = None
                # page_file_dir = fr"{self.mysql_config['dir']}"
                # is_del = True
                # processor.process(struct_str, row_dir, page_file_dir, is_del,None,rf"../tempdata/{self.mysql_config['filename']}_del.db")
                # is_del = False
                # processor.process(struct_str, row_dir, page_file_dir, is_del,None,rf"../tempdata/{self.mysql_config['filename']}_data.db")

                self.data_signal.emit(self.mysql_config)
            else:
                struct_path = fr"{self.mysql_config['struct']}"
                struct_str = open(struct_path, 'r', encoding='utf-8')
                self.mysql_config['table_struct_str'] = struct_str

                singleton = Singleton()
                singleton.my_conn = self.mysql_config

                sqlconverter_data = SQLConverter(rf"../tempdata/{self.mysql_config['filename']}_data.db")
                sqlconverter_data.convert_sql_file_to_db(struct_str)
                # sqlconverter_del = SQLConverter(rf"../tempdata/{self.mysql_config['filename']}_del.db")
                # sqlconverter_del.convert_sql_file_to_db(struct_str)
                # processor = MysqlV8()
                # row_dir = None
                # page_file_dir = fr"{self.mysql_config['dir']}"
                # is_del = True
                # processor.process(struct_str, row_dir, page_file_dir, is_del,None, rf"../tempdata/{self.mysql_config['filename']}_del.db")
                # is_del = False
                # processor.process(struct_str, row_dir, page_file_dir, is_del, None,rf"../tempdata/{self.mysql_config['filename']}_data.db")

                self.data_signal.emit(self.mysql_config)
            # 耗时操作完成后关闭进度对话框
            self.close_dialog.emit()
        self.worker = Worker(long_running)
        self.worker.finished.connect(progressDialog.close)
        self.worker.finished.connect(self.accept)
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker.start()