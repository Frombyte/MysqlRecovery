import os.path
import sys
import mysql.connector
from PyQt6.QtWidgets import QGridLayout, QDialog, QLineEdit, QPushButton, QLabel, QFileDialog, QMessageBox, \
    QProgressDialog
from PyQt6.QtCore import Qt, pyqtSignal, QThread

from maincode.mysqlFrm import MysqlFrm
from maincode.mysql_v4 import MysqlV4
from maincode.createSqlite import SQLConverter
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
        self.setWindowTitle("恢复MySQL5.X，请输入信息")
        self.setGeometry(0, 0, 600, 300)
        # self.db_manager = MyMainWindow()
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
        self.dirLineEdit.setPlaceholderText("包含.frm与.ibd文件")
        browseDirButton = QPushButton("...")
        browseDirButton.clicked.connect(self.browse_directory)
        layout.addWidget(QLabel("库文件目录:"), 1, 0)
        layout.addWidget(self.dirLineEdit, 1, 1)
        layout.addWidget(browseDirButton, 1, 2)

        self.hostLineEdit = QLineEdit()
        self.hostLineEdit.setText("127.0.0.1")  # 设置默认主机地址
        layout.addWidget(QLabel("主机:"), 2, 0)
        layout.addWidget(self.hostLineEdit, 2, 1)

        self.portLineEdit = QLineEdit()
        self.portLineEdit.setText("3306")  # 设置默认端口
        layout.addWidget(QLabel("端口:"), 3, 0)
        layout.addWidget(self.portLineEdit, 3, 1)

        self.userLineEdit = QLineEdit()
        self.userLineEdit.setText("root")  # 设置默认用户名
        layout.addWidget(QLabel("用户名:"), 4, 0)
        layout.addWidget(self.userLineEdit, 4, 1)

        self.passwordLineEdit = QLineEdit()
        # self.passwordLineEdit.setText("Frombyte505646.")
        self.passwordLineEdit.setEchoMode(QLineEdit.EchoMode.Password)
        layout.addWidget(QLabel("密码:"), 5, 0)
        layout.addWidget(self.passwordLineEdit, 5, 1)

        # 确认按钮
        self.okButton = QPushButton("确定")
        self.okButton.clicked.connect(self.on_ok_clicked)
        layout.addWidget(self.okButton, 6, 1, 1, 1, Qt.AlignmentFlag.AlignVCenter)

        self.testButton = QPushButton('测试连接')
        self.testButton.clicked.connect(self.check_connection)
        layout.addWidget(self.testButton, 6, 0, 1, 1, Qt.AlignmentFlag.AlignVCenter)

        # 默认禁用确定按钮
        self.okButton.setEnabled(False)

    def init_connections(self):
        # 绑定文本变化信号以检查输入
        self.dirLineEdit.textChanged.connect(self.check_inputs)
        self.hostLineEdit.textChanged.connect(self.check_inputs)
        self.portLineEdit.textChanged.connect(self.check_inputs)
        self.userLineEdit.textChanged.connect(self.check_inputs)
        self.passwordLineEdit.textChanged.connect(self.check_inputs)

    def check_inputs(self):
        # 检查必要的文本框是否都不为空
        is_all_filled = all(
            [self.dirLineEdit.text(), self.hostLineEdit.text(), self.portLineEdit.text(), self.userLineEdit.text(),
             self.passwordLineEdit.text()])
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

    def conn(self,con):
        for connection in con.values():
            connection.close()
        con.clear()

    def check_connection(self):
        try:
            cnx = mysql.connector.connect(user=self.userLineEdit.text(), password=self.passwordLineEdit.text(),
                                          host=self.hostLineEdit.text())
            cnx.close()
            QMessageBox.information(self, '连接测试', '连接成功')
        except mysql.connector.Error as err:
            QMessageBox.critical(self, '连接测试', '连接失败: {}'.format(err))

    def on_ok_clicked(self):

        self.mysql_config['dir'] = self.dirLineEdit.text()
        self.mysql_config['struct'] = self.structLineEdit.text()
        self.mysql_config['host'] = self.hostLineEdit.text()
        self.mysql_config['port'] = int(self.portLineEdit.text())
        self.mysql_config['user'] = self.userLineEdit.text()
        self.mysql_config['password'] = self.passwordLineEdit.text()
        self.mysql_config['mysql_v'] = 5
        self.mysql_config['filename'] = os.path.basename(self.mysql_config['dir'])
        for key, value in self.mysql_config.items():
            print(f"{key}: {value}")

        # 判断是否存在.frm文件
        if self.mysql_config['struct'] == '' and not any(
                filename.endswith('.frm') for filename in os.listdir(self.mysql_config['dir'])):
            # 弹出提示框，提醒用户添加.frm文件或者表结构文件
            msg = QMessageBox(self)
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setWindowTitle("文件缺失")
            msg.setText("没有.frm文件，请添加，或添加表结构文件。")
            msg.setStandardButtons(QMessageBox.StandardButton.Ok)
            msg.exec()
            return  # 中断后续操作


            # 第二次颠倒恢复同样的库时，删除原有库
        if any(os.scandir(r'../tempdata')):
            extracted_names = set()
            for filename in os.listdir(r'../tempdata'):
                if filename.endswith('.db'):
                    name_parts = filename.rsplit('.',1)[0].rsplit('_',1)
                    if len(name_parts)>1:
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
                    print('删除成功')

        # 初始化进度对话框
        progressDialog = QProgressDialog("正在恢复中...", "取消", 0, 0, self)
        progressDialog.setCancelButton(None)  # 移除取消按钮
        progressDialog.setModal(True)
        progressDialog.setWindowTitle("正在恢复")
        progressDialog.show()
        def long_running():
            print("=============2=============")
            if self.mysql_config['struct'] == '':

                converter = MysqlFrm(self.mysql_config['user'], self.mysql_config['password'],
                                     self.mysql_config['port'], self.mysql_config['dir'])
                struct_str = converter.convert_to_sql()
                self.mysql_config['table_struct_str'] = struct_str

                singleton = Singleton()
                singleton.my_conn = self.mysql_config

                aaaa = struct_str.getvalue()
                sqlconverter_data = SQLConverter(rf"../tempdata/{self.mysql_config['filename']}_data.db")
                sqlconverter_data.convert_sql_file_to_db(struct_str)
                # sqlconverter_del = SQLConverter(rf"../tempdata/{self.mysql_config['filename']}_del.db")
                # sqlconverter_del.convert_sql_file_to_db(struct_str)
                # processor = MysqlV4()
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
                # processor = MysqlV4()
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