import os.path
import re
import sqlite3
import sys
import threading

from PyQt6.QtCore import Qt, pyqtSignal, QUrl, QThread
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import QMainWindow, QApplication, QStatusBar, QFileDialog, QMessageBox, QDialog, QVBoxLayout, \
    QLabel

from maincode import centerWidget, mysql5Dialog, mysql8Dialog, aboutDialog
from maincode.mysqlUi import Ui_MainWindow
from maincode.saveAllTables import SaveAllTables
from maincode.selectedTables import SelectedTables
from maincode.zhongjianclass import Singleton


class ProgressDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("提示")
        self.setModal(True)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, False)
        self.setStyleSheet("background-color: #f0f0f0; color: #f2711c; font-size: 20px;")  # 设置弹窗的样式

        layout = QVBoxLayout(self)
        self.label = QLabel("正在保存中，请稍等....", self)
        self.label.setAlignment(Qt.AlignmentFlag.AlignCenter)  # 设置标签的对齐方式为居中
        layout.addWidget(self.label)


class MyMainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, parent=None):
        super(MyMainWindow, self).__init__(parent)
        self.tempDir = "../tempdata"  # 临时文件夹名称

        # 创建临时文件夹
        if not os.path.exists(self.tempDir):
            os.makedirs(self.tempDir)
        self.setupUi(self)
        self.center_widget = centerWidget.AppWidget()
        self.setCentralWidget(self.center_widget)
        self.center_widget.show_empty_data_indicator()

        self.recoverMysql5.triggered.connect(self.open5Dialog)
        self.recoverMysql8.triggered.connect(self.open8Dialog)
        self.saveAll.triggered.connect(self.get_all_tables)
        self.saveSelection.triggered.connect(self.get_selected_tables)
        self.actionabout.triggered.connect(self.openAbout)
        self.actionhelp.triggered.connect(self.openHelp)

    def confirm_restore_data(self):
        # 创建一个消息框
        msg_box = QMessageBox()
        msg_box.setIcon(QMessageBox.Icon.Question)
        msg_box.setWindowTitle("恢复数据")
        msg_box.setText("是否恢复删除数据？")
        msg_box.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        msg_box.setDefaultButton(QMessageBox.StandardButton.No)

        # 显示消息框，并等待用户响应
        response = msg_box.exec()

        # 根据用户的选择返回布尔值
        return response == QMessageBox.StandardButton.Yes

    def get_selected_tables(self):
        # # 初始化工作线程但不启动
        # self.workerThread = WorkerThread(self.center_widget, self.my_c)
        # self.workerThread.finished.connect(self.onThreadFinished)
        # self.workerThread.showMessage.connect(self.statusBar().showMessage)
        #
        # dirName = QFileDialog.getExistingDirectory(self, "选择一个保存目录")
        # if dirName:
        #     self.workerThread.dirName = dirName
        #     if not self.workerThread.isRunning():  # 检查线程是否已经在运行
        #         self.workerThread.start()  # 启动线程
        # else:
        #     self.show_message_box("未选择保存目录")
        selected_tables = {}
        for i in range(self.center_widget.treeWidget.topLevelItemCount()):
            db_item = self.center_widget.treeWidget.topLevelItem(i)
            db_name = db_item.text(0)
            selected_tables[db_name] = []
            for j in range(db_item.childCount()):
                table_item = db_item.child(j)
                if table_item.checkState(0) == Qt.CheckState.Checked:
                    selected_tables[db_name].append(table_item.text(0))
        selected_tables = {db: tables for db, tables in selected_tables.items() if tables}

        if selected_tables:
            self.is_recout_selected = self.confirm_restore_data()
            self.selectedTables = SelectedTables(self.center_widget,self.my_c,self.is_recout_selected)
            self.selectedTables.finished.connect(self.onThreadFinished)

            dirName = QFileDialog.getExistingDirectory(self,"选择一个保存目录")
            if dirName:
                self.dialog = ProgressDialog(self)
                self.dialog.show()
                self.selectedTables.dirName = dirName
                if not self.selectedTables.isRunning():
                    self.selectedTables.start()
            else:
                self.show_message_box("未选择保存目录")
        else:
            self.show_message_box("未选择任何数据库或表")

    def get_all_tables(self):
        all_tables = {}
        for i in range(self.center_widget.treeWidget.topLevelItemCount()):
            db_item = self.center_widget.treeWidget.topLevelItem(i)
            db_name = db_item.text(0)
            all_tables[db_name] = [db_item.child(j).text(0) for j in range(db_item.childCount())]

        if all_tables != {'没有连接到数据库或数据库为空': []}:
            self.is_recout_all = self.confirm_restore_data()
            self.savealltables = SaveAllTables(self.center_widget,self.my_c,self.is_recout_all)
            self.savealltables.finished.connect(self.onThreadFinished)

            dirName = QFileDialog.getExistingDirectory(self,"选择一个保存目录")

            if dirName:
                self.dialog = ProgressDialog(self)
                self.dialog.show()
                self.savealltables.dirName = dirName
                if not self.savealltables.isRunning():
                    self.savealltables.start()
            else:
                self.show_message_box("未选择保存目录")
        else:
            self.show_message_box("未选择任何数据库或表")


    def onThreadFinished(self):
        self.dialog.close()
        self.show_message_box("保存完成")

    # def get_selected_tables(self):
    #     selected_tables = {}
    #     for i in range(self.center_widget.treeWidget.topLevelItemCount()):
    #         db_item = self.center_widget.treeWidget.topLevelItem(i)
    #         db_name = db_item.text(0)
    #         selected_tables[db_name] = []
    #         for j in range(db_item.childCount()):
    #             table_item = db_item.child(j)
    #             if table_item.checkState(0) == Qt.CheckState.Checked:
    #                 selected_tables[db_name].append(table_item.text(0))
    #     # 过滤掉没有表被选中的数据库
    #     selected_tables = {db: tables for db, tables in selected_tables.items() if tables}
    #
    #     if selected_tables:
    #         dirName = QFileDialog.getExistingDirectory(self, "选择一个保存目录")
    #         self.statusBar().showMessage("正在保存中，请稍候...")
    #         for db_name in selected_tables.keys():
    #             folder_name = db_name.replace('.db', '')
    #             folder_path = os.path.join(dirName, folder_name)
    #             if not os.path.exists(folder_path):
    #                 os.makedirs(folder_path)
    #                 struct_str = self.my_c['table_struct_str']
    #                 if self.my_c['mysql_v'] == 5:
    #                     processor = MysqlV4()
    #                 elif self.my_c['mysql_v'] == 8:
    #                     processor = MysqlV8()
    #                 row_dir = folder_path
    #                 page_file_dir = fr"{self.my_c['dir']}"
    #                 if folder_name.split('_')[-1] == 'del':
    #                     is_del = True
    #                     processor.process(struct_str, row_dir, page_file_dir, is_del,selected_tables[db_name],None)
    #                 if folder_name.split('_')[-1] == 'data':
    #                     is_del = False
    #                     processor.process(struct_str, row_dir, page_file_dir, is_del,selected_tables[db_name],None)
    #             self.insertStructure(folder_path)
    #         self.statusBar().showMessage("保存完成！！！", 5000)
    #     else:
    #         self.show_message_box("未选择任何数据库或表")



    # def get_all_tables(self):
    #     all_tables = {}
    #     for i in range(self.center_widget.treeWidget.topLevelItemCount()):
    #         db_item = self.center_widget.treeWidget.topLevelItem(i)
    #         db_name = db_item.text(0)
    #         all_tables[db_name] = [db_item.child(j).text(0) for j in range(db_item.childCount())]
    #
    #     if all_tables != {'没有连接到数据库或数据库为空': []}:
    #         dirName = QFileDialog.getExistingDirectory(self, "选择一个保存目录")
    #         self.statusBar().showMessage("正在保存中，请稍候...")
    #         for db_name in all_tables.keys():
    #             folder_name = db_name.replace('.db', '')
    #             folder_path = os.path.join(dirName, folder_name)
    #             if not os.path.exists(folder_path):
    #                 os.makedirs(folder_path)
    #                 struct_str = self.my_c['table_struct_str']
    #                 if self.my_c['mysql_v'] == 5:
    #                     processor = MysqlV4()
    #                 elif self.my_c['mysql_v'] == 8:
    #                     processor = MysqlV8()
    #                 row_dir = folder_path
    #                 page_file_dir = fr"{self.my_c['dir']}"
    #                 if folder_name.split('_')[-1] == 'del':
    #                     is_del = True
    #                     processor.process(struct_str, row_dir, page_file_dir, is_del, all_tables[db_name], None)
    #                 if folder_name.split('_')[-1] == 'data':
    #                     is_del = False
    #                     processor.process(struct_str, row_dir, page_file_dir, is_del, all_tables[db_name], None)
    #             self.insertStructure(folder_path)
    #         self.statusBar().showMessage("保存完成！！！", 5000)
    #     else:
    #         self.show_message_box("未选择任何数据库或表")

    def show_message_box(self, message):
        msgBox = QMessageBox()
        msgBox.setIcon(QMessageBox.Icon.Information)
        msgBox.setText(message)
        msgBox.setWindowTitle("提示")
        msgBox.setStandardButtons(QMessageBox.StandardButton.Ok)
        msgBox.exec()



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

    def open5Dialog(self):
        self.popup5Dialog = mysql5Dialog.PopupDialog(self)
        self.popup5Dialog.data_signal.connect(self.updatePage)
        self.popup5Dialog.show()
        if self.popup5Dialog.exec():
            print("弹窗关闭")

    def open8Dialog(self):
        self.popup8Dialog = mysql8Dialog.PopupDialog(self)
        self.popup8Dialog.data_signal.connect(self.updatePage)
        self.popup8Dialog.show()
        if self.popup8Dialog.exec():
            print("弹窗关闭")

    def updatePage(self,my_c):
        self.my_c = my_c
        self.setCentralWidget(None)  # 先解除原来 widget 的引用
        self.center_widget.deleteLater()
        self.center_widget = centerWidget.AppWidget()
        self.setCentralWidget(self.center_widget)
        self.center_widget.post_init_setup([rf"../tempdata/{my_c['filename']}_data.db"])
        self.db_conn = self.center_widget.db_connections
        singleton = Singleton()
        singleton.d_co = self.db_conn


    def openAbout(self):
        self.openAbout = aboutDialog.AboutDialog(self)
        self.openAbout.show()
        if self.openAbout.exec():
            print("弹窗关闭")

    def openHelp(self):
        url = QUrl("https://www.frombyte.com/index.php?m=&c=index&a=lists&catid=6")
        QDesktopServices.openUrl(url)


    def delete_folder_contents(self,folder_path):
        # 遍历文件夹中的所有文件
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            # 检查文件是否是SQLite数据库文件（通过扩展名或其他方式）
            if filename.endswith('.db'):
                try:
                    # 尝试连接数据库，并立即关闭连接
                    # 这里的目的是确保所有挂起的事务都被提交或回滚，以便安全删除
                    conn = sqlite3.connect(file_path)
                    conn.close()
                except sqlite3.Error as e:
                    print(f"Error closing SQLite database connection: {e}")
                # 在所有连接都关闭后删除文件
                try:
                    os.remove(file_path)
                    print(f"Deleted database file: {filename}")
                except OSError as e:
                    print(f"Error deleting file {filename}: {e}")
            else:
                # 如果文件不是数据库文件，直接删除
                os.remove(file_path)

    def closeEvent(self, event):
        self.center_widget.close_all_connections()
        def background_task():
            # 删除文件夹内容
            self.delete_folder_contents(r'..\tempdata')
            # 窗口关闭时删除临时文件夹
            if os.path.exists(self.tempDir):
                os.rmdir(self.tempDir)

        thread = threading.Thread(target=background_task)
        thread.start()  # 启动线程
        event.accept()  # 确认关闭事件

if __name__ == "__main__":
    app = QApplication(sys.argv)
    my_win = MyMainWindow()
    mySta : QStatusBar = my_win.statusBar()

    my_win.show()
    sys.exit(app.exec())
