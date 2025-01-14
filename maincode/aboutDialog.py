import sys
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QDesktopServices, QPalette, QColor
from PyQt6.QtWidgets import QApplication, QDialog, QVBoxLayout, QLabel, QPushButton

class AboutDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Frombyte MySql Recovery v1.0")
        self.initUI()

    def initUI(self):
        # 设置对话框的大小
        self.resize(310, 200)

        # 使用垂直布局
        layout = QVBoxLayout()

        # 创建并配置标签，使用HTML格式的文本
        label = QLabel()
        label.setTextFormat(Qt.TextFormat.RichText)  # 设置文本格式为富文本
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextBrowserInteraction)  # 允许交互，如点击链接
        label.setOpenExternalLinks(True)  # 允许打开外部链接
        label.setText("""
            北亚数据恢复中心出品    <a href="https://www.frombyte.com">https://www.frombyte.com</a><br>
            联系地址：北京市海淀区东北旺西路中关村软件园8号楼华夏科技大厦309A<br>
            联系电话：4006-505-646<br>
            联系邮箱：<a href="mailto:user@frombyte.com">user@frombyte.com</a><br>
            本软件暂不支持MySQL5.5及以下版本恢复<br>
            本软件仅用于合法合规的数据库恢复操作<br>
            本软件禁止用于非法用途
        """)

        # 添加标签到布局
        layout.addWidget(label)

        # 创建关闭按钮
        closeButton = QPushButton("关闭")
        closeButton.clicked.connect(self.close)  # 关闭对话框
        layout.addWidget(closeButton)

        # 设置对话框的布局
        self.setLayout(layout)

        # 美化样式
        self.setStyleSheet("""
            QDialog {
                background-color: #f0f0f0;
            }
            QLabel {
                color: #333;
                font-family: Arial, sans-serif;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton {
                background-color: #969696;
                color: white;
                padding: 5px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #0053a4;
            }
        """)


