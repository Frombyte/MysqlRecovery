# Form implementation generated from reading ui file 'mysqlUi.ui'
#
# Created by: PyQt6 UI code generator 6.4.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.
import os
import sys

from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):

    def resource_path(self,relative_path):
        """获取资源的实际路径"""
        try:
            # PyInstaller 创建的临时文件夹的路径
            base_path = sys._MEIPASS
        except AttributeError:
            # 如果程序未被打包，则使用当前文件夹的路径
            base_path = os.path.abspath(".")

        return os.path.join(base_path, relative_path)

    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(1200, 789)
        MainWindow.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.NoContextMenu)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(self.resource_path(os.path.join("image", "mysql_ico.ico"))), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        MainWindow.setWindowIcon(icon)
        self.centralwidget = QtWidgets.QWidget(parent=MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(parent=MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.toolBar = QtWidgets.QToolBar(parent=MainWindow)
        self.toolBar.setEnabled(True)
        self.toolBar.setMouseTracking(False)
        self.toolBar.setContextMenuPolicy(QtCore.Qt.ContextMenuPolicy.NoContextMenu)
        self.toolBar.setIconSize(QtCore.QSize(30, 30))
        self.toolBar.setToolButtonStyle(QtCore.Qt.ToolButtonStyle.ToolButtonTextUnderIcon)
        self.toolBar.setStyleSheet("QToolButton{width: 60px; height: 50px;}")
        self.toolBar.setObjectName("toolBar")
        MainWindow.addToolBar(QtCore.Qt.ToolBarArea.TopToolBarArea, self.toolBar)
        self.recoverMysql5 = QtGui.QAction(parent=MainWindow)
        icon1 = QtGui.QIcon()
        icon1.addPixmap(QtGui.QPixmap(self.resource_path(os.path.join("image", "MySQL5.png"))), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        self.recoverMysql5.setIcon(icon1)
        self.recoverMysql5.setObjectName("recoverMysql5")
        self.recoverMysql8 = QtGui.QAction(parent=MainWindow)
        icon2 = QtGui.QIcon()
        icon2.addPixmap(QtGui.QPixmap(self.resource_path(os.path.join("image","MySQL8.png"))), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        self.recoverMysql8.setIcon(icon2)
        self.recoverMysql8.setObjectName("recoverMysql8")
        self.saveSelection = QtGui.QAction(parent=MainWindow)
        icon3 = QtGui.QIcon()
        icon3.addPixmap(QtGui.QPixmap(self.resource_path(os.path.join("image","SaveSelection.png"))), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        self.saveSelection.setIcon(icon3)
        self.saveSelection.setObjectName("saveSelection")
        self.saveAll = QtGui.QAction(parent=MainWindow)
        icon4 = QtGui.QIcon()
        icon4.addPixmap(QtGui.QPixmap(self.resource_path(os.path.join("image","SaveAll.png"))), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        self.saveAll.setIcon(icon4)
        self.saveAll.setObjectName("saveAll")
        self.actionabout = QtGui.QAction(parent=MainWindow)
        icon5 = QtGui.QIcon()
        icon5.addPixmap(QtGui.QPixmap(self.resource_path(os.path.join("image","about.png"))), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        self.actionabout.setIcon(icon5)
        self.actionabout.setObjectName("actionabout")
        self.actionhelp = QtGui.QAction(parent=MainWindow)
        icon6 = QtGui.QIcon()
        icon6.addPixmap(QtGui.QPixmap(self.resource_path(os.path.join("image","help.png"))), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        self.actionhelp.setIcon(icon6)
        self.actionhelp.setObjectName("actionhelp")
        self.toolBar.addAction(self.recoverMysql5)
        self.toolBar.addSeparator()
        self.toolBar.addAction(self.recoverMysql8)
        self.toolBar.addSeparator()
        self.toolBar.addAction(self.saveSelection)
        self.toolBar.addSeparator()
        self.toolBar.addAction(self.saveAll)
        self.toolBar.addSeparator()
        self.toolBar.addAction(self.actionabout)
        self.toolBar.addSeparator()
        self.toolBar.addAction(self.actionhelp)
        self.toolBar.addSeparator()

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Frombyte Mysql Recovery v1.0"))
        self.toolBar.setWindowTitle(_translate("MainWindow", "toolBar"))
        self.recoverMysql5.setText(_translate("MainWindow", "MySQL5.X"))
        self.recoverMysql8.setText(_translate("MainWindow", "MySQL8.X"))
        self.saveSelection.setText(_translate("MainWindow", "保存选中"))
        self.saveAll.setText(_translate("MainWindow", "保存全部"))
        self.actionabout.setText(_translate("MainWindow", "关 于"))
        self.actionhelp.setText(_translate("MainWindow", "帮 助"))


