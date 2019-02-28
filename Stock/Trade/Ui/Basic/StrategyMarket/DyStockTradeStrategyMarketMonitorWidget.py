from PyQt5 import QtCore
from PyQt5.QtWidgets import QWidget, QLabel, QGridLayout, QTabWidget

from EventEngine.DyEvent import *
from .Data.DyStockTradeStrategyMarketMonitorDataWidget import *
from .Ind.DyStockTradeStrategyMarketMonitorIndWidget import *


class DyStockTradeStrategyMarketMonitorWidget(QWidget):
    """
        股票策略实时监控窗口，动态创建
    """
    signal = QtCore.pyqtSignal(type(DyEvent()))

    def __init__(self, eventEngine, strategyCls, strategyState):
        super().__init__()

        self._eventEngine = eventEngine
        self._strategyCls = strategyCls
        
        self._cloneDataWidgets = []

        self._registerEvent()

        self._initUi(strategyState)

    def _initUi(self, strategyState):
        self._dataWidget = DyStockTradeStrategyMarketMonitorDataWidget(self._strategyCls, self)
        self._indWidget = DyStockTradeStrategyMarketMonitorIndWidget(self._eventEngine, self._strategyCls, strategyState)

        self._dataLabel = QLabel('数据')
        self._indLabel = QLabel('指示')

        grid = QGridLayout()
        grid.setSpacing(0)

        grid.addWidget(self._dataLabel, 0, 0)
        grid.addWidget(self._dataWidget, 1, 0)
        grid.addWidget(self._indLabel, 2, 0)
        grid.addWidget(self._indWidget, 3, 0)
        
        grid.setRowStretch(0, 1)
        grid.setRowStretch(1, 30)
        grid.setRowStretch(2, 1)
        grid.setRowStretch(3, 30)

        self.setLayout(grid)

        # set menu for labels
        self._dataLabel.setContextMenuPolicy(Qt.CustomContextMenu)
        self._dataLabel.customContextMenuRequested.connect(self._showDataLabelContextMenu)

        self._indLabel.setContextMenuPolicy(Qt.CustomContextMenu)
        self._indLabel.customContextMenuRequested.connect(self._showIndLabelContextMenu)

        self._dataLabelMenu = QMenu(self) 
        
        action = QAction('叠加', self)
        action.triggered.connect(self._overlapAct)
        self._dataLabelMenu.addAction(action)

        action = QAction('克隆', self)
        action.triggered.connect(self._cloneDataWidgetAct)
        self._dataLabelMenu.addAction(action)

        self._indLabelMenu = QMenu(self) 
        
        action = QAction('叠加', self)
        action.triggered.connect(self._overlapAct)
        self._indLabelMenu.addAction(action)

    def _stockMarketMonitorUiHandler(self, event):
        if 'data' in event.data:
            data = event.data['data']['data']
            new = event.data['data']['new']

            strategyCls = event.data['class']
            if strategyCls.maxUiDataRowNbr is not None:
                data = data[:strategyCls.maxUiDataRowNbr]

            self._dataWidget.update(data, new)
            for w in self._cloneDataWidgets:
                w.update(data, new)

        if 'ind' in event.data:
            self._indWidget.update(event.data['ind'])

    def _signalEmitWrapper(self, event):
        """ !!!Note: The value of signal.emit will always be changed each time you getting.
        """
        self.signal.emit(event)

    def _registerEvent(self):
        self.signal.connect(self._stockMarketMonitorUiHandler)
        self._eventEngine.register(DyEventType.stockMarketMonitorUi + self._strategyCls.name, self._signalEmitWrapper)

    def _unregisterEvent(self):
        self.signal.disconnect(self._stockMarketMonitorUiHandler)
        self._eventEngine.unregister(DyEventType.stockMarketMonitorUi + self._strategyCls.name, self._signalEmitWrapper)

    def closeEvent(self, event):
        self._dataWidget.close()
        self._indWidget.close()

        self._unregisterEvent()

        return super().closeEvent(event)

    def _showDataLabelContextMenu(self, position):
        self._dataLabelMenu.popup(QCursor.pos())

    def _showIndLabelContextMenu(self, position):
        self._indLabelMenu.popup(QCursor.pos())

    def _overlapAct(self):
        grid = self.layout()

        # remove
        self._dataLabel.setText('')
        self._indLabel.setText('')

        grid.removeWidget(self._dataLabel)
        grid.removeWidget(self._dataWidget)
        grid.removeWidget(self._indLabel)
        grid.removeWidget(self._indWidget)

        # add
        self._tabWidget = QTabWidget()

        self._tabWidget.addTab(self._dataWidget, '数据')
        self._tabWidget.addTab(self._indWidget, '指示')

        grid.addWidget(self._tabWidget, 0, 0)

        grid.setRowStretch(0, 1)
        grid.setRowStretch(1, 0)
        grid.setRowStretch(2, 0)
        grid.setRowStretch(3, 0)

        # 设置Tab右键菜单事件
        tabBar = self._tabWidget.tabBar()
        tabBar.setContextMenuPolicy(Qt.CustomContextMenu)
        tabBar.customContextMenuRequested.connect(self._showTabContextMenu)

        # 创建TabBar菜单
        self._tabBarMenu = QMenu(self)

        action = QAction('平铺', self)
        action.triggered.connect(self._flatAct)
        self._tabBarMenu.addAction(action)

    def _showTabContextMenu(self, position):
        self._tabBarMenu.popup(QCursor.pos())

    def _flatAct(self):
        grid = self.layout()

        # remove
        self._tabWidget.removeTab(0)
        self._tabWidget.removeTab(0)

        grid.removeWidget(self._tabWidget)

        self._tabWidget.hide()
        
        # add
        self._dataLabel.setText('数据')
        self._indLabel.setText('指示')

        grid.addWidget(self._dataLabel, 0, 0)
        grid.addWidget(self._dataWidget, 1, 0)
        grid.addWidget(self._indLabel, 2, 0)
        grid.addWidget(self._indWidget, 3, 0)

        self._dataWidget.show()
        self._indWidget.show()
        
        grid.setRowStretch(0, 1)
        grid.setRowStretch(1, 30)
        grid.setRowStretch(2, 1)
        grid.setRowStretch(3, 30)

    def removeCloneDataWidget(self, cloneWidget):
        try:
            self._cloneDataWidgets.remove(cloneWidget)
        except:
            pass

    def _cloneDataWidgetAct(self):
        dataWidget = self._dataWidget.clone()
        self._cloneDataWidgets.append(dataWidget)

        dataWidget.setWindowTitle('策略[{}]: 数据'.format(self._strategyCls.chName))
        dataWidget.showMaximized()
