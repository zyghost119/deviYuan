from collections import OrderedDict

from DyCommon.Ui.DyTableWidget import *


class DyStockTradeStrategyMarketMonitorDataWidget(DyTableWidget):
    """ 股票实盘策略数据窗口 """


    class LogData:
        """
            log-structured storage for clone
        """
        def __init__(self):
            self.newData = None
            self.updatedData = OrderedDict() # {row key: row}

        def init(self, data):
            self.newData = data
            self.updatedData = OrderedDict()

        def update(self, data):
            for row in data:
                code = row[0] # pos 0 is code, date or something else, but should be key for one row
                self.updatedData[code] = row


    def __init__(self, strategyCls, parent):
        super().__init__(None, True, False)

        self._strategyCls = strategyCls
        self._parent = parent

        self._logData = self.LogData() # for clone

        self.setColNames(strategyCls.dataHeader)
        self.setAutoForegroundCol('涨幅(%)')

    def update(self, data, newData=False):
        """ @data: [[col0, col1, ...]] """

        if newData: # !!!new, without considering keys
            self.fastAppendRows(data, autoForegroundColName='涨幅(%)', new=True)

            self._logData.init(data)
        else: # updating by keys
            rowKeys = []
            for row in data:
                code = row[0] # pos 0 is code, date or something else, but should be key for one row
                self[code] = row

                rowKeys.append(code)

            self.setItemsForeground(rowKeys, (('买入', Qt.red), ('卖出', Qt.darkGreen)))

            self._logData.update(data)

    def clone(self):
        self_ = self.__class__(self._strategyCls, self._parent)

        # new data
        if self._logData.newData is not None:
            self_.update(self._logData.newData, newData=True)

        # data with keys
        data = [row for _, row in self._logData.updatedData.items()]
        if data:
            self_.update(data, newData=False)

        return self_

    def closeEvent(self, event):
        self._parent.removeCloneDataWidget(self)

        return super().closeEvent(event)
