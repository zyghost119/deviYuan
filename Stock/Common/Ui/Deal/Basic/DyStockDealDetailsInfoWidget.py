from PyQt5.QtWidgets import QMessageBox

from DyCommon.DyCommon import DyErrorInfo
from Stock.Data.Engine.DyStockDataEngine import DyStockDataEngine
from DyCommon.Ui.DyTableWidget import DyTableWidget
from Stock.Common.DyStockCommon import DyStockCommon


class DyStockDealDetailsInfoWidget(DyTableWidget):

    colNames = ['日期','代码','名称','收盘','涨幅(%)','换手(%)','指数','指数收盘','指数涨幅(%)']

    def __init__(self, daysEngine):
        super().__init__(None, True, False, autoScroll=False)

        self._daysEngine = daysEngine

        self.setColNames(self.colNames)
        self.setAutoForegroundCol('涨幅(%)')

    def set(self, code, date, n=0):
        # 包含前一交易日数据
        dates = [date, n-1] if n <= 0 else [date, n]
        if not self._daysEngine.loadCode(code, dates, latestAdjFactorInDb=False):
            return

        self._code = code
        day = self._daysEngine.codeTDayOffset(code, date, n)
        if day is None:
            return

        self._day = day

        # stock
        df = self._daysEngine.getDataFrame(code)

        # get previous close
        preTDay = self._daysEngine.codeTDayOffset(code, self._day, -1)
        preClose = df.ix[preTDay, 'close']

        # 获取当日数据
        close = df.ix[self._day, 'close']
        turn = df.ix[self._day, 'turn']

        # index
        index = self._daysEngine.getIndex(code)
        indexDf = self._daysEngine.getDataFrame(index)

        # get previous close of index
        preCloseIndex = None
        preTDay = self._daysEngine.tDaysOffset(self._day, -1)
        try:
            preCloseIndex = indexDf.ix[preTDay, 'close']
        except: # 如果出现停牌，对应的指数的前一日是缺失的，所以需要重新载入指数日线数据
            errorInfo = DyErrorInfo(self._daysEngine.eventEngine)
            errorDataEngine = DyStockDataEngine(self._daysEngine.eventEngine, errorInfo, registerEvent=False)
            if not errorDataEngine.daysEngine.load([self._day, -1], latestAdjFactorInDb=False, codes=[]):
                return

            errorIndexDf = errorDataEngine.daysEngine.getDataFrame(index)
            try:
                preCloseIndex = errorIndexDf.ix[preTDay, 'close']
            except: # 免费的Wind的数据质量不咋地
                QMessageBox.warning(self, '警告', '{}({}){}日线数据缺失!'.format(index, DyStockCommon.indexes[index], preTDay))

        # 获取指数的当日数据
        closeIndex = None
        try:
            closeIndex = indexDf.ix[self._day, 'close']
        except:
            QMessageBox.warning(self, '警告', '{}({}){}日线数据缺失!'.format(index, DyStockCommon.indexes[index], self._day))

        row = [self._day,
               code,
               self._daysEngine.stockAllCodesFunds[code],
               close,
               (close - preClose)*100/preClose,
               turn,
               self._daysEngine.stockAllCodesFunds[index],
               closeIndex,
               (closeIndex - preCloseIndex)*100/preCloseIndex if closeIndex is not None and preCloseIndex else None
              ]

        self.appendRow(row, True)

    @property
    def day(self):
        return self._day

    @property
    def code(self):
        return self._code

    def forward(self):
        self.set(self.code, self.day, -1)

    def backward(self):
        self.set(self.code, self.day, 1)

    @property
    def turn(self):
        return float(self[0, '换手(%)'])