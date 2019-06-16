from collections import namedtuple

from ..DyStockCtaTemplate import *


class DyST_LimitUpDownMonitor(DyStockCtaTemplate):
    """
        监控市场涨停和跌停股票的变化情况
    """
    name = 'DyST_LimitUpDownMonitor'
    chName = '涨跌停监控'

    # UI
    dataHeader = ['涨停数', '跌停数']

    # '信息' is like '涨停增加10%'
    signalDetailsHeader = ['信息']


    #--------------- 策略参数 ---------------
    DeltaSetting = namedtuple('DeltaSetting', 'samplePeriod delta threshold')

    # 涨停数在多少秒内变化多少(%)通知，并且涨停数要大于@threshold
    limitUp = DeltaSetting(60, 10, 10)

    # 跌停数在多少秒内变化多少(%)通知，并且跌停数要大于@threshold
    limitDown = DeltaSetting(60, 10, 10)
    

    def __init__(self, ctaEngine, info, state, strategyParam=None):
        super().__init__(ctaEngine, info, state, strategyParam)

    @DyStockCtaTemplate.onOpenWrapper
    def onOpen(self, date, codes=None):
        self._limitUpCounts = [] # [[time, count]]
        self._limitDownCounts = []

        self._monitoredStocks.extend(list(self._preparedData['codes']))
        return True

    def _updateUi(self):
        """
            更新UI的数据窗口
        """
        rows = [[self._limitUpCounts[-1][1] if self._limitUpCounts else 0, self._limitDownCounts[-1][1] if self._limitDownCounts else 0]]
        self.putStockMarketMonitorUiEvent(data=rows, newData=True, datetime_=self.marketDatetime)

    def _checkLimitUp(self):
        startTime, startCount = self._limitUpCounts[0]
        endTime, endCount = self._limitUpCounts[-1]

        newStart = None
        if startCount > 0:
            delta = (endCount - startCount)/startCount*100
            if abs(delta) >= self.limitUp.delta:
                # notify
                if endCount > self.limitUp.threshold:
                    timeDelta = DyStockCommon.getTimeInterval(startTime, endTime)
                    limitDownCount = self._limitDownCounts[-1][1]
                    action = '增加' if delta > 0 else '减少'
                    info = '涨停数{}秒内{}{:.1f}%, 涨停数:{}, 跌停数:{}'.format(timeDelta, action, abs(delta), endCount, limitDownCount)
                    self.putStockMarketMonitorUiEvent(signalDetails=[[info]], datetime_=self.marketDatetime)
            
                newStart = -1
        else: # remove 0
            newStart = 1

        if newStart is None:
            timeDelta = DyStockCommon.getTimeInterval(startTime, endTime)
            if timeDelta >= self.limitUp.samplePeriod:
                newStart = 1

        # reset sample period
        if newStart is not None:
            self._limitUpCounts = self._limitUpCounts[newStart:]

    def _checkLimitDown(self):
        startTime, startCount = self._limitDownCounts[0]
        endTime, endCount = self._limitDownCounts[-1]
        
        newStart = None
        if startCount > 0:
            delta = (endCount - startCount)/startCount*100
            if abs(delta) >= self.limitDown.delta:
                # notify
                if endCount > self.limitDown.threshold:
                    timeDelta = DyStockCommon.getTimeInterval(startTime, endTime)
                    limitUpCount = self._limitUpCounts[-1][1]
                    action = '增加' if delta > 0 else '减少'
                    info = '跌停数{}秒内{}{:.1f}%, 涨停数:{}, 跌停数:{}'.format(timeDelta, action, abs(delta), limitUpCount, endCount)
                    self.putStockMarketMonitorUiEvent(signalDetails=[[info]], datetime_=self.marketDatetime)
            
                newStart = -1
        else: # remove 0
            newStart = 1

        if newStart is None:
            timeDelta = DyStockCommon.getTimeInterval(startTime, endTime)
            if timeDelta >= self.limitDown.samplePeriod:
                newStart = 1

        # reset sample period
        if newStart is not None:
            self._limitDownCounts = self._limitDownCounts[newStart:]

    def _check(self):
        self._checkLimitUp()
        self._checkLimitDown()

    def _count(self, ticks):
        limitUpCount = 0
        limitDownCount = 0
        sampleTick = None
        for _, tick in ticks.items():
            sampleTick = tick
            increase = (tick.price - tick.preClose)/tick.preClose*100

            if increase >= DyStockCommon.limitUpPct:
                limitUpCount += 1

            if increase <= DyStockCommon.limitDownPct:
                limitDownCount += 1

        # for backtesting
        marketTime = self.marketTime
        if marketTime is None and sampleTick is not None:
            marketTime = sampleTick.time

        if marketTime is None:
            return

        self._limitUpCounts.append([marketTime, limitUpCount])
        self._limitDownCounts.append([marketTime, limitDownCount])

    @DyStockCtaTemplate.onTicksWrapper
    def onTicks(self, ticks):
        """
            收到行情TICKs推送
            @ticks: {code: DyStockCtaTickData}
        """
        self._count(ticks)
        self._check()

        self._updateUi()


    #################### 开盘前的数据准备 ####################
    @classmethod
    def prepare(cls, date, dataEngine, info, codes=None, errorDataEngine=None, backTestingContext=None):
        """
            @date: 回测或者实盘时，此@date为前一交易日
            @return: {'codes': [code]}
        """
        daysEngine = dataEngine.daysEngine

        if not daysEngine.loadCodeTable(codes=codes):
            return None
        
        return {'codes': list(daysEngine.stockCodes)}

    ######################################## For IM ########################################
    ######################################## For IM ########################################
    @classmethod
    def data2Msg(cls, data):
        """
            将推送给UI的data转成消息列表，这样可以推送给QQ或者微信
            @return: [message]
        """
        row = data[0]
        return ['涨停数:{}, 跌停数:{}'.format(row[0], row[1])]
