import pandas as pd

import DyCommon.DyTalib as DyTalib
from ..DyStockCtaTemplate import *
from ....Data.Utility.DyStockDataUtility import *


class DyST_DoubleMa_BT(DyStockCtaTemplate):
    """
        Pure backtesting strategy using vector method
        此策略是纯向量回测，由于向量回测跟实盘有不少不同，所以相应的实盘策略需要重新写。
        纯向量回测的策略必须以"_BT"结尾，这样实盘界面不会载入此策略。
    """
    name = 'DyST_DoubleMa_BT'
    chName = '双均线_BT'

    backTestingMode = 'bar1d'

    curCodeBuyMaxNbr = 1
    buyPrice = 'price'
    sellPrice = 'price'

    #------------ 策略prepare参数 ------------
    fastMa = 5
    lowMa = 10

    #--------------- 策略参数 ---------------
    maxPosNbr = 10 # 最多持仓10只股票
    

    def __init__(self, ctaEngine, info, state, strategyParam=None):
        super().__init__(ctaEngine, info, state, strategyParam)

        self._curInit()

    def _onOpenConfig(self):
        backTestingCodes = self._preparedData.get('backTesting')
        self._monitoredStocks.extend(backTestingCodes)

    def _curInit(self, date=None):
        pass

    @DyStockCtaTemplate.onOpenWrapper
    def onOpen(self, date, codes=None):
        # 当日初始化
        self._curInit(date)

        self._onOpenConfig()

        return True

    def _execBuySignal(self, buyCodes, ticks):
        """
            执行买入信号
        """
        trueBuyCodes = []
        for code in buyCodes:
            if code in self._curPos:
                continue

            tick = ticks.get(code)
            if tick is None:
                continue

            trueBuyCodes.append(code)

        for code in trueBuyCodes:
            self.buyByRatio(ticks.get(code), 1/self.maxPosNbr*100, self.cAccountCapital)

    def _execSellSignal(self, sellCodes, ticks):
        """
            执行卖出信号
        """
        if sellCodes is None:
            return

        for code in sellCodes:
            tick = ticks.get(code)
            if tick is None:
                continue

            self.closePos(tick)

    def _execSignal(self, buyCodes, sellCodes, ticks):
        """
            执行信号
            先卖后买，对于日线级别的回测，可以有效利用仓位。
        """
        self._execSellSignal(sellCodes, ticks)
        self._execBuySignal(buyCodes, ticks)

    def _procSignal(self, ticks):
        buyCodes = self._preparedData.get('backTesting')
        sellCodes = self._preparedPosData.get('backTesting')

        self._execSignal(buyCodes, sellCodes, ticks)

    @DyStockCtaTemplate.onTicksWrapper
    def onTicks(self, ticks):
        """
            收到行情TICKs推送
            @ticks: {code: DyStockCtaTickData}
        """
        # 处理信号
        self._procSignal(ticks)

    @DyStockCtaTemplate.onBarsWrapper
    def onBars(self, bars):
        self.onTicks(bars)


    #################### 开盘前的数据准备 ####################
    @classmethod
    def _prepareAll(cls, date, dataEngine, info, codes=None, errorDataEngine=None, backTestingContext=None):
        """
            准备回测的所有信号和数据
            @return: {code: indicator DF}
        """
        # 检查是否已经准备好所有的信号向量
        if hasattr(backTestingContext, 'prepareAll'):
            return backTestingContext.prepareAll

        info.print('开始准备回测[{}, {}]的所有信号和数据...'.format(backTestingContext.startDate, backTestingContext.endDate), DyLogData.ind)

        prepareAll = {}
        daysEngine = dataEngine.daysEngine
        errorDaysEngine = errorDataEngine.daysEngine
        errorticksEngine = errorDataEngine.ticksEngine

        if not daysEngine.loadCodeTable(codes=codes):
            return None
        codes = daysEngine.stockCodes

        info.print('开始计算{}只股票的指标...'.format(len(codes)), DyLogData.ind)
        progress = DyProgress(info)
        progress.init(len(codes), 100, 1)

        for code in codes:
            print('{}({})'.format(code, daysEngine.stockCodes[code]))

            if not errorDaysEngine.loadCode(code, [-cls.lowMa, backTestingContext.startDate, backTestingContext.endDate], latestAdjFactorInDb=False):
                progress.update()
                continue

            # 计算均线
            df = errorDaysEngine.getDataFrame(code)
            mas = DyStockDataUtility.getMas(df, [cls.fastMa, cls.lowMa])

            # 买点: 快速均线上穿慢速均线
            buyIndicator = DyTalib.CROSS(mas['ma%d'%cls.fastMa], mas['ma%d'%cls.lowMa])
            buyIndicator = buyIndicator.shift(-1)

            # 卖点: 快速均线下穿慢速均线达到3%
            sellIndicator = (mas['ma%d'%cls.lowMa] - mas['ma%d'%cls.fastMa])/mas['ma%d'%cls.lowMa] > 0.03
            sellIndicator = sellIndicator.shift(-1)

            # 涨幅
            increase = df['close'].pct_change()*100

            # now we combine them into DF
            df = pd.concat([buyIndicator, sellIndicator, increase], axis=1)
            df.columns = ['buy', 'sell', 'increase']

            prepareAll[code] = df

            progress.update()

        info.print('完成计算{}只股票的指标...'.format(len(codes)), DyLogData.ind)

        info.print('完成准备回测[{}, {}]的所有信号和数据'.format(backTestingContext.startDate, backTestingContext.endDate), DyLogData.ind)
        backTestingContext.prepareAll = prepareAll # 保存信号向量到@backTestingContext

        return prepareAll

    @classmethod
    def prepare(cls, date, dataEngine, info, codes=None, errorDataEngine=None, backTestingContext=None):
        """
            回测采用矢量法，所以实盘的准备数据和回测的准备数据不一样。策略也需要做不同的处理。
            @date: 回测或者实盘时，此@date为前一交易日
            @return: {
                        # 回测
                        'backTesting': [code], # 买入股票代码
                     }
        """
        prepareAll = cls._prepareAll(date, dataEngine, info, codes, errorDataEngine, backTestingContext)
        if prepareAll is None:
            return None

        # check if we statisfy all conditions
        backTestingCodes = {} # {code: increase}
        for code, df in prepareAll.items():
            df = df[:date]
            if df.empty:
                continue

            if sum(df.iloc[-1, :1]) == 1: # ['buy'], 现在只有一个买入信号指标，如果是多个，则需要多个都是1，所以取sum
                print('{}: prepare buy {}'.format(date, code))
                backTestingCodes[code] = df.iat[-1, -1] # For sorting by increase

        backTestingCodes = sorted(backTestingCodes, key=lambda k: backTestingCodes[k], reverse=True)
        backTestingCodes = backTestingCodes[:cls.maxPosNbr]

        return {'backTesting': backTestingCodes}

    @classmethod
    def preparePos(cls, date, dataEngine, info, posCodes=None, errorDataEngine=None, backTestingContext=None):
        """
            策略开盘前持仓准备数据
            @date: 前一交易日
            @return: {
                        # 回测
                        'backTesting': [code], # 买入股票代码
                     }
        """
        if not posCodes: # not positions
            return {}

        errorDaysEngine = errorDataEngine.daysEngine

        # 回测
        backTestingCodes = []
        for code in posCodes:
            df = backTestingContext.prepareAll.get(code)
            df = df[:date]
            sell = df.iat[-1, 1]
            if sell:
                print('{}: prepare sell {}'.format(date, code))
                backTestingCodes.append(code)

        return {'backTesting': backTestingCodes}
