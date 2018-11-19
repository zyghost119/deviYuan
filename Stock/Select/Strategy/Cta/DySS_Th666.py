import operator

from ..DyStockSelectStrategyTemplate import *
from ....Data.Utility.DyStockDataUtility import *
from DyCommon.DyTalib import *


class DySS_Th666(DyStockSelectStrategyTemplate):
    """
        https://www.joinquant.com/post/15297?tag=algorithm
        1.首先趋势判断，中证500趋势判断，日线级别kdj金叉多区间，上一交易日为大阴线3/5实体（并且50日atr大于2/3平均值），obos（超买超卖指标）小于-100，满足条件则选股
        2.选股9：25-9：30之间高开五个点以上（包含），不含st，开盘前五秒上涨，选出并打印

        魔元注:
            由于obos是绝对值，是不是可以改成相对值? 即 OBOS=(N日内上涨家数移动总和－N日内下跌家数移动总和)/N日内上涨下跌家数总和 * 100

            OBOS=N日内上涨家数移动总和－N日内下跌家数移动总和
            N日的采样统计一般设定为10日。
    """
    name = 'DySS_Th666'
    chName = 'th666聚宽求助'

    autoFillDays = True
    optimizeAutoFillDays = True
    continuousTicks = True

    colNames = ['代码', '名称']

    param = OrderedDict\
                ([
                    ('基准日期', '2018-11-15'),
                    ('中证500: 上一交易日阴线实体比例>=', 0.1), # 0.6
                    ('中证500: 50日ATR/平均值>', 0.67),
                    ('中证500: OBOS<', 1000), # -100
                ])


    def __init__(self, param, info):
        super().__init__(param, info)

        # unpack parameters
        self._baseDate              = param['基准日期']
        self._bodyRatio             = param['中证500: 上一交易日阴线实体比例>=']
        self._atrOverMean           = param['中证500: 50日ATR/平均值>']
        self._obos                  = param['中证500: OBOS<']

        self._canContinue = True
        self._preCloses = {}

    def onDaysLoad(self):
        return self._baseDate, -50-14

    def onTicksLoad(self):
        return self._baseDate, 0

    def _get500(self, daysEngine):
        # 选取中小创流通市值前500只
        codes = {}
        for code in daysEngine.stockCodes:
            # 由于DY里没有中证500成分股信息，所以用中小创代替
            indexCode = DyStockCommon.getIndex(code)
            if indexCode not in [DyStockCommon.cybIndex, DyStockCommon.zxbIndex]:
                continue

            df = self._daysEngine.getDataFrame(code)
            if df is None:
                continue

            codes[code] = df['amt'][-1]/df['turn'][-1]

        sortedCodes = sorted(codes, key=lambda k: codes[k], reverse=True)
        return sortedCodes[:500]

    def onInit(self, dataEngine, errorDataEngine):
        print("onInit...")

        self._daysEngine = dataEngine.daysEngine
        self._stockAllCodes = self._daysEngine.stockAllCodes

        selectedCodes = self._get500(self._daysEngine)

        # obos（超买超卖指标）小于-100
        N = 10

        # date range
        df = self._daysEngine.getDataFrame(DyStockCommon.szIndex)
        dateRange = df[:self._baseDate].tail(N+1).index
        startDate, endDate = dateRange[0].strftime("%Y-%m-%d"), dateRange[-1].strftime("%Y-%m-%d")

        zeros = pd.Series([0]*N, index=df[:self._baseDate].tail(N).index)
        ups = zeros
        downs = zeros
        for code in selectedCodes:
            df = self._daysEngine.getDataFrame(code)
            if df is None:
                continue

            closes = df['close'][startDate:endDate]
            pctChanges = closes.pct_change().dropna()

            # 上涨
            temp = zeros + pctChanges.apply(lambda x: 1 if x > 0 else 0)
            ups = ups + temp.fillna(0)

            # 下跌
            temp = zeros + pctChanges.apply(lambda x: 1 if x < 0 else 0)
            downs = downs + temp.fillna(0)

        obos = ups.sum() - downs.sum()
        if obos >= self._obos:
            print("中证500: OBOS: {}".format(obos))
            self._canContinue = False

    def onEtfDays(self, code, df):
        """ Etf日线数据 """
        if not self._canContinue:
            return

        # 日线级别kdj金叉多区间
        if code == DyStockCommon.etf500:
            K, D, J = KDJ(df['high'].values, df['low'].values, df['close'].values, adjust=False)
            if K[-1] < D[-1]:
                print("中证500: KDJ不是金叉区间")
                self._canContinue = False
                return

            # 上一交易日为大阴线3/5实体
            if df['close'][-2] >= df['open'][-2]:
                print("中证500: 上一交易日不是阴线")
                self._canContinue = False
                return

            bodyRatio = abs(df['close'][-2] - df['open'][-2])/abs(df['low'][-2] - df['high'][-2])
            if bodyRatio < self._bodyRatio:
                print("中证500: 上一交易日阴线实体比例: {}".format(bodyRatio))
                self._canContinue = False
                return

            # 50日atr大于2/3平均值
            atr = np.array(ATR(df['high'].values, df['low'].values, df['close'].values))
            atrOverMean = float(atr[-1]/atr[-50:].mean())
            if atrOverMean <= self._atrOverMean:
                print("中证500: 50日atr/平均值: {}".format(atrOverMean))
                self._canContinue = False
                return

    def onStockDays(self, code, df):
        if not self._canContinue:
            return

        # 选股9：25-9：30之间高开五个点以上（包含），不含st
        openIncrease = (df['open'][-1] - df['close'][-2])/df['close'][-2] * 100
        if openIncrease < 5:
            return

        if 'st' in self._stockAllCodes[code] or 'ST' in self._stockAllCodes[code]:
            return

        self._preCloses[code] = df['close'][-2]

        # 设置结果
        row = [code, self._stockAllCodes[code]]
        print(row)
        self._result.append(row)

    def onStockTicks(self, code, dfs):
        # 开盘前五秒上涨
        prices = dfs['price']
        date = dfs.index[0].strftime("%Y-%m-%d")
        if self._preCloses[code] >= prices[:"{} 09:30:05".format(date)][-1]:
            print("Remove {}".format([code, self._stockAllCodes[code]]))
            self.removeFromResult(code)