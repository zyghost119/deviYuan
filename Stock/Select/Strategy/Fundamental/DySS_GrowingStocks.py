from datetime import datetime
import operator
import time
from collections import OrderedDict

import tushare as ts

from Stock.Select.Strategy.DyStockSelectStrategyTemplate import DyStockSelectStrategyTemplate
from Stock.Common.DyStockCommon import DyStockCommon


class DySS_GrowingStocks(DyStockSelectStrategyTemplate):
    name = 'DySS_GrowingStocks'
    chName = '成长股'

    colNames = ['代码', '名称', '得分', '3Y平均营业收入YoY(%)', '3Y平均净利润YoY(%)', 'PE', 'PEG', '相对指数强度(%)']

    param = OrderedDict\
                ([
                    ('基准日期', datetime.today().strftime("%Y-%m-%d")),
                    ('向前N日周期', 30),
                    ('得分至少', 80),
                    ('TuSharePro访问间隔(ms)', 0),
                ])

    paramToolTip = {'向前N日周期': '周期内相对指数强度'}


    def __init__(self, param, info):
        super().__init__(param, info)

        ts.set_token(DyStockCommon.tuShareProToken)
        self._pro = ts.pro_api()

        # unpack parameters
        self._baseDate              = param['基准日期']
        self._forwardNTDays         = param['向前N日周期'] # @self._baseDate is included
        self._score                 = param['得分至少']
        self._tuShareProInterval    = param['TuSharePro访问间隔(ms)']/1000

    def onDaysLoad(self):
        return self._baseDate, -self._forwardNTDays + 1

    def onInit(self, dataEngine, errorDataEngine):
        self._daysEngine = dataEngine.daysEngine

        self._stockAllCodes = self._daysEngine.stockAllCodes

    def _getIndicators(self, code):
        """
            @return: [近三年营业收入同比增长率], [近三年净利润同比增长率], 最新年化每股收益
        """
        time.sleep(self._tuShareProInterval)
        print("TuSharePro: 获取{}({})财务数据...".format(code, self._stockAllCodes[code]))

        # 最近五年
        fowardYearNbr = 5
        now = datetime.now()
        year = now.year

        # get from TuSharePro
        df = self._pro.income(ts_code=code, start_date='{}0101'.format(year-fowardYearNbr), end_date=now.strftime("%Y%m%d"))
        df = df[['end_date', 'revenue', 'operate_profit', 'basic_eps']]
        df = df.drop_duplicates(['end_date'])

        # select annual data for 'revenue', 'operate_profit'
        annualDf = df[['end_date', 'revenue', 'operate_profit']]
        annualEndDates = ['{}1231'.format(year-i) for i in range(1, fowardYearNbr+1)]
        annualDf = df[df['end_date'].isin(annualEndDates)]

        # 取最近三年的营业收入，运营利润增长率(%)
        annualDf = annualDf[::-1]
        pctChangeDf = annualDf[['revenue', 'operate_profit']].pct_change()
        pctChangeDf *= 100
        pctChangeDf = pctChangeDf.tail(fowardYearNbr-2)
        revenues, operateProfits = pctChangeDf['revenue'].tolist(), pctChangeDf['operate_profit'].tolist()

        # 最新年化每股收益
        eps = df['basic_eps'][0]
        quarter = df['end_date'][0]
        quarter = int(quarter[4:6])
        eps /= quarter/12

        return revenues, operateProfits, eps

    def _getScore(self, rates):
        """
            100分制
            @return: 得分，平均每季度增长率
        """
        # (0, 10]: 1, (10, 20]: 2, (20, 30]: 3, (30, ): 4
        scores = []
        for rate in rates:
            score = 0
            if 0 < rate <= 10:
                score = 1
            elif 10 < rate <= 20:
                score = 2
            elif 20 < rate <= 30:
                score = 3
            elif rate > 30:
                score = 4

            scores.append(score)

        return sum(scores)*100/(4*len(scores)), sum(rates)/len(rates)

    def _getScores(self, code):
        """
            100分制
            @return: 总得分，平均每季度营业总收入同比增长率，平均每季度净利润同比增长率
        """
        rates1, rates2, earningPerShare = self._getIndicators(code)

        score1, aveRate1 = self._getScore(rates1)
        score2, aveRate2 = self._getScore(rates2)

        return (score1 + score2)/2, aveRate1, aveRate2, earningPerShare

    def onStockDays(self, code, df):
        try:
            score, aveRate1, aveRate2, earningPerShare = self._getScores(code)
        except Exception as ex:
            self._info("从TuSharePro读取{}({})'财务数据'异常: {}".format(code, self._stockAllCodes[code], ex), DyLogData.warning)
            return

        if score < self._score:
            return

        # PE & PEG
        pe = None
        peg = None
        if not df.empty:
            try:
                pe = df.ix[-1, 'close']/earningPerShare
                peg = pe/aveRate2
            except Exception as ex:
                pass

        # 相对指数强度
        strong = None
        indexDf = self._daysEngine.getDataFrame(self._daysEngine.getIndex(code))
        indexCloses = indexDf['close']
        indexRatio = indexCloses[-1]/indexCloses[0]

        if not df.empty:
            closes = df['close']
            stockRatio = closes[-1]/closes[0]

            strong = (stockRatio - indexRatio)*100/indexRatio

        # 设置结果
        row = [code, self._stockAllCodes[code], score, aveRate1, aveRate2, pe, peg, strong]
        print(row)
        self._result.append(row)
        self._result.sort(key=operator.itemgetter(2), reverse=True)
