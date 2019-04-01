
class DyStockBackTestingEventHandType:
    engine = 0
    other = 1

    nbr = 2


class DyStockBackTestingStrategyReqData:
    def __init__(self, strategyCls, tDays, settings, param, codes=None, paramGroupNo=None):
        self.strategyCls = strategyCls
        self.tDays = tDays # 来自UI的req，@tDays是[start date, end date]。分发给子进程的req，@tDays是[tDay]
        self.settings = settings # {}, 回测参数设置, 不包含日期(也就是说忽略日期相关参数)
        self.codes = codes # 测试用的股票代码集
        self.param = param # 策略参数
        self.paramGroupNo = paramGroupNo # 策略参数组合号


class DyStockBackTestingStrategyAckData:
    def __init__(self, datetime, strategyCls, paramGroupNo, period, isClose=False):
        self.datetime = datetime
        self.strategyCls = strategyCls
        self.paramGroupNo = paramGroupNo # 策略参数组合编号
        self.period = period  # 策略运行周期, [start tDay, end tDay]

        self.day = None  # 策略现在运行日期

        self.deals = [] # 交易细节的增量
        self.curPos = [] # 持仓

        self.curCash = None
        self.initCash = None

        self.isClose = isClose # 是不是收盘后的Ack


class DyStockBackTestingCommon:
    maViewerIndicator = 'close'


class DyStockBackTestingContext:
    """
        回测时，由于策略@prepare函数会有大量重复的计算，所以需要context缓存数据，提高策略回测效率。
        context给策略提供相应的回测周期和策略参数，然后会传给策略的@prepare函数。
        这样策略可以根据整个回测周期，一次性准备好所有的回测数据，
        然后存到context里(由于python的动态特性，策略可以直接在context里添加成员变量)。
        策略可以利用pandas进行矢量运算，把选股结果针对整个回测周期一次性完成。
        context将会由回测策略引擎保存，每次开盘传给策略@prepare函数。
        策略可以通过hasattr(<userAddedAttr>)来判断有没有一次性准备好所有的回测数据。

        对实盘来讲，context是None，策略可以认为@self.startDate = self.endDate = date，
        这样对于策略来讲，可以回测和实盘统一@prepare函数的内部逻辑实现。
    """
    def __init__(self, period, strategyParam):
        self.startDate, self.endDate = period # 策略回测周期
        self.strategyParam = strategyParam # 来自回测窗口界面的策略回测参数
        