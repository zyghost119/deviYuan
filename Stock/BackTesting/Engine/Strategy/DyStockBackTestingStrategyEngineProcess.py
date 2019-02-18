import queue

from DyCommon.DyCommon import *
from EventEngine.DyEvent import *
from EventEngine.DyEventEngine import *
from ...DyStockBackTestingCommon import *
from .DyStockBackTestingCtaEngine import *
from ....Data.Engine.DyStockDataEngine import *
from Stock.Config.DyStockConfig import DyStockConfig
from Stock.Data.Engine.DyStockDbCache import DyStockDbCache


def __setDbCache(reqData):
    dbCachePreLoadDaysSize = reqData.settings.get('dbCachePreLoadDaysSize')

    useDbCache = False
    if dbCachePreLoadDaysSize is not None:
        useDbCache = True

        # change to the length of period
        if dbCachePreLoadDaysSize == 0:
            dbCachePreLoadDaysSize = len(reqData.tDays)

        DyStockDbCache.preLoadDaysSize = dbCachePreLoadDaysSize

    return useDbCache

def dyStockBackTestingStrategyEngineProcess(outQueue, inQueue, reqData, config=None):
    """
        股票回测处理实体。每个回测处理实体由一个参数组合和一个回测周期组成。
        每个交易日回测结束后向UI推送持仓和成交信息
    """
    paramGroupNo = reqData.paramGroupNo
    period = [reqData.tDays[0], reqData.tDays[-1]]

    if config is not None:
        DyStockConfig.setConfigForBackTesting(config)

    # set DB Cache
    useDbCache = __setDbCache(reqData)

    # Engines
    eventEngine = DyDummyEventEngine()
    info = DySubInfo(paramGroupNo, period, outQueue)
    dataEngine = DyStockDataEngine(eventEngine, info, False, dbCache=useDbCache)

    # create stock back testing CTA engine
    ctaEngine = DyStockBackTestingCtaEngine(eventEngine, info, dataEngine, reqData, dbCache=useDbCache)
    
    for tDay in reqData.tDays:
        try:
            event = inQueue.get_nowait()
        except queue.Empty:
            pass

        # 回测当日数据
        if not ctaEngine.run(tDay):
            break

        # 发送当日回测结果数据事件
        event = DyEvent(DyEventType.stockStrategyBackTestingAck)
        event.data = ctaEngine.getCurAckData()

        outQueue.put(event)

    # 发送'股票回测策略引擎处理结束'事件
    event = DyEvent(DyEventType.stockBackTestingStrategyEngineProcessEnd)
    event.data[paramGroupNo] = period

    outQueue.put(event)
