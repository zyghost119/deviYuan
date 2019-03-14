import pandas as pd

from ....Trade.Strategy.Cta.DyST_TraceFocus import *
from ....Trade.Strategy.DyStockCtaBase import *
from ....Trade.DyStockStrategyBase import *


class DyStockDataFocusAnalysisUtility(object):
    """
        热点分析工具类
        这个类有点特别，会借助DyST_FocusTrace类
    """
    class DummyCtaEngine:
        def __init__(self, eventEngine):
            self.errorInfo = DyErrorInfo(eventEngine)
            self.errorDataEngine = DyStockDataEngine(eventEngine, self.errorInfo, registerEvent=False)
            self.dataEngine = self.errorDataEngine

            self.dummyInfo = DyDummyInfo()
            self.dummyDataEngine = DyStockDataEngine(eventEngine, self.dummyInfo, registerEvent=False)

        def loadPreparedData(self, *args, **kwargs):
            return None

        def tDaysOffsetInDb(self, base, n=0):
            return self.dataEngine.daysEngine.tDaysOffsetInDb(base, n)

        def loadOnClose(self, *args, **kwargs):
            return None

        def putStockMarketMonitorUiEvent(self, *args, **kwargs):
            pass

        def __getattr__(self, name):
            return None


    def _convert2Tick(day, code, name, df):
        """
            @df: 含有'preClose'列
        """
        tick = DyStockCtaTickData()
        
        try:
            s = df.ix[day]

            pos = df.index.get_loc(day)
            if pos == 0:
                return None

        except Exception:
            return None

        tick.code = code
        tick.name = name

        tick.date = day
        tick.time = '15:00:00'
        tick.datetime = datetime.strptime(day + ' 15:00:00', '%Y-%m-%d %H:%M:%S')

        tick.preClose = df.ix[pos - 1, 'close']
        tick.price = s['close']
        tick.open = s['open']
        tick.high = s['high']
        tick.low = s['low']
        tick.volume = s['volume']
        tick.amount = s['amt']

        return tick

    def _convert2Ticks(day, dfs, codeTable):
        ticks = {}
        for code, df in dfs.items():
            tick = DyStockDataFocusAnalysisUtility._convert2Tick(day, code, codeTable[code], df)
            if tick is None:
                continue

            ticks[code] = tick

        return ticks

    def _createFocusStrengthDf(dayIndex, focusInfoPool):
        data = {}
        for focus, focusInfo in focusInfoPool.items():
            data[focus] = [focusInfo.strength]

        df = pd.DataFrame(data, index=[dayIndex])

        return df

    def _initTraceFocusObj(traceFocusObj, date, info, codes, conceptsDict, dummyDaysEngine):
        """
            Initialize prepared data
        """
        # init
        traceFocusObj._curInit(date)

        # we only update UI for first time
        if traceFocusObj._preparedData:
            info = DyDummyInfo()

        # only classify codes not in 'oldStocks' dict
        codes = set(codes) - set(traceFocusObj._preparedData.get('oldStocks', []))
        preparedData = DyST_TraceFocus.classifyCodes(date, codes, info, dummyDaysEngine, conceptsDict)

        # update prepared data of DyST_TraceFocus object
        traceFocusObj._preparedData.setdefault('oldStocks', {}).update(preparedData['oldStocks'])
        traceFocusObj._preparedData['newStocks'] = preparedData['newStocks']

    def _changeTraceFocusObj(traceFocusObj):
        """
            replace dragons in focus info pool by [[code, name]]
        """
        for _, focusInfo in traceFocusObj._focusInfoPool.items():
            focusInfo.dragons = [[code, traceFocusObj._focusCodePool[code].name] for code in focusInfo.dragons]

    def _incrementAnalysis(dummyTraceFocusObj,
                           day,
                           info,
                           codes,
                           dfs,
                           codeTable,
                           conceptsDict,
                           dummyDaysEngine):
        """
            增量分析每日热点，这样只需要增量归类归类股票
        """
        # initialize incremently
        DyStockDataFocusAnalysisUtility._initTraceFocusObj(dummyTraceFocusObj,
                                                           day,
                                                           info,
                                                           codes,
                                                           conceptsDict,
                                                           dummyDaysEngine)

        # push ticks
        ticks = DyStockDataFocusAnalysisUtility._convert2Ticks(day, dfs, codeTable)
        if ticks:
            dummyTraceFocusObj.onTicks(ticks)

            DyStockDataFocusAnalysisUtility._changeTraceFocusObj(dummyTraceFocusObj)

        return dummyTraceFocusObj._focusInfoPool

    def analysis(dfs, indexDfIndex, codeTable, eventEngine, info):
        """
            @dfs: {code: df}, 不含指数
            @indexDfIndex: 对应的指数DF的index
            @return: foucs strength DF, dict of focus info pool
        """
        dummyCtaEngine = DyStockDataFocusAnalysisUtility.DummyCtaEngine(eventEngine)
        dummyTraceFocusObj = DyST_TraceFocus(dummyCtaEngine, dummyCtaEngine.errorInfo, DyStockStrategyState(DyStockStrategyState.backTesting)) # create a dummy instance of DyST_TraceFoucs

        # classify first time
        assert indexDfIndex.size > 1
        codes = list(dfs)
        conceptsDict = DyST_TraceFocus.getConceptsFromFile()
        DyStockDataFocusAnalysisUtility._initTraceFocusObj(dummyTraceFocusObj,
                                                           indexDfIndex[0].strftime("%Y-%m-%d"),
                                                           info,
                                                           codes,
                                                           conceptsDict,
                                                           dummyCtaEngine.dummyDataEngine.daysEngine)

        # focus analysis
        info.print('开始热点分析...', DyLogData.ind)
        progress = DyProgress(info)
        progress.init(indexDfIndex.size)
        
        focusInfoPoolDict = {} # {day: focus info pool}
        focusStrengthDfList = [] # [focus DF of one day]
        for dayIndex in indexDfIndex:
            day = dayIndex.strftime("%Y-%m-%d")

            # analysis incremently
            focusInfoPool = DyStockDataFocusAnalysisUtility._incrementAnalysis(dummyTraceFocusObj,
                                                                               day,
                                                                               info,
                                                                               codes,
                                                                               dfs,
                                                                               codeTable,
                                                                               conceptsDict,
                                                                               dummyCtaEngine.dummyDataEngine.daysEngine)

            focusInfoPoolDict[day] = focusInfoPool
            focusStrengthDfList.append(DyStockDataFocusAnalysisUtility._createFocusStrengthDf(dayIndex, focusInfoPool))

            progress.update()

        # concatenate into DF and 按热点出现次数排序(列排序)
        focusStrengthDf = pd.concat(focusStrengthDfList)

        columns = list(focusStrengthDf.columns)
        columns = sorted(columns, key=lambda x: focusStrengthDf[x].notnull().sum(), reverse=True)
        focusStrengthDf = focusStrengthDf.reindex(columns=columns)

        info.print('热点分析完成', DyLogData.ind)

        return focusStrengthDf, focusInfoPoolDict

    def _analysisProcess(outQueue, days, dayIndexes, info, dummyTraceFocusObj, dfs, codeTable, conceptsDict, dummyDaysEngine):
        """
            以子进程方式分析每日热点
        """
        codes = list(dfs)
        for day, dayIndex in zip(days, dayIndexes):
            # analysis incremently
            focusInfoPool = DyStockDataFocusAnalysisUtility._incrementAnalysis(dummyTraceFocusObj,
                                                                               day,
                                                                               info,
                                                                               codes,
                                                                               dfs,
                                                                               codeTable,
                                                                               conceptsDict,
                                                                               dummyDaysEngine)

            outQueue.put([day, dayIndex, focusInfoPool])
