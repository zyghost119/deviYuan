from time import sleep
from collections import Counter

import tushare as ts

from ..DyStockCtaTemplate import *
from ....Common.DyStockCommon import *
from ....Data.Utility.DyStockDataUtility import *
import DyCommon.DyTalib as DyTalib


class DyST_TraceFocus(DyStockCtaTemplate):
    name = 'DyST_TraceFocus'
    chName = '追踪热点'

    broker = 'simu9'

    #--------------------- 风控相关 ---------------------
    curCodeBuyMaxNbr = 1

    # UI
    maxUiDataRowNbr = 30
    dataHeader = [# 龙头个股信息
                  '代码',
                  '名称',
                  '现价',
                  '涨幅(%)',
                  '涨速(%)', # 最近1分钟涨幅
                  '最高涨幅(%)',
                  '所属行业',
                  '概念',

                  # 热点公共信息
                  '热点', # 若归类到现有热点，一只股票有多个热点，只取强度最大的作为其热点
                  '强度MAX', # 此热点强度的最大值
                  '强度', # 此热点在市场中的强度
                  '成交额(亿元)', # 此热点的成交额
                  '热点涨幅(%)', # 被此热点追踪到的股票的平均涨幅
                  '涨停数',
                  '涨停数占比(%)',
                  '股票数',

                  '龙头涨幅(%)', # 龙一，龙二，龙三的平均涨幅
                  '龙一',
                  '龙二',
                  '龙三',
                  ]

    signalDetailsHeader = ['时间', '操作'] + dataHeader 


    #------------ 策略prepare参数 ------------
    # concept
    prepareConceptFowardNDays = 60
    prepareAsyncCodeSize = 20 # 从网络异步获取股票概念的股票个数，0表示不通过异步获取
    prepareSessions = []

    # technical indicators
    prepareIndicatorFeedNDays = 100
    prepareRsiPeriod = 10
    prepareAtrPeriod = 10

    prepareDaysSize = 20 # 个股准备的日线(OHLCV)数据大小
    prepareVolatilityNDays = 20
    

    #--------------- 策略参数 ---------------
    invalidFocus = ['...', '融资融券', '转融券标的', '沪港通概念', '证金持股', '深港通']
    tickBufTimeSize = 60 # 单位是秒，每只股票缓存x秒的Tick数据
    focusSize = 3 # 一个热点至少有几只股票
    focusStrengthDiscountCodeNbr = 6 # 热点股票数太少，则需要打折热点强度

    # 进入热点股票池的门槛
    doorsillIncrease = 5 # 个股现价涨幅(%)
    doorsillIncreaseSpeed = 1 # 个股最近每@tickBufTimeSize的涨幅(%)

    reclassifyFocus = True # True-根据最新tick重新归类热点，False-归类到现有热点

    # 市场强度
    marketStrengthMiddleUpper = 50
    marketStrengthMiddleLower = 40

    marketStrengthDiscountFocusNbr = 3 # 热点太少，则需要打折市场强度

    focusInfoBufSize = 30 # 热点信息缓存大小


    #------------ 买入信号相关参数 ------------
    # 热点参数
    buySignalFocusStrengthThreshold = 30 # 热点强度
    buySignalFocusCodeNbrThreshold = 10 # 热点股票数
    buySignalFocusRankThreshold = 3 # 热点排名

    buySignalEnableFocusAmountRatioCheck = True
    buySignalFocusAmountRatioThreshold = 2.5 # 热点成交额/市场的总成交额(%)

    # 市场强度参数
    buySignalEnableMarketStrengthCheck = False
    buySignalMarketStrengthThreshold = marketStrengthMiddleLower

    # 单个龙头参数
    buySignalEnableDragonCheck = True # 针对单个龙头进行买入信号检查
    buySignalRsiThreshold = 70 # 防止超买
    buySignalStopProfitThreshold = 1.1
    buySignalVolatilityThreshold = 10
    

    #------------ 卖出信号相关参数 ------------
    sellSignalStopLossThreshold = 0.9


    class MarketStrengthInfo:
        tickSizeList = [30*1, 30*5, 30*10, 30*20, 30*30, 30*60]


        def __init__(self):
            self.cur = None
            self.open = None
            self.max = None
            self.maxTime = None
            self.strongCount = 0
            self.middleCount = 0
            self.weakCount = 0

            self.rollingCounts = OrderedDict() # {最近多少tick: [strong count, middle count, weak count]}

            # rolling buffer
            self.timeList = []
            self.strengthList = []

        def copy(self):
            newStrengthInfo = copy.copy(self)

            newStrengthInfo.rollingCounts = copy.copy(self.rollingCounts)

            return newStrengthInfo

        def update(self, tick, strength):
            if tick is None:
                return

            self.cur = strength

            if self.max is None or strength > self.max:
                self.max = strength
                self.maxTime = tick.time

            if '09:25:00' <= tick.time < '09:30:00':
                self.open = strength

            if strength > DyST_TraceFocus.marketStrengthMiddleUpper:
                self.strongCount += 1
            elif strength < DyST_TraceFocus.marketStrengthMiddleLower:
                self.weakCount += 1
            else:
                self.middleCount += 1

            # update rolling counts
            self._updateRollingCounts(tick, strength)

        def _updateRollingCounts(self, tick, strength):
            """
                只统计连续时间的tick市场强度
            """
            # 不考虑开盘前的时间段
            if '09:25:00' <= tick.time < '09:30:00':
                return

            # add and wrangle
            self.timeList.insert(0, tick.datetime)
            self.strengthList.insert(0, strength)

            self.timeList[:] =  self.timeList[:self.tickSizeList[-1]]
            self.strengthList[:] =  self.strengthList[:self.tickSizeList[-1]]

            # count
            strongCount, middleCount, weakCount = 0, 0, 0
            for i, (time, strength) in enumerate(zip(self.timeList, self.strengthList)):
                if strength > DyST_TraceFocus.marketStrengthMiddleUpper:
                    strongCount += 1
                elif strength < DyST_TraceFocus.marketStrengthMiddleLower:
                    weakCount += 1
                else:
                    middleCount += 1

                if i+1 not in self.tickSizeList:
                    continue

                # 刨去上午收盘时间段
                if tick.datetime.hour > 12 and time.hour < 12:
                    adjustSeconds = 90*60
                else:
                    adjustSeconds = 0

                seconds = (tick.datetime - time).total_seconds()
                seconds = int(seconds) - adjustSeconds
                deltaTime = '{0}m{1}s'.format(seconds//60, seconds%60)

                # new list, so don't need deepcopy
                self.rollingCounts[i+1] = [deltaTime, strongCount, middleCount, weakCount]


    class FocusInfo:
        """
            热点公共信息
            对于没有形成热点的股票，则归为@name为'无'的FocusInfo
        """
        def __init__(self, name):
            self.name = name # 热点名字
            self.codes = [] # 此热点的股票代码列表, 必须是非空list
            self.increase = None # 被此热点追踪到的股票的平均涨幅(%)
            self.limitUpNbr = None # 涨停数
            self.amount = 0 # 成交额(亿元)

            self.strength = None # 热点强度，可以为负值

            # 龙一，二，三
            self.dragons = None # [code], 长度不超过3
            self.dragonIncrease = None


    class FocusCodeInfo:
        """
            热点单只股票信息
        """
        def __init__(self, code, name, highTick, ticks):
            self.code = code
            self.name = name

            self.highTick = highTick
            self.ticks = ticks # 最近缓存的ticks, [tick], 最近的在表尾

            self.industry = None
            self.selfConcepts = None # [concept], 股票自身概念，主要区别是包含概念排名，比如锂电池(一)
            self.concepts = None # [concept], 去除排名后的概念，由于是list，所以其实可以分出概念排名，如果有排名的话
            
            self.focusInfo = None


    def __init__(self, ctaEngine, info, state, strategyParam=None):
        super().__init__(ctaEngine, info, state, strategyParam)

    def _onOpenConfig(self):
        """
            配置开盘前载入好的数据
        """
        self._monitoredStocks.extend(list(self._preparedData['newStocks']) + list(self._preparedData['oldStocks']))

        # For time of market strength.
        # Actually index tick should be used, but in backtesting mode, there's no index history tick data. So we use ETF300 tick.

    def _curInit(self, date=None):
        self._codeTicks = {} # 每只股票的Tick列表，{code: [[buffered ticks], high tick, low tick]}，按时间周期缓存tick

        self._focusCodePool = {} # 热点股票代码池，{code: FocusCodeInfo}
        self._focusInfoPool = {} # 热点信息池，{热点: FocusInfo}
        self._focusStrengthMax = {} # {focus: max strength}，若采用重新归类法，会清空@self._focusInfoPool

        self._focusInfoPoolBuf = {} # 热点信息池缓存，{热点: [FocusInfo]}

        self._monitorBuyFocusPool = {} # 监控可能买入的热点池，{focus: {code}}

        self._uiDataDict = {} # 策略UI数据字典，{code: [row]}

        self._curFocusBuyList = [] # 当日已经买入的热点

        self._marketStrengthInfo = self.MarketStrengthInfo()

        self._totalAmount = 0 # 股票池的当日成交额(亿元)

    @DyStockCtaTemplate.onOpenWrapper
    def onOpen(self, date, codes=None):
        # 当日初始化
        self._curInit(date)

        # 配置开盘前载入好的数据
        self._onOpenConfig()

        return True

    @DyStockCtaTemplate.onCloseWrapper
    def onClose(self):
        """
            策略每天收盘后的数据处理（由用户选择继承实现）
            持仓数据由策略模板类负责保存
            其他收盘后的数据，则必须由子类实现（即保存到@self._curSavedData）
        """
        #self._curSavedData['focus'] = self._focusInfoPool
        pass

    def _processPreparedPosDataAdj(self, tick):
        """
            处理持仓准备数据除复权
        """
        code = tick.code

        posData = self._preparedPosData.get(code)
        if posData is None:
            return True

        preClose = posData['preClose']

        # 已经处理过除复权或者股票当日没有除权除息
        if tick.preClose == preClose:
            return True

        # 复权因子
        adjFactor = tick.preClose/preClose

        # channel
        data = posData['channel']
        data[:] = list(map(lambda x, y: x*y, data, [adjFactor]*len(data)))

        # set back, so that we don't need to do it next time
        posData['preClose'] = tick.preClose

        return True

    def _processPreparedDataAdj(self, tick):
        """
            处理准备数据除复权
        """
        code = tick.code

        # preClose of code
        preCloses = self._preparedData.get('preClose')
        if preCloses is None:
            return True

        preClose = preCloses.get(code)
        if preClose is None:
            return True

        # 已经处理过除复权或者股票当日没有除权除息
        if tick.preClose == preClose:
            return True

        # 复权因子
        adjFactor = tick.preClose/preClose

        # channel
        data = self._preparedData['channel'].get(code)
        if data is None:
            return True

        data[:] = list(map(lambda x, y: x*y, data, [adjFactor]*len(data)))

        # RSI
        data = self._preparedData['rsi'].get(code)
        if data is None:
            return True

        data *= adjFactor
        self._preparedData['rsi'][code] = data

        # OHLCV
        data = self._preparedData['days'].get(code)
        if data is None:
            return True

        for dayData in data:
            for i in range(4): # price related
                dayData[i] *= adjFactor

            dayData[-1] /= adjFactor # volume

        # set back, so that we don't need to do it next time
        preCloses[code] = tick.preClose

        return True

    def _processAdj(self, tick):
        """
            处理除复权
                - 持仓准备数据
                - 准备数据
        """
        if not self._processPreparedPosDataAdj(tick):
            return False

        return self._processPreparedDataAdj(tick)

    def _doorsillCheck(self, tick):
        """
            对不在热点股票代码字典的股票做门槛检查
            @return: True-热点股票，False-非热点股票
        """
        ticks, highTick, lowTick = self._codeTicks[tick.code]

        #----- 次新股 -----
        if tick.code in self._preparedData['newStocks']:
            if highTick.price == lowTick.price: # 去除当日还是一字板，当日不是一字板的次新股则认为是老股
                return False

        #----- 老股 -----
        # 现价涨幅
        increase = (tick.price - tick.preClose)/tick.preClose*100
        if increase > self.doorsillIncrease:
            return True

        # 涨速
        if increase <= 0:
            return False

        preIncrease = (ticks[0].price - tick.preClose)/tick.preClose*100 # 缓存的第一个tick的涨幅
        seconds = (tick.datetime - ticks[0].datetime).total_seconds()
        if seconds < self.tickBufTimeSize*0.6: # 保证有一定时间的tick缓存，因为tick是逐个缓存的
            return False

        increaseSpeed = (increase - preIncrease)/seconds*self.tickBufTimeSize
        if increaseSpeed > self.doorsillIncreaseSpeed:
            return True

        return False

    def _addTick(self, tick):
        """
            添加每只股票的Tick数据到缓存数据里
        """
        ticksList = self._codeTicks.get(tick.code)
        if ticksList is None:
            self._codeTicks[tick.code] = [[tick], tick, tick] # [[buffered tick], high tick, low tick]
            return

        # 是否是最新更新过的tick
        if tick.time == ticksList[0][-1].time:
            return

        # update high tick
        if tick.price > ticksList[1].price: # high tick，只是大于，这样可以记录第一次封涨停板的tick
            ticksList[1] = tick

        # update low tick
        if tick.price < ticksList[2].price: # low tick，只是小于，这样可以记录第一次封跌停板的tick
            ticksList[2] = tick

        # 添加到tick buffer
        ticks = ticksList[0]
        for i, tick_ in enumerate(ticks):
            if (tick.datetime - tick_.datetime).total_seconds() < self.tickBufTimeSize:
                break

        else: # 所有的tick跟当前tick时间相差太大，则取最后一个tick
            i += 1

        ticks[:] = ticks[i - len(ticks) - 1:]
        ticks.append(tick)

    def _enterDoorsill(self, tick):
        """
            对每只股票的tick做进入热点门槛的检查，数据保存和更新，同时缓存tick。
        """
        # 缓存Tick
        self._addTick(tick)

        # 已经在热点股票池了，则更新high tick
        # no need to update tick buffer
        focusCodeInfo = self._focusCodePool.get(tick.code)
        if focusCodeInfo is not None:
            focusCodeInfo.highTick = self._codeTicks[tick.code][1]
            return

        if not self._doorsillCheck(tick):
            return

        focusCodeInfo = self.FocusCodeInfo(tick.code, tick.name, self._codeTicks[tick.code][1], self._codeTicks[tick.code][0])
        self._focusCodePool[tick.code] = focusCodeInfo

    def _getIndustryConcept(self, code):
        conceptList = self._preparedData['oldStocks'].get(code)
        if conceptList is None:
            conceptList = self._preparedData['newStocks'].get(code)

        return [None, None] if conceptList is None else conceptList  # [所属行业, [概念]]

    def _setConcepts(self):
        """
            设置热点股票池里没有概念的股票
        """
        # 哪些股票还没有概念信息
        invalidCodes = [] # 没有公司信息的股票
        for code, focusCodeInfo in self._focusCodePool.items():
            if focusCodeInfo.concepts is not None:
                continue

            # get from prepared data
            industry, conceptList = self._getIndustryConcept(code)
            if industry is None: # think it as invalid
                invalidCodes.append(code)
                continue

            # 去除无效热点
            selfConcepts = []
            onlyConcepts = []
            for concept in conceptList:
                conceptTemp = concept[:-3] if concept[-1] == ')' else concept
                if conceptTemp in self.invalidFocus:
                    continue

                selfConcepts.append(concept)
                onlyConcepts.append(conceptTemp)

            # save
            focusCodeInfo = self._focusCodePool[code]
            focusCodeInfo.industry = industry
            focusCodeInfo.selfConcepts = selfConcepts
            focusCodeInfo.concepts = onlyConcepts

        # remove invalid codes from focus code pool
        for code in invalidCodes:
            del self._focusCodePool[code]

    def _classifyFocus(self, reclassifyFocus=False):
        """
            将股票归类热点
            @reclassifyFocus：True-重新归类, False-归类到现有热点
            @return: [no focus code]
        """
        if reclassifyFocus:
            # clear already classified focus
            for _, focusCodeInfo in self._focusCodePool.items():
                focusCodeInfo.focusInfo = None

            self._focusInfoPool = {}

            noFocusCodes = list(self._focusCodePool)

        else:
            # 按热点强度依次归类
            focusList = sorted(self._focusInfoPool, key=lambda k: self._focusInfoPool[k].strength, reverse=True)

            noFocusCodes = []
            for code, focusCodeInfo in self._focusCodePool.items():
                if focusCodeInfo.focusInfo is not None:
                    continue

                for focus in focusList:
                    if focus in focusCodeInfo.concepts:
                        focusCodeInfo.focusInfo = self._focusInfoPool[focus]
                        self._focusInfoPool[focus].codes.append(code)
                        break

                else: # 不能归为现有热点
                    noFocusCodes.append(code)

        return noFocusCodes

    def _newFocus(self, noFocusCodes):
        """
            根据无热点股票创建新的热点
            无热点的股票归为'无'热点Info
        """
        # 重新归类无热点股票
        noFocusInfo = self._focusInfoPool.get('无')
        if noFocusInfo is not None:
            noFocusCodes.extend(noFocusInfo.codes)
            del self._focusInfoPool['无']

        # 根据概念出现次数进行降序排序
        allConcepts = []
        for code in noFocusCodes:
            allConcepts.extend(self._focusCodePool[code].concepts)

        conceptDict = dict(Counter(allConcepts))
        conceptList = sorted(conceptDict, key=lambda k: conceptDict[k], reverse=True)

        # 热点归类，可能出现某些股票无法归类到任何热点
        for concept in conceptList: # 按热点出现次数由多到少匹配
            if not noFocusCodes: # all no focus codes are classified to focus
                break

            if conceptDict[concept] == 1: # 对于热点只有一个股票，则认为不是热点
                break

            # new FocusInfo instance
            focusInfo = self.FocusInfo(concept)

            for code in noFocusCodes:
                # 热点归类成功
                if concept in self._focusCodePool[code].concepts:
                    # 相互关联
                    focusInfo.codes.append(code)
                    self._focusCodePool[code].focusInfo = focusInfo

            # only think number of focus codes greater than or equal to @self.focusSize as a focus
            if len(focusInfo.codes) >= self.focusSize:
                for code in focusInfo.codes: # remove already focus classified codes
                    noFocusCodes.remove(code)

                # save
                self._focusInfoPool[concept] = focusInfo
            else:
                for code in focusInfo.codes: # unlink code and focus
                    self._focusCodePool[code].focusInfo = None
        
        # 没法归类到热点的股票则归类到无热点
        if noFocusCodes:
            focusInfo = self.FocusInfo('无')
            self._focusInfoPool['无'] = focusInfo
            for code in noFocusCodes:
                focusInfo.codes.append(code)
                self._focusCodePool[code].focusInfo = focusInfo

    def _estimateAllFocusStrength(self):
        """
            全盘评估热点强度：
                热点涨幅
                热点龙头涨幅
        """
        for focus, focusInfo in self._focusInfoPool.items():
            # 热点涨幅
            focusIncreaseScore = focusInfo.increase*10
            if focusIncreaseScore >= DyStockCommon.limitUpPct*10:
                focusIncreaseScore = 100

            # 热点龙头涨幅
            dragonIncreaseScore = focusInfo.dragonIncrease*10
            if dragonIncreaseScore >= DyStockCommon.limitUpPct*10:
                dragonIncreaseScore = 100

            # 热点涨幅不是跌时，则考虑龙头的涨幅，否则以热点涨幅作为其强度。
            # 热点强度可以为负值。
            if focusInfo.increase >= 0:
                focusInfo.strength = round((focusIncreaseScore + dragonIncreaseScore)/2)

                # 热点强度打折
                focusInfo.strength *= len(focusInfo.codes)/max(len(focusInfo.codes), self.focusStrengthDiscountCodeNbr)
            else:
                focusInfo.strength = focusIncreaseScore

            # save MAX focus strength
            maxStrength = self._focusStrengthMax.get(focus, -100)
            self._focusStrengthMax[focus] = max(maxStrength, focusInfo.strength)

            # 缓存focusInfo
            buf = self._focusInfoPoolBuf.setdefault(focus, [])
            buf.append(copy.deepcopy(focusInfo))
            buf[:] = buf[-self.focusInfoBufSize:]
        
    def _estimateOneFocus(self, focusInfo):
        """
            评估一个热点
        """
        #---------- 统计热点信息 ----------
        totalIncrease = 0
        limitUpCodes = [] # 涨停股票代码列表，由于涨停时的涨幅不一样，所以单独一个list
        nonLimitUpCodes = {} # {increase: [code]}
        amount = 0 # 热点成交额
        for code in focusInfo.codes:
            # 涨幅
            tick = self._focusCodePool[code].ticks[-1]
            increase = (tick.price - tick.preClose)/tick.preClose*100
            increase = round(increase, 2) # 只取小数点两位，四舍五入

            totalIncrease += increase

            # 涨停
            if increase >= DyStockCommon.limitUpPct:
                limitUpCodes.append(code)
            else:
                nonLimitUpCodes.setdefault(increase, []).append(code)

            # 成交额
            amount += tick.amount/10**8

        focusInfo.increase = totalIncrease/len(focusInfo.codes)
        focusInfo.limitUpNbr = len(limitUpCodes)
        focusInfo.amount = amount

        #---------- 龙头股分类 ----------
        focusInfo.dragons = None # reset

        # 先分类涨停股
        highTicks = {code: self._focusCodePool[code].highTick for code in limitUpCodes}
        codes = sorted(highTicks, key=lambda k: highTicks[k].time) # 根据封板时间排序

        focusInfo.dragons = codes[:3] # 龙一，二，三

        # 还有龙头空间，则考虑非涨停股，优先考虑现价涨幅大的
        if len(focusInfo.dragons) < 3:
            increaseList = sorted(nonLimitUpCodes, reverse=True)
            for increase in increaseList:
                dragonSpace = 3 - len(focusInfo.dragons)

                ticks = {code: self._focusCodePool[code].ticks[-1] for code in nonLimitUpCodes[increase]}
                codes = sorted(ticks, key=lambda k: ticks[k].time) # 若现价涨幅相同，则根据时间排序

                # 添加到龙头股list
                focusInfo.dragons[len(focusInfo.dragons):] = codes[:dragonSpace]
                if len(focusInfo.dragons) == 3:
                    break

        # 龙头涨幅
        totalIncrease = 0
        for code in focusInfo.dragons:
            # 涨幅
            tick = self._focusCodePool[code].ticks[-1]
            increase = (tick.price - tick.preClose)/tick.preClose*100
            increase = round(increase, 2) # 只取小数点两位，四舍五入

            totalIncrease += increase

        focusInfo.dragonIncrease = totalIncrease/len(focusInfo.dragons)

    def _estimateAllFocus(self):
        """
            全盘评估和汇总热点
        """
        # 逐个评估每个热点
        for _, focusInfo in self._focusInfoPool.items():
            self._estimateOneFocus(focusInfo)

        # 全盘评估热点强度
        self._estimateAllFocusStrength()
                    
    def _traceFocus(self):
        """
            根据最新行情全盘追踪热点，并更新热点数据
        """
        # 设置热点股票的概念
        self._setConcepts()

        # 归类到现有热点
        noFocusCodes = self._classifyFocus(self.reclassifyFocus)

        # 还没有归类到热点的股票，生成新热点
        if noFocusCodes:
            self._newFocus(noFocusCodes)

        # 全盘评估热点
        self._estimateAllFocus()

        # 更新UI
        self._updateUi()

    def _updateUi(self):
        """
            更新UI的数据窗口
        """
        def _getDragonsData(focusInfo):
            data = [None]*3
            if not focusInfo.dragons:
                return data

            for i, code in enumerate(focusInfo.dragons):
                data[i] = self._focusCodePool[code].name

            return data


        self._uiDataDict = {}

        # 按热点强度依次归类
        data = []
        focusList = sorted(self._focusInfoPool, key=lambda k: self._focusInfoPool[k].strength, reverse=True)
        for focus in focusList:
            focusInfo = self._focusInfoPool[focus]

            # 热点公共信息
            rowFocus = [focusInfo.name,
                        self._focusStrengthMax[focus],
                        focusInfo.strength,
                        focusInfo.amount,
                        focusInfo.increase,
                        focusInfo.limitUpNbr,
                        focusInfo.limitUpNbr/len(focusInfo.codes)*100,
                        len(focusInfo.codes),

                        # 龙头
                        focusInfo.dragonIncrease
                        ]
            rowFocus.extend(_getDragonsData(focusInfo))
                     
            # 个股信息，这里只更新龙头股的信息   
            for code in focusInfo.dragons:
                focusCodeInfo = self._focusCodePool[code]

                highIncrease = (focusCodeInfo.highTick.price - focusCodeInfo.highTick.preClose)/focusCodeInfo.highTick.preClose*100

                increase = (focusCodeInfo.ticks[-1].price - focusCodeInfo.ticks[-1].preClose)/focusCodeInfo.ticks[-1].preClose*100
                preIncrease = (focusCodeInfo.ticks[0].price - focusCodeInfo.ticks[0].preClose)/focusCodeInfo.ticks[0].preClose*100

                seconds = (focusCodeInfo.ticks[-1].datetime - focusCodeInfo.ticks[0].datetime).total_seconds()
                increaseSpeed = (increase - preIncrease)/seconds*self.tickBufTimeSize if seconds > 0 else None

                rowCode = [code,
                           focusCodeInfo.name,
                           focusCodeInfo.ticks[-1].price,
                           increase,
                           increaseSpeed,
                           highIncrease,
                           focusCodeInfo.industry,
                           ','.join(focusCodeInfo.selfConcepts[:3]) + ('[%d]'%len(focusCodeInfo.selfConcepts) if len(focusCodeInfo.selfConcepts) > 3 else '')
                           ]

                # save data
                rowData = rowCode + rowFocus
                data.append(rowData)
                self._uiDataDict[code] = rowData

        self.putStockMarketMonitorUiEvent(data=data, newData=True, datetime_=self.marketDatetime)

    def _calcFocusBuySignal(self, focus):
        """
            维护热点买入监控池并计算热点买入信号
            @return: bool
        """
        if focus == '无':
            return False

        if focus in self._curFocusBuyList:
            return False

        # 热点成交额
        if self.buySignalEnableFocusAmountRatioCheck:
            if self._focusInfoPool[focus].amount/self._totalAmount*100 < self.buySignalFocusAmountRatioThreshold:
                return False

        # 热点股票数
        #if len(self._focusInfoPool[focus].codes) < self.buySignalFocusCodeNbrThreshold:
        #    return False

        # 热点强度阈值
        #if self._focusInfoPool[focus].strength < self.buySignalFocusStrengthThreshold:
        #    return False

        # 热点强度动量
        focusInfoBuf = self._focusInfoPoolBuf[focus]
        if len(focusInfoBuf) < self.focusInfoBufSize:
            return False

        focusInfoBuf_ = focusInfoBuf[::-5][::-1]
        for i in range(1, len(focusInfoBuf_)):
            if focusInfoBuf_[i].strength < focusInfoBuf_[i-1].strength:
                return False

        # 计算哪些股票进入可买入热点监控池
        codes = set()
        for focusInfo_ in focusInfoBuf_:
            for dragon in focusInfo_.dragons:
                codes.add(dragon)

        self._monitorBuyFocusPool[focus] = codes

        return True

    def _calcDragonBuySignal(self, tick):
        """
            计算单个龙头买入信号
            @return: bool
        """
        if not self.buySignalEnableDragonCheck:
            return True

        dragon = tick.code

        # ST股
        if 'ST' in tick.name:
            return False

        if (tick.price - tick.preClose)/tick.preClose*100 < 5:
            return False

        """
        # channel
        channel = self._preparedData['channel'].get(dragon)
        if channel is None:
            return False

        upper, lower = channel

        if tick.price <= upper:
            return False

        # RSI
        rsi = self._preparedData['rsi'].get(dragon)
        if rsi is None:
            return False

        if rsi > self.buySignalRsiThreshold:
            return False
        """

        # 现价为最高价
        """
        if tick.price < tick.high:
            return False
        """

        # 波动率
        """
        volatility = self._preparedData['volatility'].get(dragon)
        if volatility is None:
            return False

        if volatility > self.buySignalVolatilityThreshold:
            return False
        """

        # 突破指定周期内的最大值
        """
        days = self._preparedData['days'].get(dragon) # OHLCV
        if days is None:
            return False

        high = max([day[1] for day in days])
        if tick.price <= high:
            return False
        """

        # time
        """
        if not ('10:00:00' <= tick.time <= '11:00:00' or '13:30:00' <= tick.time <= '14:30:00'):
            return False
        """

        # K线
        """
        days = self._preparedData['days'].get(dragon) # OHLCV
        if days is None:
            return False

        redLineCount = 0
        for day in days:
            if day[-2] >= day[0]:
                redLineCount += 1

        if redLineCount/len(days) < 0.5:
            return False
        """

        # 大幅高开
        """
        if (tick.open - tick.preClose)/tick.preClose*100 >= 5:
            return False
        """

        # 曾经涨停
        #if (tick.high - tick.preClose)/tick.preClose*100 >= DyStockCommon.limitUpPct:
        #    return False

        return True

    def _calcFocusDragonsBuySignal(self, ticks, focus):
        """
            计算热点所有龙头的买入信号
            @return: {dragon: 买入强度}
        """
        # 从龙头中挑选突破力度最大的
        breakoutDragons = {}
        for dragon in self._monitorBuyFocusPool.get(focus):
            tick = ticks.get(dragon)
            if tick is None:
                continue

            # 计算龙头买入信号
            if not self._calcDragonBuySignal(tick):
                continue

            #upper, lower = self._preparedData['channel'].get(dragon)

            breakoutDragons[dragon] = (tick.price - tick.preClose)/tick.preClose*100

        return breakoutDragons

    def _estimateFocusDragonsBuySignal(self, ticks, focus, breakoutDragons):
        """
            评估热点所有龙头的买入信号并保存
            同一热点只买入一个龙头
            @return: dragon
        """
        dragonList = sorted(breakoutDragons, key=lambda k: breakoutDragons[k], reverse=True)
        for dragon in dragonList:
            buyCount = self._curCodeBuyCountDict.get(dragon, 0)
            if buyCount > 0:
                continue

            if dragon in self._curPos:
                continue

            if self.canBuy(ticks.get(dragon)):
                self._curFocusBuyList.append(focus)
                return dragon

        return None

    def _calcMarketStrengthBuySignal(self):
        """
            计算市场强度买入信号
        """
        if not self.buySignalEnableMarketStrengthCheck:
            return True

        if self._marketStrengthInfo.cur < self.buySignalMarketStrengthThreshold:
            return False

        return True

    def _calcBuySignal(self, ticks):
        """
            计算买入信号
        """
        buyCodes = []

        # 按热点强度排序
        focusListByStrength = sorted(self._focusInfoPool, key=lambda k: self._focusInfoPool[k].strength, reverse=True)
        if '无' in focusListByStrength: # 2017.10.9
            focusListByStrength.remove('无')

        for focus in focusListByStrength[:self.buySignalFocusRankThreshold]:
            # 计算热点买入信号
            if not self._calcFocusBuySignal(focus):
                continue

            if not self._calcMarketStrengthBuySignal():
                continue

            # 计算热点所有龙头的买入信号
            breakoutDragons = self._calcFocusDragonsBuySignal(ticks, focus)
            if not breakoutDragons:
                continue

            # 评估热点所有龙头的买入信号
            dragon = self._estimateFocusDragonsBuySignal(ticks, focus, breakoutDragons)
            if dragon is None:
                continue

            buyCodes.append(dragon)

        return buyCodes

    def _calcEtfVolatility(self, code):
        """
            计算ETF波动率
            @return: etf volatility(%)
        """
        tick = self.getEtfTick(code)
        if tick is None:
            return None

        highVolatility = (tick.high - tick.preClose)/tick.preClose
        lowVolatility = (tick.low - tick.preClose)/tick.preClose
        highLowVolatility = highVolatility - lowVolatility

        trueVolatility = max(abs(highVolatility), abs(lowVolatility), abs(highLowVolatility))
        return trueVolatility*100

    def _calcEtfVolatilityFactor(self, code):
        curVolatility = self._calcEtfVolatility(code)
        if curVolatility is None:
            return None

        volatility = self._preparedData.get('etfVolatility')
        if volatility is None:
            return None

        volatility = volatility.get(code)
        if volatility is None:
            return None

        return curVolatility/volatility

    def _calcSellSignal(self, ticks):
        """
            计算卖出信号
        """
        sellCodes = []
        for code, pos in self._curPos.items():
            if pos.availVolume == 0:
                continue

            tick = ticks.get(code)
            if tick is None:
                continue

            if tick.volume == 0:
                continue

            data = self._preparedPosData.get(code)
            if data is None:
                continue

            # 计算ETF波动率
            if code[:3] in ['002', '300']:
                etfCode = DyStockCommon.etf500
            else:
                etfCode = DyStockCommon.etf300

            etfVolatilityFactor = self._calcEtfVolatilityFactor(etfCode)
            if etfVolatilityFactor is None:
                continue

            # 限定通道宽度范围
            factor = etfVolatilityFactor
            if factor < 1.191:
                factor = 1.191
            elif factor > 2:
                factor = 2

            # 根据ETF波动调整通道宽度
            upper, lower = data['channel']
            middle = (upper + lower)/2
            channelWidth = upper - middle

            upper, lower = middle + factor*channelWidth, middle - factor*channelWidth

            if tick.price < lower or tick.price > upper or \
                tick.price < pos.cost*self.sellSignalStopLossThreshold or \
                tick.price > pos.cost*self.buySignalStopProfitThreshold:
                sellCodes.append(code)

        return sellCodes

    def _calcSignal(self, ticks):
        """
            计算信号
            @return: [buy code], [sell code]
        """
        return self._calcBuySignal(ticks), self._calcSellSignal(ticks)

    def _execBuySignal(self, buyCodes, ticks):
        """
            执行买入信号
        """
        for code in buyCodes:
            tick = ticks.get(code)
            if tick is None:
                continue

            row = self._uiDataDict.get(code)
            if row is None:
                row = [code, tick.name, tick.price] + [None]*(len(self.dataHeader) - 3)

            signalDetails = [tick.time, '买入'] + row
            self.buyByRatio(tick, 10, self.cAccountCapital, signalDetails=signalDetails)

    def _execSellSignal(self, sellCodes, ticks):
        """
            执行卖出信号
        """
        for code in sellCodes:
            self.closePos(ticks.get(code))

    def _execSignal(self, buyCodes, sellCodes, ticks):
        """
            执行信号
        """
        self._execBuySignal(buyCodes, ticks)
        self._execSellSignal(sellCodes, ticks)

    def _procSignal(self, ticks):
        """
            处理买入和卖出信号
        """
        buyCodes, sellCodes = self._calcSignal(ticks)

        self._execSignal(buyCodes, sellCodes, ticks)

    def _calcMarketStrength(self):
        """
            计算市场强度
        """
        # 按热点强度排序
        focusList = sorted(self._focusInfoPool, key=lambda k: self._focusInfoPool[k].strength, reverse=True)

        # 去除热点'无'
        if '无' in focusList:
            focusList.remove('无')

        # 前三热点强度均值
        strengthTotal = 0
        first3FocusList = focusList[:3]
        for focus in first3FocusList:
            strengthTotal += self._focusInfoPool[focus].strength

        first3FocusStrengthMean = 0
        if len(first3FocusList) > 0:
            first3FocusStrengthMean = strengthTotal/len(first3FocusList)

        # 热点强度均值
        strengthTotal = 0
        for focus in focusList:
            strengthTotal += self._focusInfoPool[focus].strength

        focusStrengthMean = 0
        if len(focusList) > 0:
            focusStrengthMean = strengthTotal/len(focusList)

        # 市场强度
        marketStrength = (first3FocusStrengthMean + focusStrengthMean)/2

        # 市场强度打折
        marketStrength *= len(focusList)/max(len(focusList), self.marketStrengthDiscountFocusNbr)

        # upate object
        self._marketStrengthInfo.update(self.etf300Tick, marketStrength)

        # put event
        self.putStockMarketStrengthUpdateEvent(self.etf300Tick.time if self.etf300Tick else None, self._marketStrengthInfo)

    #@DyTime.instanceTimeitWrapper
    def onTicks(self, ticks):
        """
            收到行情TICKs推送
            @ticks: {code: DyStockCtaTickData}
        """
        self._totalAmount = 0
        for code, tick in ticks.items():
            # 停牌
            if tick.volume == 0:
                continue

            # 处理除复权
            if not self._processAdj(tick):
                continue

            # 市场的总成交额(亿元)
            self._totalAmount += tick.amount/10**8

            # 进入热点股票门槛
            self._enterDoorsill(tick)

        # 全盘追踪热点
        self._traceFocus()

        # 计算市场强度
        self._calcMarketStrength()

        # 处理买入和卖出信号
        self._procSignal(ticks)

    def onBars(self, bars):
        raise Exception("策略[{}]只支持Tick级别的运行方式".format(self.chName))


    ######################################## 开盘前的数据准备 ########################################
    ######################################## 开盘前的数据准备 ########################################
    @classmethod
    def classifyCodes(cls, date, codes, info, errorDaysEngine, conceptsDict):
        info.print('开始股票归类...', DyLogData.ind)
        forwardNDays = cls.prepareConceptFowardNDays # including @date，新股上市考虑的周期大小，即认为新股上市不可能有连续这么多的一字板涨停

        progress = DyProgress(info, printConsole=True)
        progress.init(len(codes), 100, 5)

        newStocks, oldStocks = {}, {}
        for code in codes:
            if errorDaysEngine.loadCode(code, [date, -forwardNDays + 1], latestAdjFactorInDb=False):
                df = errorDaysEngine.getDataFrame(code)
                if df is not None:
                    concept = conceptsDict.get(code)
                    if concept is not None:
                        # old stocks
                        if df.shape[0] == forwardNDays:
                            oldStocks[code] = concept

                        else: # 次新股
                            df = df[1:] # 剔除第一个交易日
                            if df.empty:
                                newStocks[code] = concept
                            else:
                                if (df['high'] != df['low']).sum() == 0:
                                    newStocks[code] = concept
                                else:
                                    oldStocks[code] = concept

            progress.update()

        info.print('股票归类完成', DyLogData.ind)

        return {'newStocks': newStocks, 'oldStocks': oldStocks}

    @classmethod
    def getConceptsFromFile(cls):
        path = DyCommon.createPath('Stock/Program/Strategy/{}'.format(cls.chName))

        for _, dirnames, _ in os.walk(path):
            if not dirnames:
                break

            # 遍历找到最新的数据
            for dirname in dirnames[::-1]:
                fileName = os.path.join(path, dirname, 'preparedData.json')

                try:
                    with open(fileName) as f:
                        data = json.load(f)

                        return dict(data['newStocks'], **data['oldStocks'])
                except:
                    pass

        return None

    @classmethod
    def __getConceptDetailsFromTuSharePro(cls, pro):
        concepts = {} # {code: [concept]}

        # 概念股分类
        sleep(1)
        print("TuSharePro: 获取概念股分类...")
        df = pro.concept()
        conceptsTemp = df.values.tolist()
    
        # 概念股列表
        sleepTime = 1
        for i, (conceptCode, conceptName, _) in enumerate(conceptsTemp, 1):
            # get from tusharepro
            for _ in range(3):
                sleep(sleepTime)
                print("TuSharePro: 获取概念[{}]...({}/{})".format(conceptName, i, len(conceptsTemp)))
                try:
                    df = pro.concept_detail(id=conceptCode, fields='ts_code')
                except Exception as ex:
                    exStr = 'TuSharePro: 获取概念异常: {}, retry...'.format(ex)
                    print(exStr)
                    if '最多访问' in exStr:
                        # Here we don't analyze accurately, just think limitation happened like "每分钟最多访问该接口60次"
                        sleepTime += 0.2
                        print('TuSharePro: Sleep 60s, and then retry with @sleepTime={} ...'.format(sleepTime))
                        sleep(60)
                else:
                    break
            else:
                raise

            # append
            for stockCode, in df.values.tolist():
                conceptList = concepts.setdefault(stockCode, [])
                if conceptName in conceptList:
                    print('{}: 概念[{}] already existing'.format(stockCode, conceptName))
                    continue

                conceptList.append(conceptName)

        return concepts

    @classmethod
    def __getIndustriesFromTuSharePro(cls, pro):
        industries = {} # {code: industry}

        print("TuSharePro: 获取股票所属行业...")

        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,industry')
        for code, industry in df.values.tolist():
            industries[code] = industry

        return industries

    @classmethod
    def __getConceptsFromTuSharePro(cls, pro):
        """
            @return: {code: [所属行业, [概念]]}
        """
        industries = cls.__getIndustriesFromTuSharePro(pro)
        concepts = cls.__getConceptDetailsFromTuSharePro(pro)

        # combine
        conceptsDict = {}
        for code, industry in industries.items():
            conceptsDict[code] = [industry, concepts.get(code, [])]

        return conceptsDict

    @classmethod
    def __getConcepts(cls, codeTable, info, isBackTesting):
        """
            从TuSharePro获取股票概念
        """
        conceptsDict = None
        needSaved2File = False # 回测时使用，这样没必要每次都网上抓取，节省时间。本策略回测，概念数据会导致一定的未来函数。

        if isBackTesting:
            conceptsDict = cls.getConceptsFromFile()

        if conceptsDict is None:
            needSaved2File = isBackTesting

            info.print('开始从TuSharePro获取股票所属行业和概念...', DyLogData.ind)

            ts.set_token(DyStockCommon.tuShareProToken)
            pro = ts.pro_api()
            try:
                conceptsDict = cls.__getConceptsFromTuSharePro(pro)
            except Exception as ex:
                info.print('TuSharePro: 获取概念异常: {}'.format(ex), DyLogData.error)
                return None, needSaved2File

            info.print('从TuSharePro获取股票所属行业和概念完成', DyLogData.ind)

        filteredConceptsDict = {}
        for code, name in codeTable.items():
            if code not in conceptsDict:
                info.print('TuSharePro不存在{}({})的所属行业'.format(code, name), DyLogData.warning)
                continue

            filteredConceptsDict[code] = conceptsDict[code]

        return filteredConceptsDict, needSaved2File

    @classmethod
    def _writePreparedData(cls, date, data):
        """
            file name is like 'Program\Strategy\strategyCls.chName\date\preparedData.json'
        """
        path = DyCommon.createPath('Stock/Program/Strategy/{}/{}'.format(cls.chName, date))
        fileName = os.path.join(path, 'preparedData.json')
        with open(fileName, 'w') as f:
            f.write(json.dumps(data, indent=4, cls=DyJsonEncoder))

    @classmethod
    def _prepareConcepts(cls, date, dataEngine, info, codes=None, errorDataEngine=None, strategyParam=None, isBackTesting=False):
        """
            @return: {'newStocks': {code: [所属行业, [概念]]}, 'oldStocks': {code: [所属行业, [概念]]}}
            'newStocks'指那些刚上市一直涨停的股，即@date之前仍然是一字板。只要开过板的股，则归为'oldStocks'
        """
        daysEngine = dataEngine.daysEngine
        errorDaysEngine = errorDataEngine.daysEngine

        if not daysEngine.loadCodeTable(codes=codes):
            return None, False

        # get all codes concepts
        conceptsDict, needSaved2File = cls.__getConcepts(daysEngine.stockCodes, info, isBackTesting)
        if conceptsDict is None:
            return None, False

        # classify stocks
        data = cls.classifyCodes(date, list(daysEngine.stockCodes), info, errorDaysEngine, conceptsDict)

        return data, needSaved2File

    @classmethod
    def _prepareIndicators(cls, date, dataEngine, info, codes=None, errorDataEngine=None, strategyParam=None, isBackTesting=False):
        """
            @return: {'channel': {code: [upper, lower]},
                      'rsi': {code: RSI},
                      'days': {code: [[OHLCV]]},
                      'preClose': {code: preClose},
                      'etfVolatility': {etf300: volatiliy, etf500: volatility}
                     }
        """
        daysEngine = dataEngine.daysEngine
        errorDaysEngine = errorDataEngine.daysEngine

        if not daysEngine.loadCodeTable(codes=codes):
            return None
        codes = daysEngine.stockCodes

        info.print('开始计算{}只股票的指标...'.format(len(codes)), DyLogData.ind)
        progress = DyProgress(info, printConsole=True)
        progress.init(len(codes), 100, 10)

        preparedData = {}

        channelData = {}
        rsiData = {}
        daysData = {}
        preCloseData = {}
        volatilityData = {}
        for code in codes:
            if errorDaysEngine.loadCode(code, [date, -cls.prepareIndicatorFeedNDays], latestAdjFactorInDb=False):
                df = errorDaysEngine.getDataFrame(code)

                highs, lows, closes = df['high'].values, df['low'].values, df['close'].values

                # channel upper and lower
                if df.shape[0] > cls.prepareAtrPeriod:
                    atr = DyTalib.ATR(highs, lows, closes, timeperiod=cls.prepareAtrPeriod)

                    upper = closes[-1] + atr[-1]*2
                    lower = closes[-1] - atr[-1]*2
                    channelData[code] = [upper, lower]

                # RSI
                if df.shape[0] > cls.prepareRsiPeriod:
                    rsi = DyTalib.RSI(closes, timeperiod=cls.prepareRsiPeriod)
                    rsiData[code] = rsi[-1]

                # 日线OHLCV
                if df.shape[0] >= cls.prepareDaysSize:
                    days = df.ix[-cls.prepareDaysSize:, ['open', 'high', 'low', 'close', 'volume']]
                    daysData[code] = days.values.tolist()

                # 波动率
                volatilityMean = DyStockDataUtility.getVolatility(df[-cls.prepareVolatilityNDays-1:]).mean()
                volatilityData[code] = volatilityMean

                # preClose
                preCloseData[code] = closes[-1]

            progress.update()

        preparedData['channel'] = channelData
        preparedData['rsi'] = rsiData
        preparedData['days'] = daysData
        preparedData['preClose'] = preCloseData
        preparedData['volatility'] = volatilityData

        # 计算ETF相关数据
        etfCodes = [DyStockCommon.etf300, DyStockCommon.etf500]

        etfVolatilityData = {}
        if not errorDaysEngine.load([date, -cls.prepareAtrPeriod], latestAdjFactorInDb=False, codes=etfCodes):
            info.print('载入ETF日线数据错误', DyLogData.error)
            return None

        for code in etfCodes:
            df = errorDaysEngine.getDataFrame(code)
            if df.shape[0] <= cls.prepareAtrPeriod:
                info.print('ETF日线数据缺失', DyLogData.error)
                return None

            volatilitySeries = DyStockDataUtility.getVolatility(df)
            etfVolatilityData[code] = volatilitySeries.mean()

        preparedData['etfVolatility'] = etfVolatilityData

        info.print('计算{}只股票的指标完成'.format(len(codes)), DyLogData.ind)

        return preparedData
        
    @classmethod
    def prepare(cls, date, dataEngine, info, codes=None, errorDataEngine=None, strategyParam=None, isBackTesting=False):
        """
            策略开盘前准备数据
        """
        preparedData = {}

        # 股票概念
        data, needSaved2File = cls._prepareConcepts(date, dataEngine, info, codes, errorDataEngine, strategyParam, isBackTesting)
        if data is None:
            return None
        preparedData.update(data)

        # 技术指标
        data = cls._prepareIndicators(date, dataEngine, info, codes, errorDataEngine, strategyParam, isBackTesting)
        if data is None:
            return None
        preparedData.update(data)

        if needSaved2File:
            cls._writePreparedData(date, preparedData)

        return preparedData

    @classmethod
    def preparePos(cls, date, dataEngine, info, posCodes=None, errorDataEngine=None, strategyParam=None, isBackTesting=False):
        """
            策略开盘前持仓准备数据
            @date: 前一交易日
            @return:
        """
        if not posCodes: # no positions
            return {}

        errorDaysEngine = errorDataEngine.daysEngine

        data = {}
        for code in posCodes:
            if not errorDaysEngine.loadCode(code, [date, -cls.prepareIndicatorFeedNDays], latestAdjFactorInDb=False):
                return None

            df = errorDaysEngine.getDataFrame(code)

            highs, lows, closes = df['high'].values, df['low'].values, df['close'].values

            # channel upper and lower
            atr = DyTalib.ATR(highs, lows, closes, timeperiod=cls.prepareAtrPeriod)

            upper = closes[-1] + atr[-1]
            lower = closes[-1] - atr[-1]

            data[code] = {'preClose': closes[-1], # 为了除复权
                          'channel': [upper, lower]
                          }

        return data


    ######################################## For IM ########################################
    ######################################## For IM ########################################
    @classmethod
    def data2Msg(cls, data):
        """
            将推送给UI的data转成消息列表，这样可以推送给QQ或者微信
            只取前每个热点的龙一
            @return: [message]
        """
        qqMsgList = []
        focusList = []
        focusPos = cls.dataHeader.index('热点')
        for row in data:
            if row[focusPos] in focusList:
                continue

            focusList.append(row[focusPos])

            # convert to QQ message
            qqMsg = ''
            for key, value in zip(cls.dataHeader, row):
                if qqMsg:
                    qqMsg += '\n'

                qqMsg += key + ': ' + cls.value2Str(value)
                
            qqMsgList.append(qqMsg)

        return qqMsgList