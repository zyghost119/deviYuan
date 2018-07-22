from time import sleep
import pandas as pd
import tushare as ts
import numpy as np

# copy from tushare
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib3 import urlopen, Request
    pass

from pandas.compat import StringIO
from tushare.stock import cons as ct

from DyCommon.DyCommon import *
from EventEngine.DyEvent import *
from ..DyStockDataCommon import *
from ...Common.DyStockCommon import *
from .DyStockDataTdx import DyStockDataTdx


class DyStockDataTicksGateway(object):
    """
        股票历史分笔数据网络接口
        分笔数据可以从新浪，腾讯，网易，通达信获取
        每个hand一个实例，这样可以防止数据互斥
        通达信的分笔数据没有秒只有分钟，现在的算法是分笔数据随机分布在分钟内
    """
    dataSourceRules = { # startDate - date when data soure have ticks data
        '腾讯': {'startDate': None},
        '新浪': {'startDate': None},
        }

    @classmethod
    def setDataSourceStartDate(cls, dataSoureDetectData):
        for name, date in dataSoureDetectData.items():
            cls.dataSourceRules[name]['startDate'] = date

    class DataSource:
        def __init__(self, name, func):
            self.name = name
            self.func = func
            self.errorCount = 0


    def __init__(self, eventEngine, info, hand):
        self._eventEngine = eventEngine
        self._info = info
        self._hand = hand

        self._tdx = DyStockDataTdx(hand, self._info, emptyTicksRetry=True)

        self._setTicksDataSources(DyStockDataCommon.defaultHistTicksDataSource)

        self._registerEvent()

    def _codeTo163Symbol(code):
        if code[0] in ['5', '6']:
            return '0' + code

        return '1' + code

    def _getTickDataFrom163(code=None, date=None, retry=3, pause=0.001):
        """
            从网易获取分笔数据
            网易的分笔数据只有最近5日的
            接口和返回的DF，保持跟tushare一致
        Parameters
        ------
            code:string
                        股票代码 e.g. 600848
            date:string
                        日期 format：YYYY-MM-DD
            retry : int, 默认 3
                        如遇网络等问题重复执行的次数
            pause : int, 默认 0
                        重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
            return
            -------
            DataFrame 当日所有股票交易数据(DataFrame)
                    属性:成交时间、成交价格、价格变动，成交手、成交金额(元)，买卖类型
        """
        if code is None or len(code)!=9 or date is None:
            return None
        code = code[:-3]
        symbol = DyStockDataTicksGateway._codeTo163Symbol(code)
        yyyy, mm, dd = date.split('-')
        for _ in range(retry):
            sleep(pause)
            try:
                url = 'http://quotes.money.163.com/cjmx/{0}/{1}/{2}.xls'.format(yyyy, yyyy+mm+dd, symbol)
                socket = urlopen(url)
                xd = pd.ExcelFile(socket)
                df = xd.parse(xd.sheet_names[0], names=['time', 'price', 'change', 'volume', 'amount', 'type'])
                df['amount'] = df['amount'].astype('int64') # keep same as tushare
            except Exception as e:
                print(e)
                ex = e
            else:
                return df
        raise ex

    def _codeToTencentSymbol(code):
        if code[0] in ['5', '6']:
            return 'sh' + code

        return 'sz' + code

    def _getTickDataFromTencent(code=None, date=None, retry=3, pause=0.001):
        """
            从腾讯获取分笔数据
            接口和返回的DF，保持跟tushare一致
        Parameters
        ------
            code:string
                        股票代码 e.g. 600848.SH
            date:string
                        日期 format：YYYY-MM-DD
            retry : int, 默认 3
                        如遇网络等问题重复执行的次数
            pause : int, 默认 0
                        重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
            return
            -------
            DataFrame 当日所有股票交易数据(DataFrame)
                    属性:成交时间、成交价格、价格变动，成交手、成交金额(元)，买卖类型
        """
        print('从腾讯获取[{}, {}]的分笔数据...'.format(code, date))

        if code is None or len(code)!=9 or date is None:
            return None
        code = code[:-3]
        symbol = DyStockDataTicksGateway._codeToTencentSymbol(code)
        yyyy, mm, dd = date.split('-')
        for _ in range(retry):
            sleep(pause)
            try:
                re = Request('http://stock.gtimg.cn/data/index.php?appn=detail&action=download&c={0}&d={1}'.format(symbol, yyyy+mm+dd))
                lines = urlopen(re, timeout=10).read()
                lines = lines.decode('GBK') 
                df = pd.read_table(StringIO(lines), names=['time', 'price', 'change', 'volume', 'amount', 'type'],
                                    skiprows=[0])      
            except Exception as e:
                print(e)
                ex = e
            else:
                return df
        raise ex

    def _codeToSinaSymbol(code):
        return DyStockDataTicksGateway._codeToTencentSymbol(code)

    def _getTickDataFromSina(code=None, date=None, retry=3, pause=0.001):
        """
            获取分笔数据
        Parameters
        ------
            code:string
                      股票代码 e.g. 600848
            date:string
                      日期 format：YYYY-MM-DD
            retry : int, 默认 3
                      如遇网络等问题重复执行的次数
            pause : int, 默认 0
                     重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
         return
         -------
            DataFrame 当日所有股票交易数据(DataFrame)
                  属性:成交时间、成交价格、价格变动，成交手、成交金额(元)，买卖类型
        """
        if code is None or len(code)!=9 or date is None:
            return None
        code = code[:-3]
        symbol = DyStockDataTicksGateway._codeToSinaSymbol(code)
        for _ in range(retry):
            sleep(pause)
            try:
                re = Request('http://market.finance.sina.com.cn/downxls.php?date={}&symbol={}'.format(date, symbol))
                lines = urlopen(re, timeout=10).read()
                lines = lines.decode('GBK') 
                df = pd.read_table(StringIO(lines), names=['time', 'price', 'change', 'volume', 'amount', 'type'],
                                   skiprows=[0])      
            except Exception as e:
                print(e)
                ex = e
            else:
                return df
        raise ex

    def _getTicks(self, code, date):
        """
            get history ticks data from network
            @returns: None - error happened, i.e. timer out or errors from server
                             If error happened, ticks engine will retry it.
                      DyStockHistTicksAckData.noData - no data for specified date
                      BSON format data - sucessful situation
        """
        switch = False
        data = None
        for dataSource in self._ticksDataSources:
            # check rule
            rule = self.dataSourceRules.get(dataSource.name)
            if rule is not None:
                startDate = rule['startDate']
                if startDate is not None and (startDate == '' or date < startDate):
                    continue

            # get ticks from data source
            data = self._getTicksByFunc(dataSource.func, code, date)

            # 如果数据源应该有数据却没有数据或者发生错误，则换个数据源获取
            if data == DyStockHistTicksAckData.noData or data is None:
                # fatal error from data source
                if data is None:
                    dataSource.errorCount += 1

                    if dataSource.errorCount >= 3:
                        switch = True
                        dataSource.errorCount = 0

            else: # 超时或者有数据, we don't think timer out as needed to switch data source, which might happen because of network
                dataSource.errorCount = 0 # we only care about consecutive errors
                break

        # Too many errors happend for data source, so we think it as fatal error and then switch data source
        if switch:
            oldTicksDataSourceNames = [s.name for s in self._ticksDataSources]
            self._ticksDataSources = self._ticksDataSources[1:] + self._ticksDataSources[0:1]
            newTicksDataSourceNames = [s.name for s in self._ticksDataSources]

            self._info.print('Hand {}: 历史分笔数据源切换{}->{}'.format(self._hand, oldTicksDataSourceNames, newTicksDataSourceNames), DyLogData.warning)

        # convert return value to retain same interface for ticks engine
        return None if data == 'timer out' else data

    def _getTicksByFunc(self, func, code, date):
        """
            @return: [{indicator: value}], i.e. MongoDB BSON format
                     None - fatal error from server
                     DyStockHistTicksAckData.noData - no data for sepcified date
                     'timer out'
        """
        try:
            df = func(code, date=date)

            if 'change' in df:
                del df['change']

            df = df.dropna() # sometimes Sina will give wrong data that price is NaN
            df = df[df['volume'] > 0] # !!!drop 0 volume, added 2017/05/30, sometimes Sina contains tick with 0 volume.
            df = df.drop_duplicates(['time']) # drop duplicates

            # sometimes Sina will give wrong time format like some time for 002324.SZ at 2013-03-18 is '14.06'
            while True:
                try:
                    df['time']  =  pd.to_datetime(date + ' ' + df['time'], format='%Y-%m-%d %H:%M:%S')
                except ValueError as ex:
                    strEx = str(ex)
                    errorTime = strEx[strEx.find(date) + len(date) + 1:strEx.rfind("'")]
                    df = df[~(df['time'] == errorTime)]
                    continue
                break

            df.rename(columns={'time': 'datetime'}, inplace=True)

            df = df.T
            data = [] if df.empty else list(df.to_dict().values())

        except Exception as ex:
            if '当天没有数据' in str(ex):
                return DyStockHistTicksAckData.noData
            else:
                self._info.print("Hand {}: {}获取[{}, {}]Tick数据异常: {}".format(self._hand, func.__name__, code, date, str(ex)), DyLogData.warning)
                if 'timed out' in str(ex):
                    return 'timer out'
                else:
                    return None

        return data if data else DyStockHistTicksAckData.noData

    def _stockHistTicksReqHandler(self, event):
        code = event.data.code
        date = event.data.date

        data = self._getTicks(code, date)

        # put ack event
        event = DyEvent(DyEventType.stockHistTicksAck)
        event.data = DyStockHistTicksAckData(code, date, data)

        self._eventEngine.put(event)

    def _registerEvent(self):
        self._eventEngine.register(DyEventType.stockHistTicksReq + str(self._hand), self._stockHistTicksReqHandler, self._hand)
        self._eventEngine.register(DyEventType.updateHistTicksDataSource, self._updateHistTicksDataSourceHandler, self._hand)

    def _updateHistTicksDataSourceHandler(self, event):
        self._setTicksDataSources(event.data)

    def _setTicksDataSources(self, dataSource):
        if dataSource == '新浪':
            self._ticksDataSources = [self.DataSource('新浪', self.__class__._getTickDataFromSina)]
        elif dataSource == '腾讯':
            self._ticksDataSources = [self.DataSource('腾讯', self.__class__._getTickDataFromTencent)]
        elif dataSource == '通达信':
            self._ticksDataSources = [self.DataSource('通达信', self._tdx.getTicks)]
        else: # '智能'
            self._ticksDataSources = [self.DataSource('腾讯', self.__class__._getTickDataFromTencent),
                                      self.DataSource('通达信', self._tdx.getTicks),
                                      ]
