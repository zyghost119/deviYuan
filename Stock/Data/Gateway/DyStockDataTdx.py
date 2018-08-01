import datetime
import random
import time
from collections import OrderedDict

import pandas as pd
from pytdx.hq import TdxHq_API

from DyCommon.DyCommon import DyLogData


class DyStockDataTdx:
    """
        We keep connection open to get high performance until consecutive errors happened.
    """
    class Api:
        def __init__(self, tdxApi, ip, port, latency, errorCount=0):
            self.tdxApi = tdxApi

            # save ip:port to loose couple with pytdx
            self.ip = ip
            self.port = port

            self.latency = latency
            self.errorCount = errorCount


    ticksBatchSize = 2000  # 800 or 2000 ? 2000 maybe also work
    maxApiErrorCount = 3

    ipListTest = [
        {'ip': '101.227.73.20', 'port': 7709}, # default
        ]

    ipList = [
        {'ip': '101.227.73.20', 'port': 7709}, # default

        # from QUANTAXIS
        {'ip': '114.80.80.222', 'port': 7709},
        {'ip': '123.125.108.24', 'port': 7709},
        {'ip': '123.125.108.23', 'port': 7709},
        {'ip': '218.75.126.9', 'port': 7709}, {
            'ip': '115.238.90.165', 'port': 7709},
        {'ip': '124.160.88.183', 'port': 7709}, {
            'ip': '60.12.136.250', 'port': 7709},
        {'ip': '218.108.98.244', 'port': 7709}, {
            'ip': '218.108.47.69', 'port': 7709},
        {'ip': '180.153.39.51', 'port': 7709}, {
            'ip': '121.14.2.7', 'port': 7709},
        {'ip': '60.28.29.69', 'port': 7709}, {
            'ip': '180.153.18.170', 'port': 7709},
        {'ip': '180.153.18.171', 'port': 7709}, {
            'ip': '180.153.18.17', 'port': 7709},
        {'ip': '61.135.142.73', 'port': 7709}, {
            'ip': '115.238.56.198', 'port': 7709},
        {'ip': '60.191.117.167', 'port': 7709}, {
            'ip': 'hq.cjis.cn', 'port': 7709},
        {'ip': '59.173.18.69', 'port': 7709}, {
            'ip': 'sztdx.gtjas.com', 'port': 7709},
        {'ip': 'jstdx.gtjas.com', 'port': 7709}, {
            'ip': 'shtdx.gtjas.com', 'port': 7709},
        {'ip': '218.9.148.108', 'port': 7709}, {
            'ip': '61.153.144.179', 'port': 7709},
        {'ip': '61.153.209.138', 'port': 7709}, {
            'ip': '61.153.209.139', 'port': 7709},
        {'ip': 'hq1.daton.com.cn', 'port': 7709}, {
            'ip': '119.29.51.30', 'port': 7709},
        {'ip': '114.67.61.70', 'port': 7709}, {'ip': '14.17.75.11', 'port': 7709},
        {'ip': '121.14.104.70', 'port': 7709}, {
            'ip': '121.14.104.72', 'port': 7709},
        {'ip': '112.95.140.74', 'port': 7709}, {
            'ip': '112.95.140.92', 'port': 7709},
        {'ip': '112.95.140.93', 'port': 7709}, {
            'ip': '114.80.149.19', 'port': 7709},
        {'ip': '114.80.149.22', 'port': 7709}, {
            'ip': '114.80.149.84', 'port': 7709}
    ]


    def __init__(self, tdxNo, info, emptyTicksRetry=True):
        self._tdxNo = tdxNo
        self._info = info
        self._emptyTicksRetry = emptyTicksRetry

        self._apis = None

    def _getMarketCode(self, code):
        """
            1- sh
            0 -sz
        """
        return 1 if code[-2:] == 'SH' else 0

    def _adjustApis(self, api):
        """
            @return: True - New tdx api established successfully
        """
        if not self._apis:
            return False

        if api not in self._apis:
            return False

        if api.errorCount < self.maxApiErrorCount:
            return False

        # We think this API isn't stable or connection is closed.
        # So we try ping it again.
        self._closeTdxApi(api.tdxApi) # close firstly
        time.sleep(1)
        tdxApi, latency = self._ping(api.ip, api.port)
        if tdxApi is None:
            self._apis.remove(api)
            return False

        # new one
        api.tdxApi = tdxApi
        api.latency = latency # just reassign, actually no use
        api.errorCount = 0

        # We don't sort. 
        # Assume one condition that all connections are closed by server and this latency is big,
        # so this API will be moved close to end.
        # Because there're about 30+ APIs, after these APIs tried failed, time elapsed too much,
        # which brings this API closed by server again.
        # So there might be continuous loop in connection again.
        #self._apis.sort(key=lambda e: e.latency)

        return True

    def _ping(self, ip, port=7709):
        print('TDX{}: Start connecting to {}:{}...'.format(self._tdxNo, ip, port))

        tdxApi = TdxHq_API(raise_exception=True)
        startTime = datetime.datetime.now()
        try:
            tdxApi.connect(ip, port, time_out=1)
            if len(tdxApi.get_security_list(0, 1)) > 800:
                print('TDX{}: Connect to {}:{} successfully'.format(self._tdxNo, ip, port))
                return tdxApi, datetime.datetime.now() - startTime

            print('TDX{}: Bad response from {}:{}'.format(self._tdxNo, ip, port))
        except Exception as ex:
            print('TDX{}: Exception happened when ping {}:{}, {}'.format(self._tdxNo, ip, port, ex))
 
        return None, None

    def _startApis(self):
        self._info.print('TDX{}: Start APIs...'.format(self._tdxNo))

        # ping all ips
        self._apis = []
        for ipDict in self.ipList:
            ip, port = ipDict['ip'], ipDict['port']
            tdxApi, latency = self._ping(ip, port)
            if tdxApi is None:
                continue
            
            self._apis.append(self.Api(tdxApi, ip, port, latency))

        if not self._apis:
            self._info.print('TDX{}: Start APIs failed'.format(self._tdxNo), DyLogData.error)
            return False

        # sort by latency
        self._apis.sort(key=lambda e: e.latency)

        self._info.print('TDX{}: Start {} APIs successfully'.format(self._tdxNo, len(self._apis)))
        return True

    def _getApis(self):
        if not self._apis:
            if not self._startApis():
                return None

        return [x for x in self._apis] # copy it because @self._apis might be changed in function

    def _transform1Min(self, chunk):
        """
            randomly scatter ticks in one minute
        """
        if not chunk:
            return []

        if len(chunk) > 60:
            return None

        random.seed()
        newChunk = []
        curSec = -1
        for i, tick in enumerate(chunk):
            leftNbr = len(chunk) - i
            randSize = ((59 - curSec) - leftNbr) + 1
            assert 60 >= randSize >= 1 and 59 >= curSec + randSize >= 0

            curSec += random.randint(1, randSize)

            # new tick
            newTick = OrderedDict()
            newTick['time'] = '{}:{}'.format(tick['time'], curSec if curSec >= 10 else ('0' + str(curSec)))
            newTick['price'] = tick['price']
            newTick['volume'] = tick['vol']
            newTick['amount'] = tick['vol']*100*tick['price']
            
            # 1--sell 0--buy 2--盘前
            if tick['buyorsell'] == 0:
                newTick['type'] = '买盘'
            elif tick['buyorsell'] == 1:
                newTick['type'] = '卖盘'
            else:
                newTick['type'] = '中性盘'

            newChunk.append(newTick)

        return newChunk

    def _transform(self, chunks, api, code, date):
        time = None
        chunk = None
        newChunks = []
        for tick in chunks:
            if time != tick['time']:
                # transform this chunk
                newChunk = self._transform1Min(chunk)
                if newChunk is None:
                    self._info.print('TDX{}: [{}, {}] minute ticks[{}] from {}:{}: length > 60'.format(self._tdxNo, code, date, chunk[0]['time'], api.ip, api.port), DyLogData.warning)
                    return None

                newChunks += newChunk

                # new min start
                time = tick['time']
                chunk = [tick]
            else:
                chunk.append(tick)

        return newChunks

    def _getTicksOneChunkByApi(self, api, code, date, offset, retry=3):
        loop = True
        while loop:
            for _ in range(retry):
                try:
                    chunk = api.tdxApi.get_history_transaction_data(
                                                            self._getMarketCode(code),
                                                            code[:-3],
                                                            offset*self.ticksBatchSize,
                                                            self.ticksBatchSize,
                                                            int(date.replace('-', ''))
                                                            )
                except Exception as ex:
                    api.errorCount += 1
                    print('TDX{}: Exception happended when getting ticks[{}, {}] from {}:{}, {}'.format(self._tdxNo, code, date, api.ip, api.port, ex))

                else: # successful
                    api.errorCount = 0 # reset
                    return chunk
            else: # error
                print('TDX{}: {} retries of getting ticks[{}, {}] failed from {}:{}'.format(self._tdxNo, retry, code, date, api.ip, api.port))
                loop = self._adjustApis(api)

        return None

    def _closeTdxApi(self, tdxApi):
        try:
            tdxApi.disconnect()
        except:
            pass

    def close(self):
        if not self._apis:
            return

        for api in self._apis:
            self._closeTdxApi(api.tdxApi)

    def _getTicksByApi(self, api, code, date, retry=3):
        print('TDX{}: Get ticks[{}, {}] from {}:{}'.format(self._tdxNo, code, date, api.ip, api.port))

        # get ticks
        chunks = []
        offset = 0
        while True:
            chunk = self._getTicksOneChunkByApi(api, code, date, offset, retry)
            if chunk is None:
                return None
            
            chunks = chunk + chunks # insert in front
            offset += 1

            # we think it's finished
            if len(chunk) < self.ticksBatchSize:
                break

        # to DF
        newChunks = self._transform(chunks, api, code, date)
        if newChunks:
            df = api.tdxApi.to_df(newChunks)
        else: # None or empty
            # For None, we just think TDX API can give us the data, but the data is wrong.
            # So we take it as empty DF, so that ticks Engine will not try it again and again.
            # It's not good return value, but keep DevilYuan system not in stuck.
            df = pd.DataFrame(columns=['time', 'price', 'volume', 'amount', 'type'])

        return df
    
    def getTicks(self, code, date, retry=3, pause=0):
        apis = self._getApis()
        if apis is None:
            return None

        df = None
        for api in apis:
            df = self._getTicksByApi(api, code, date, retry=retry)
            if df is None:
                continue

            if df.empty and self._emptyTicksRetry:
                continue

            return df

        return df

    def _getStockCodesByApi(self, api):
        """
            0 - 深圳， 1 - 上海
        """
        def sz(code):
            if code[:2] in ['00', '30', '02']:
                return True

            return False

        def sh(code):
            if code[0] == '6':
                return True

            return False

        def get(market, rule, ):
            count = api.tdxApi.get_security_count(market)

            chunks = []
            step = 1000 # each request to tdx will return <=1000 codes
            for i in range(0, count, step):
                chunk = api.tdxApi.get_security_list(market, i)
                chunks += chunk

            df = api.tdxApi.to_df(chunks)
            return df[df.code.apply(rule)] # filter SZ&SH codes


        dfs = []
        for market, rule in ((0, sz), (1, sh)):
            try:
                dfs.append(get(market, rule))
            except:
                self._info.print('TDX{}: Get stock code table failed from {}:{}'.format(self._tdxNo, api.ip, api.port), DyLogData.error)
                return None

        return pd.concat(dfs)

    def getStockCodes(self):
        """
            get stock code table
            @return: DF(['code', 'name'])
        """
        apis = self._getApis()
        if apis is None:
            return None

        for api in apis:
            df = self._getStockCodesByApi(api)
            if df is not None and not df.empty:
                return df

        return None