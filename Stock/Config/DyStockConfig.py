import os
import json
import copy

from DyCommon.DyCommon import DyCommon
from Stock.Common.DyStockCommon import DyStockCommon
from ..Data.Engine.DyStockMongoDbEngine import DyStockMongoDbEngine
from ..Trade.WeChat.DyStockTradeWxEngine import DyStockTradeWxEngine
from ..Trade.Broker.YhNew.YhTrader import YhTrader
from ..Trade.Broker.Ths.ThsTrader import ThsTrader
from ..Data.Gateway.DyStockDataGateway import DyStockDataGateway


class DyStockConfig(object):
    """
        Read configs from files and then set to corresponding variables
    """

    defaultMongoDb = {"Connection": {"Host": "localhost", "Port": 27017},
                      "CommonDays": {
                          "Wind": {
                              "stockCommonDb": 'stockCommonDb',
                              'tradeDayTableName': "tradeDayTable",
                              'codeTableName': "codeTable",
                              'stockDaysDb': 'stockDaysDb'
                              },
                          "TuShare": {
                              "stockCommonDb": 'stockCommonDbTuShare',
                              'tradeDayTableName': "tradeDayTableTuShare",
                              'codeTableName': "codeTableTuShare",
                              'stockDaysDb': 'stockDaysDbTuShare'
                              }
                          },
                      "Ticks": {"db": 'stockTicksDb'}
                      }

    defaultWxScKey = {"WxScKey": ""}

    defaultAccount = {"Ths": {"Account": "", "Password": "", "Exe": r"C:\Program Files\同花顺\xiadan.exe"},
                      "Yh": {"Account": "", "Password": "", "Exe": r"C:\Program Files\中国银河证券双子星3.2\Binarystar.exe"},
                      }

    defaultTradeDaysMode = {"tradeDaysMode": "Verify"}

    defaultTuShareDaysInterval = {"interval": 0}
    defaultTuShareProDaysInterval = {"interval": 0}


    def getDefaultHistDaysDataSource():
        if DyStockCommon.WindPyInstalled:
            return {"Wind": True, "TuShare": False}

        return {"Wind": False, "TuShare": True}

    def _configStockHistDaysDataSource():
        file = DyStockConfig.getStockHistDaysDataSourceFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.getDefaultHistDaysDataSource()

        DyStockConfig.configStockHistDaysDataSource(data)

    def configStockHistDaysDataSource(data):
        DyStockCommon.defaultHistDaysDataSource = []
        if data.get('Wind'):
            DyStockCommon.defaultHistDaysDataSource.append('Wind')

        if data.get('TuShare'):
            DyStockCommon.defaultHistDaysDataSource.append('TuShare')

    def getStockHistDaysDataSourceFileName():
        path = DyCommon.createPath('Stock/User/Config/Common')
        file = os.path.join(path, 'DyStockHistDaysDataSource.json')

        return file

    def _configStockHistDaysTuSharePro():
        file = DyStockConfig.getStockHistDaysTuShareProFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.getDefaultHistDaysTuSharePro()

        DyStockConfig.configStockHistDaysTuSharePro(data)

    def configStockHistDaysTuSharePro(data):
        DyStockCommon.useTuSharePro = False
        DyStockCommon.tuShareProToken = None

        if data.get('TuSharePro'):
            DyStockCommon.useTuSharePro = True

        DyStockCommon.tuShareProToken = data.get('Token')

    def getStockHistDaysTuShareProFileName():
        path = DyCommon.createPath('Stock/User/Config/Common')
        file = os.path.join(path, 'DyStockHistDaysTuSharePro.json')

        return file

    def getDefaultHistDaysTuSharePro():
        return {'TuSharePro': False, 'ShowToken': False}

    def _configStockMongoDb():
        file = DyStockConfig.getStockMongoDbFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.defaultMongoDb

        DyStockConfig.configStockMongoDb(data)

    def configStockMongoDb(data):
        DyStockMongoDbEngine.host = data['Connection']['Host']
        DyStockMongoDbEngine.port = data['Connection']['Port']

        # Wind
        DyStockMongoDbEngine.stockCommonDb = data["CommonDays"]["Wind"]['stockCommonDb']
        DyStockMongoDbEngine.tradeDayTableName = data["CommonDays"]["Wind"]['tradeDayTableName']
        DyStockMongoDbEngine.codeTableName = data["CommonDays"]["Wind"]['codeTableName']

        DyStockMongoDbEngine.stockDaysDb = data["CommonDays"]["Wind"]['stockDaysDb']

        # TuShare
        DyStockMongoDbEngine.stockCommonDbTuShare = data["CommonDays"]["TuShare"]['stockCommonDb']
        DyStockMongoDbEngine.tradeDayTableNameTuShare = data["CommonDays"]["TuShare"]['tradeDayTableName']
        DyStockMongoDbEngine.codeTableNameTuShare = data["CommonDays"]["TuShare"]['codeTableName']

        DyStockMongoDbEngine.stockDaysDbTuShare = data["CommonDays"]["TuShare"]['stockDaysDb']

        # ticks
        DyStockMongoDbEngine.stockTicksDb = data["Ticks"]["db"]

    def getStockMongoDbFileName():
        path = DyCommon.createPath('Stock/User/Config/Common')
        file = os.path.join(path, 'DyStockMongoDb.json')

        return file

    def _configStockWxScKey():
        file = DyStockConfig.getStockWxScKeyFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.defaultWxScKey

        DyStockConfig.configStockWxScKey(data)

    def configStockWxScKey(data):
        DyStockTradeWxEngine.scKey = data["WxScKey"]

    def getStockWxScKeyFileName():
        path = DyCommon.createPath('Stock/User/Config/Trade')
        file = os.path.join(path, 'DyStockWxScKey.json')

        return file

    def _configStockAccount():
        file = DyStockConfig.getStockAccountFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.defaultAccount

        DyStockConfig.configStockAccount(data)

    def configStockAccount(data):
        YhTrader.account = data["Yh"]["Account"]
        YhTrader.password = data["Yh"]["Password"]
        YhTrader.exePath = data["Yh"]["Exe"]

        ThsTrader.exePath = data["Ths"]["Exe"]

    def getStockAccountFileName():
        path = DyCommon.createPath('Stock/User/Config/Trade')
        file = os.path.join(path, 'DyStockAccount.json')

        return file

    def _configStockTradeDaysMode():
        file = DyStockConfig.getStockTradeDaysModeFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.defaultTradeDaysMode

        DyStockConfig.configStockTradeDaysMode(data)

    def configStockTradeDaysMode(data):
        DyStockDataGateway.tradeDaysMode = data["tradeDaysMode"]

    def getStockTradeDaysModeFileName():
        path = DyCommon.createPath('Stock/User/Config/Common')
        file = os.path.join(path, 'DyStockTradeDaysMode.json')

        return file

    def _configStockTuShareDaysInterval():
        file = DyStockConfig.getStockTuShareDaysIntervalFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.defaultTuShareDaysInterval

        DyStockConfig.configStockTuShareDaysInterval(data)

    def configStockTuShareDaysInterval(data):
        DyStockDataGateway.tuShareDaysSleepTimeConst = data["interval"]

    def getStockTuShareDaysIntervalFileName():
        path = DyCommon.createPath('Stock/User/Config/Common')
        file = os.path.join(path, 'DyStockTuShareDaysInterval.json')

        return file

    def _configStockTuShareProDaysInterval():
        file = DyStockConfig.getStockTuShareProDaysIntervalFileName()

        # open
        try:
            with open(file) as f:
                data = json.load(f)
        except:
            data = DyStockConfig.defaultTuShareProDaysInterval

        DyStockConfig.configStockTuShareProDaysInterval(data)

    def configStockTuShareProDaysInterval(data):
        DyStockDataGateway.tuShareProDaysSleepTimeConst = data["interval"]

    def getStockTuShareProDaysIntervalFileName():
        path = DyCommon.createPath('Stock/User/Config/Common')
        file = os.path.join(path, 'DyStockTuShareProDaysInterval.json')

        return file

    def config():
        DyStockConfig._configStockHistDaysDataSource() # first
        DyStockConfig._configStockHistDaysTuSharePro()
        DyStockConfig._configStockTradeDaysMode()
        DyStockConfig._configStockTuShareDaysInterval()
        DyStockConfig._configStockTuShareProDaysInterval()
        DyStockConfig._configStockMongoDb()
        DyStockConfig._configStockWxScKey()
        DyStockConfig._configStockAccount()

    def _getStockMongoDbForBackTesting():
        data = copy.deepcopy(DyStockConfig.defaultMongoDb)

        # connection
        data['Connection']['Host'] = DyStockMongoDbEngine.host
        data['Connection']['Port'] = DyStockMongoDbEngine.port

        # Wind
        data["CommonDays"]["Wind"]['stockCommonDb'] = DyStockMongoDbEngine.stockCommonDb
        data["CommonDays"]["Wind"]['tradeDayTableName'] = DyStockMongoDbEngine.tradeDayTableName
        data["CommonDays"]["Wind"]['codeTableName'] = DyStockMongoDbEngine.codeTableName

        data["CommonDays"]["Wind"]['stockDaysDb'] = DyStockMongoDbEngine.stockDaysDb

        # TuShare
        data["CommonDays"]["TuShare"]['stockCommonDb'] = DyStockMongoDbEngine.stockCommonDbTuShare
        data["CommonDays"]["TuShare"]['tradeDayTableName'] = DyStockMongoDbEngine.tradeDayTableNameTuShare
        data["CommonDays"]["TuShare"]['codeTableName'] = DyStockMongoDbEngine.codeTableNameTuShare

        data["CommonDays"]["TuShare"]['stockDaysDb'] = DyStockMongoDbEngine.stockDaysDbTuShare

        # ticks
        data["Ticks"]["db"] = DyStockMongoDbEngine.stockTicksDb

        return data

    def getConfigForBackTesting():
        """
            多进程回测需要当前进程的配置参数
        """
        data = {}
        data['exePath'] = DyCommon.exePath
        data['defaultHistDaysDataSource'] = DyStockCommon.defaultHistDaysDataSource
        data['tuSharePro'] = {'useTuSharePro': DyStockCommon.useTuSharePro,
                            'tuShareProToken': DyStockCommon.tuShareProToken,
                            }
        data['mongoDb'] = DyStockConfig._getStockMongoDbForBackTesting()

        return data

    def setConfigForBackTesting(data):
        DyCommon.exePath = data['exePath']
        DyStockCommon.defaultHistDaysDataSource = data['defaultHistDaysDataSource']
        DyStockCommon.useTuSharePro = data['tuSharePro']['useTuSharePro']
        DyStockCommon.tuShareProToken = data['tuSharePro']['tuShareProToken']

        DyStockConfig.configStockMongoDb(data['mongoDb'])
        