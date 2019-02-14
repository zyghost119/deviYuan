# -*- coding: utf-8 -*-
import abc
import functools
import os
import sys
import time
import io
import pandas as pd

from . import pop_dialog_handler

from DyCommon.DyCommon import DyLogData


if not sys.platform.startswith("darwin"):
    try:
        import pywinauto
        import pywinauto.clipboard
    except:
        if sys.platform == 'win32':
            raise


class IGridStrategy(abc.ABC):
    @abc.abstractmethod
    def get(self, control_id):
        """
        获取 gird 数据并格式化返回
        :param control_id: grid 的 control id
        :return: grid 数据
        """
        pass


class BaseStrategy(IGridStrategy):
    def __init__(self, trader):
        self._trader = trader

    @abc.abstractmethod
    def get(self, control_id):
        """
        :param control_id: grid 的 control id
        :return: grid 数据
        """
        pass

    def _get_grid(self, control_id):
        grid = self._trader.main.window(
            control_id=control_id, class_name="CVirtualGridCtrl"
        )
        return grid


class Copy(BaseStrategy):
    """
    通过复制 grid 内容到剪切板z再读取来获取 grid 内容
    """

    def get(self, control_id, dataType):
        print('同花顺客户端: 获取[{}]数据...'.format(dataType))

        i = 0.5
        while True:
            pywinauto.clipboard.EmptyClipboard() # clear
            time.sleep(0.1)

            try:
                df = self._get(control_id)
            except Exception as ex:
                print('同花顺客户端: 获取[{}]数据失败: {}, 重试...'.format(dataType, ex))
            else:
                break

            i += 0.1
            time.sleep(i)

        return df

    def _get(self, control_id):
        grid = self._get_grid(control_id)
        grid.type_keys("^A^C")
        content = self._get_clipboard_data()
        return self._format_grid_data(content)

    def _format_grid_data(self, data):
        df = pd.read_csv(
            io.StringIO(data),
            delimiter="\t",
            dtype=self._trader.config.GRID_DTYPE,
            na_filter=False,
        )
        return df[df.columns[:-1]] # drop the last unnamed column

    def _get_clipboard_data(self):
        return pywinauto.clipboard.GetData()



class CommonConfig:
    DEFAULT_EXE_PATH = ""
    TITLE = "网上股票交易系统5.0"

    TRADE_SECURITY_CONTROL_ID = 1032
    TRADE_PRICE_CONTROL_ID = 1033
    TRADE_AMOUNT_CONTROL_ID = 1034

    TRADE_SUBMIT_CONTROL_ID = 1006

    TRADE_MARKET_TYPE_CONTROL_ID = 1541

    COMMON_GRID_CONTROL_ID = 1047

    COMMON_GRID_LEFT_MARGIN = 10
    COMMON_GRID_FIRST_ROW_HEIGHT = 30
    COMMON_GRID_ROW_HEIGHT = 16

    BALANCE_MENU_PATH = ["查询[F4]", "资金股票"]
    POSITION_MENU_PATH = ["查询[F4]", "资金股票"]
    TODAY_ENTRUSTS_MENU_PATH = ["查询[F4]", "当日委托"]
    TODAY_TRADES_MENU_PATH = ["查询[F4]", "当日成交"]

    BALANCE_CONTROL_ID_GROUP = {
        "资金余额": 1012,
        "可用金额": 1016,
        "可取金额": 1017,
        "股票市值": 1014,
        "总资产": 1015,
    }

    POP_DIALOD_TITLE_CONTROL_ID = 1365

    GRID_DTYPE = {
        "操作日期": str,
        "委托编号": str,
        "申请编号": str,
        "合同编号": str,
        "证券代码": str,
        "股东代码": str,
        "资金帐号": str,
        "资金帐户": str,
        "发生日期": str,
    }

    CANCEL_ENTRUST_ENTRUST_FIELD = "合同编号"
    CANCEL_ENTRUST_GRID_LEFT_MARGIN = 50
    CANCEL_ENTRUST_GRID_FIRST_ROW_HEIGHT = 30
    CANCEL_ENTRUST_GRID_ROW_HEIGHT = 16

    AUTO_IPO_SELECT_ALL_BUTTON_CONTROL_ID = 1098
    AUTO_IPO_BUTTON_CONTROL_ID = 1006
    AUTO_IPO_MENU_PATH = ["新股申购", "批量新股申购"]


class IClientTrader(abc.ABC):
    @property
    @abc.abstractmethod
    def app(self):
        """Return current app instance"""
        pass

    @property
    @abc.abstractmethod
    def main(self):
        """Return current main window instance"""
        pass

    @property
    @abc.abstractmethod
    def config(self):
        """Return current config instance"""
        pass

    @abc.abstractmethod
    def wait(self, seconds: float):
        """Wait for operation return"""
        pass

    @abc.abstractmethod
    def refresh(self):
        """Refresh data"""
        pass


class ThsClientTrader(IClientTrader):
    # The strategy to use for getting grid data
    grid_strategy = Copy

    def __init__(self, info):
        self._info = info # DY info

        self._config = CommonConfig
        self._app = None
        self._main = None

    @property
    def app(self):
        return self._app

    @property
    def main(self):
        return self._main

    @property
    def config(self):
        return self._config

    def login(self, user, password, exe_path, comm_password=None, **kwargs):
        """
            其他券商通用同花顺客户端(需要手动登陆)
        """
        self.connect(exe_path, **kwargs)

    def connect(self, exe_path=None, **kwargs):
        """
        直接连接登陆后的客户端
        :param exe_path: 客户端路径类似 r'C:\\htzqzyb2\\xiadan.exe', 默认 r'C:\\htzqzyb2\\xiadan.exe'
        :return:
        """
        connect_path = exe_path
        if connect_path is None:
            raise ValueError(
                "参数 exe_path 未设置，请设置客户端对应的 exe 地址,类似 C:\\客户端安装目录\\xiadan.exe"
            )

        self._app = pywinauto.Application().connect(
            path=connect_path, timeout=10
        )
        self._close_prompt_windows()
        self._main = self._app.top_window()

    @property
    def broker_type(self):
        return "ths"

    @property
    def balance(self):
        self._switch_left_menus(["查询[F4]", "资金股票"])

        return self._get_balance_from_statics()

    def _get_balance_from_statics(self):
        result = {}
        for key, control_id in self._config.BALANCE_CONTROL_ID_GROUP.items():
            result[key] = float(
                self._main.window(
                    control_id=control_id, class_name="Static"
                ).window_text()
            )
        return pd.DataFrame(result, index=range(len(result)))

    @property
    def position(self):
        self._switch_left_menus(["查询[F4]", "资金股票"])

        return self._get_grid_data(self._config.COMMON_GRID_CONTROL_ID, "持仓")

    @property
    def today_entrusts(self):
        self._switch_left_menus(["查询[F4]", "当日委托"])

        return self._get_grid_data(self._config.COMMON_GRID_CONTROL_ID, "当日委托")

    @property
    def today_trades(self):
        self._switch_left_menus(["查询[F4]", "当日成交"])

        return self._get_grid_data(self._config.COMMON_GRID_CONTROL_ID, "当日成交")

    @property
    def cancel_entrusts(self):
        self.refresh()
        self._switch_left_menus(["撤单[F3]"])

        return self._get_grid_data(self._config.COMMON_GRID_CONTROL_ID, "撤单")

    def cancel_entrust(self, entrust_no):
        self.refresh()
        for i, entrust in enumerate(self.cancel_entrusts):
            if (
                entrust[self._config.CANCEL_ENTRUST_ENTRUST_FIELD]
                == entrust_no
            ):
                self._cancel_entrust_by_double_click(i)
                return self._handle_pop_dialogs()
        return {"message": "委托单状态错误不能撤单, 该委托单可能已经成交或者已撤"}

    def buy(self, security, price, amount, **kwargs):
        self._switch_left_menus(["买入[F1]"])

        return self.trade(security, price, amount)

    def sell(self, security, price, amount, **kwargs):
        self._switch_left_menus(["卖出[F2]"])

        return self.trade(security, price, amount)

    def market_buy(self, security, amount, ttype=None, **kwargs):
        """
        市价买入
        :param security: 六位证券代码
        :param amount: 交易数量
        :param ttype: 市价委托类型，默认客户端默认选择，
                     深市可选 ['对手方最优价格', '本方最优价格', '即时成交剩余撤销', '最优五档即时成交剩余 '全额成交或撤销']
                     沪市可选 ['最优五档成交剩余撤销', '最优五档成交剩余转限价']
        :return: {'entrust_no': '委托单号'}
        """
        self._switch_left_menus(["市价委托", "买入"])

        return self.market_trade(security, amount, ttype)

    def market_sell(self, security, amount, ttype=None, **kwargs):
        """
        市价卖出
        :param security: 六位证券代码
        :param amount: 交易数量
        :param ttype: 市价委托类型，默认客户端默认选择，
                     深市可选 ['对手方最优价格', '本方最优价格', '即时成交剩余撤销', '最优五档即时成交剩余 '全额成交或撤销']
                     沪市可选 ['最优五档成交剩余撤销', '最优五档成交剩余转限价']
        :return: {'entrust_no': '委托单号'}
        """
        self._switch_left_menus(["市价委托", "卖出"])

        return self.market_trade(security, amount, ttype)

    def market_trade(self, security, amount, ttype=None, **kwargs):
        """
        市价交易
        :param security: 六位证券代码
        :param amount: 交易数量
        :param ttype: 市价委托类型，默认客户端默认选择，
                     深市可选 ['对手方最优价格', '本方最优价格', '即时成交剩余撤销', '最优五档即时成交剩余 '全额成交或撤销']
                     沪市可选 ['最优五档成交剩余撤销', '最优五档成交剩余转限价']
        :return: {'entrust_no': '委托单号'}
        """
        self._set_market_trade_params(security, amount)
        if ttype is not None:
            self._set_market_trade_type(ttype)
        self._submit_trade()

        return self._handle_pop_dialogs(
            handler_class=pop_dialog_handler.TradePopDialogHandler
        )

    def _set_market_trade_type(self, ttype):
        """根据选择的市价交易类型选择对应的下拉选项"""
        selects = self._main.window(
            control_id=self._config.TRADE_MARKET_TYPE_CONTROL_ID,
            class_name="ComboBox",
        )
        for i, text in selects.texts():
            # skip 0 index, because 0 index is current select index
            if i == 0:
                continue
            if ttype in text:
                selects.select(i - 1)
                break
        else:
            raise TypeError("不支持对应的市价类型: {}".format(ttype))

    def auto_ipo(self):
        self._switch_left_menus(self._config.AUTO_IPO_MENU_PATH)

        stock_list = self._get_grid_data(self._config.COMMON_GRID_CONTROL_ID, "新股申购")

        if len(stock_list) == 0:
            return {"message": "今日无新股"}
        invalid_list_idx = [
            i for i, v in enumerate(stock_list) if v["申购数量"] <= 0
        ]

        if len(stock_list) == len(invalid_list_idx):
            return {"message": "没有发现可以申购的新股"}

        self._click(self._config.AUTO_IPO_SELECT_ALL_BUTTON_CONTROL_ID)
        self.wait(0.1)

        for row in invalid_list_idx:
            self._click_grid_by_row(row)
        self.wait(0.1)

        self._click(self._config.AUTO_IPO_BUTTON_CONTROL_ID)
        self.wait(0.1)

        return self._handle_pop_dialogs()

    def _click_grid_by_row(self, row):
        x = self._config.COMMON_GRID_LEFT_MARGIN
        y = (
            self._config.COMMON_GRID_FIRST_ROW_HEIGHT
            + self._config.COMMON_GRID_ROW_HEIGHT * row
        )
        self._app.top_window().window(
            control_id=self._config.COMMON_GRID_CONTROL_ID,
            class_name="CVirtualGridCtrl",
        ).click(coords=(x, y))

    def _is_exist_pop_dialog(self):
        self.wait(0.2)  # wait dialog display
        return (
            self._main.wrapper_object()
            != self._app.top_window().wrapper_object()
        )

    def _run_exe_path(self, exe_path):
        return os.path.join(os.path.dirname(exe_path), "xiadan.exe")

    def wait(self, seconds):
        time.sleep(seconds)

    def exit(self):
        self._app.kill()

    def _close_prompt_windows(self):
        self.wait(1)
        for window in self._app.windows(class_name="#32770"):
            if window.window_text() != self._config.TITLE:
                window.close()
        self.wait(1)

    def trade(self, security, price, amount):
        self._set_trade_params(security, price, amount)

        self._submit_trade()

        return self._handle_pop_dialogs(
            handler_class=pop_dialog_handler.TradePopDialogHandler
        )

    def _click(self, control_id):
        self._app.top_window().window(
            control_id=control_id, class_name="Button"
        ).click()

    def _submit_trade(self):
        time.sleep(0.05)
        self._main.window(
            control_id=self._config.TRADE_SUBMIT_CONTROL_ID,
            class_name="Button",
        ).click()

    def _get_pop_dialog_title(self):
        return (
            self._app.top_window()
            .window(control_id=self._config.POP_DIALOD_TITLE_CONTROL_ID)
            .window_text()
        )

    def get_code_type(self, code):
        """
        判断代码是属于那种类型，目前仅支持 ['fund', 'stock']
        :return str 返回code类型, fund 基金 stock 股票
        """
        if code.startswith(("00", "30", "60")):
            return "stock"
        return "fund"

    def round_price_by_code(self, price, code):
        """
        根据代码类型[股票，基金] 截取制定位数的价格
        :param price: 证券价格
        :param code: 证券代码
        :return: str 截断后的价格的字符串表示
        """
        if isinstance(price, str):
            return price

        typ = self.get_code_type(code)
        if typ == "fund":
            return "{:.3f}".format(price)
        return "{:.2f}".format(price)

    def _set_trade_params(self, security, price, amount):
        code = security[-6:]

        self._type_keys(self._config.TRADE_SECURITY_CONTROL_ID, code)

        # wait security input finish
        self.wait(0.1)

        self._type_keys(
            self._config.TRADE_PRICE_CONTROL_ID,
            self.round_price_by_code(price, code),
        )
        self._type_keys(self._config.TRADE_AMOUNT_CONTROL_ID, str(int(amount)))

    def _set_market_trade_params(self, security, amount):
        code = security[-6:]

        self._type_keys(self._config.TRADE_SECURITY_CONTROL_ID, code)

        # wait security input finish
        self.wait(0.1)

        self._type_keys(self._config.TRADE_AMOUNT_CONTROL_ID, str(int(amount)))

    def _get_grid_data(self, control_id, dataType):
        return self.grid_strategy(self).get(control_id, dataType)

    def _type_keys(self, control_id, text):
        self._main.window(
            control_id=control_id, class_name="Edit"
        ).set_edit_text(text)

    def _switch_left_menus(self, path, sleep=0.2):
        self._get_left_menus_handle().get_item(path).click()
        self.wait(sleep)

    def _switch_left_menus_by_shortcut(self, shortcut, sleep=0.5):
        self._app.top_window().type_keys(shortcut)
        self.wait(sleep)

    @functools.lru_cache()
    def _get_left_menus_handle(self):
        for _ in range(3):
            try:
                handle = self._main.window(
                    control_id=129, class_name="SysTreeView32"
                )
                # sometime can't find handle ready, must retry
                handle.wait("ready", 2)
                return handle
            except Exception as ex:
                print('同花顺客户端: _get_left_menus_handle异常: {}'.format(self._main, handle, ex))
        else:
            text = '同花顺客户端: 请重新启动同花顺客户端和DevilYuan!'
            print(text)
            self._info.print(text, DyLogData.error)

    def _cancel_entrust_by_double_click(self, row):
        x = self._config.CANCEL_ENTRUST_GRID_LEFT_MARGIN
        y = (
            self._config.CANCEL_ENTRUST_GRID_FIRST_ROW_HEIGHT
            + self._config.CANCEL_ENTRUST_GRID_ROW_HEIGHT * row
        )
        self._app.top_window().window(
            control_id=self._config.COMMON_GRID_CONTROL_ID,
            class_name="CVirtualGridCtrl",
        ).double_click(coords=(x, y))

    def refresh(self):
        self._switch_left_menus(["买入[F1]"], sleep=0.05)

    def _handle_pop_dialogs(
        self, handler_class=pop_dialog_handler.PopDialogHandler
    ):
        handler = handler_class(self._app)

        while self._is_exist_pop_dialog():
            title = self._get_pop_dialog_title()

            result = handler.handle(title)
            # moyuanz:
            # Here's one assumption that if pop dialog indicates errors with title '提示信息'
            # and then a new dialog should be popped with title '提示', which eventually raises exception to indicate this action failed.
            # If no pop dialog with title '提示', it makes this action return {"message": "success"}.
            #
            # I'm not sure if above assumption valid or not? Currently I think shidenggui gives correct implementation.
            if result:
                return result
        return {"message": "success"}
