from DyCommon.Ui.DyTreeWidget import *

from EventEngine.DyEvent import *
from ....Trade.Ui.Basic.DyStockTradeStrategyWidget import * 
from ....Select.Ui.Basic.DyStockSelectStrategyWidget import *
from Stock.Trade.Ui.Basic import DyStockTradeStrategyClsMap


class DyStockBackTestingStrategyWidget(DyStockSelectStrategyWidget):
    """ 只能选中一个策略回测 """

    def __init__(self, paramWidget=None):
        self.__class__.strategyFields = DyStockTradeStrategyWidget.strategyFields
        super().__init__(paramWidget)

    def _transform(self, fields):
        newFields = []
        for field in fields:
            if isinstance(field, list):
                retFields = self._transform(field)
                if retFields:
                    newFields.append(retFields)
            else:
                if hasattr(field,  'chName'):
                    if field.__name__ + '_BT' not in DyStockTradeStrategyClsMap: # If we have pure backtesting strategy, we only add it.
                        newFields.append(field.chName)
                        self._strategies[field.chName] = field
                else:
                    newFields.append(field)

        return newFields