from DyCommon.Ui.DyTreeWidget import *
from EventEngine.DyEvent import *
from ...DyStockStrategyBase import *

from . import DyStockTradeStrategyWidgetAutoFields


class DyStockTradeStrategyWidget(DyTreeWidget):
    
    strategyFields = DyStockTradeStrategyWidgetAutoFields


    def __init__(self, eventEngine):
        self._strategies = {} # {strategy chName: [state, strategy class]}
        newFields = self._transform(self.__class__.strategyFields)
        
        super().__init__(newFields)
        self.collapse('Obsolete')

        self._eventEngine = eventEngine

        # At last, set tooltip of each strategy to which broker it uses
        for chName, (_, strategyCls) in self._strategies.items():
            itemList =  self.findItems(chName, Qt.MatchExactly|Qt.MatchRecursive, 0)
            assert len(itemList) == 1
            itemList[0].setToolTip(0, 'broker={}'.format(strategyCls.broker))

    def on_itemClicked(self, item, column):
        super(DyStockTradeStrategyWidget, self).on_itemClicked(item, column)

        if item.checkState(0) == Qt.Checked:
            pass

    def on_itemChanged(self, item, column):

        text = item.text(0)

        if item.checkState(0) == Qt.Checked:

            if text in self._strategies:
                strategyState, strategyCls = self._strategies[text]
                strategyState.checkAll(strategyCls, self._eventEngine)

            elif text == '运行' or text == '监控':
                strategyState, strategyCls = self._strategies[item.parent().text(0)]

                state = self._getStateByText(text)
                strategyState.checkState(state, strategyCls, self._eventEngine)
        else:

            if text in self._strategies:
                strategyState, strategyCls = self._strategies[text]
                strategyState.uncheckAll(strategyCls, self._eventEngine)

            elif text == '运行' or text == '监控':
                strategyState, strategyCls = self._strategies[item.parent().text(0)]

                state = self._getStateByText(text)
                strategyState.uncheckState(state, strategyCls, self._eventEngine)

        super().on_itemChanged(item, column)

    def _getStateByText(self, text):
        if text == '运行':
            return DyStockStrategyState.running

        return DyStockStrategyState.monitoring

    def _transform(self, fields):
        newFields = []
        for field in fields:
            if isinstance(field, list):
                retFields = self._transform(field)
                if retFields:
                    newFields.append(retFields)
            else:
                if hasattr(field,  'chName'):
                    if field.__name__[-3:] != '_BT': # ignore pure backtesting strategy
                        newFields.append(field.chName)
                        newFields.append(['运行'])
                        newFields.append(['监控'])

                        self._strategies[field.chName] = [DyStockStrategyState(), field]
                else:
                    newFields.append(field)

        return newFields
