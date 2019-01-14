import os

from Stock import DynamicLoadStrategyFields


# dynamically load strategies from Stock/Select/Strategy
__pathList = os.path.dirname(__file__).split(os.path.sep)
__stratgyPath = os.path.sep.join(__pathList[:-2] + ['Strategy'])

DyStockSelectStrategyClsMap = {}
DyStockSelectStrategyWidgetAutoFields = DynamicLoadStrategyFields(__stratgyPath, 'Stock.Select.Strategy', DyStockSelectStrategyClsMap)
