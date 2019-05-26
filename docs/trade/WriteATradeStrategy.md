# 写一个实盘策略
在Stock\Trade\Strategy\Cta下添加一个策略文件，或者你可以在Stock\Trade\Strategy下创建新的目录夹，然后把策略添加到这个新目录夹下  
![image](https://github.com/moyuanz/DevilYuan/blob/master/docs/trade/strategyPath.png)

### 注意
由于支持自动载入策略到UI，策略的类名一定要跟策略的文件名一致。

# 策略回测
在主窗口打开策略回测窗口，勾选对应的策略即可回测。回测采用的类事件方式，跟实盘保持一致，这样导致回测比较慢。  
![image](https://github.com/moyuanz/DevilYuan/blob/master/docs/backtesting/resultDetails.png)

# 策略实盘
在主窗口打开实盘窗口。   
![image](https://github.com/moyuanz/DevilYuan/blob/master/docs/trade/trade.png)


# 策略文件解析

# 策略向量回测
在Stock\Trade\Strategy\Cta下，策略DyST_DoubleMa_BT使用了向量回测。此策略是纯向量回测，由于向量回测跟实盘有不少不同，所以相应的实盘策略需要重新写。纯向量回测的策略必须以"_BT"结尾，这样实盘界面不会载入此策略。
