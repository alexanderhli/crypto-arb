import backtester

bt = backtester.Backtester('params_backtest.yaml')
bt.go() 
bt.print_results()
bt.save_results('test_lite_separate_ratio.csv')