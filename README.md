# crypto-arb
This is a realtime trading program written in Python that performs pair trades and statistical arbitrage on a wide variety of cryptocurrency exchanges using multiple currencies. It is a demo of proprietary trading software used and developed by Epsilon Zero LLP.

**Functionality**

The program fetches historical price data, conducts analysis using customizable trading strategies, and can make strategy-based trades in realtime (notifying the user of each trade) or perform backtests of the strategy.

**Sections**
- "Zero" is the earliest prototype that conducts pair trades on a few exchanges, notifying the user by text.
- "One" features more exchanges and contains more advanced features such as modular/customizable strategies, saving the state of the programs + user wallets, and backtesting/executing multiple different strategies in parallel.
- "Archive" contains the strategy and backtesting infrastructure for more complex strategies, using a multi-factor model based on eigenvectors of correlation matrices between more than two currencies.
