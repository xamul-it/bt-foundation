#region imports
from AlgorithmImports import *
#endregion

class DualMomentumTechStocksEnhanced(QCAlgorithm):
    """
    Enhanced Dual Momentum Strategy for Tech Stocks
    
    Strategy:
    - Universe: Configurable tech stocks (default: AMD, TSLA, AMZN, AAPL, SPXL)
    - Momentum Score: Weighted sum of 1-month, 3-month, and 6-month returns
    - Rebalance: Configurable (default: weekly)
    - Position: 100% in single stock with highest momentum score
    
    Features:
    - Configurable lookback periods
    - Optional momentum weighting
    - Detailed performance tracking
    - Risk management options
    
    Based on: https://github.com/johnhou13579/Dual-Momentum-Trading-Bot
    """
    
    def Initialize(self):
        # === BASIC SETTINGS ===
        self.SetStartDate(2014, 1, 1)
        self.SetEndDate(2020, 9, 11)
        self.SetCash(100000)
        
        # === STRATEGY PARAMETERS ===
        # Momentum lookback periods (in trading days)
        self.lookback_1m = 21   # ~1 month
        self.lookback_3m = 63   # ~3 months
        self.lookback_6m = 126  # ~6 months
        
        # Momentum weighting (set all to 1.0 for equal weight, or adjust)
        # Higher weight = more importance in final score
        self.weight_1m = 1.0
        self.weight_3m = 1.0
        self.weight_6m = 1.0
        
        # Rebalance frequency
        # Options: "weekly", "monthly", "bimonthly", "quarterly"
        self.rebalance_frequency = "weekly"
        
        # === UNIVERSE ===
        self.tickers = ["AMD", "TSLA", "AMZN", "AAPL", "SPXL"]
        
        self.symbols = {}
        for ticker in self.tickers:
            self.symbols[ticker] = self.AddEquity(ticker, Resolution.Daily).Symbol
        
        # === STATE TRACKING ===
        self.current_holding = None
        self.momentum_history = []  # Track momentum scores over time
        
        # === SCHEDULING ===
        self.SetRebalanceSchedule()
        
        # Warm up period
        warmup_days = max(self.lookback_1m, self.lookback_3m, self.lookback_6m) + 10
        self.SetWarmUp(timedelta(days=warmup_days))
        
        # === CHARTS ===
        self.InitializeCharts()
        
    def SetRebalanceSchedule(self):
        """Set up rebalancing schedule based on frequency parameter"""
        if self.rebalance_frequency == "monthly":
            self.rebalance_months = list(range(1, 13))  # All months
        elif self.rebalance_frequency == "bimonthly":
            self.rebalance_months = [1, 3, 5, 7, 9, 11]  # Every other month
        elif self.rebalance_frequency == "quarterly":
            self.rebalance_months = [1, 4, 7, 10]  # Quarterly
        elif self.rebalance_frequency == "weekly":
            self.rebalance_months = list(range(1, 13))  # All months (handled by weekly schedule)
        else:
            raise ValueError(f"Invalid rebalance frequency: {self.rebalance_frequency}")
        
        # Schedule rebalancing
        if self.rebalance_frequency == "weekly":
            # Rebalance every Monday
            self.Schedule.On(
                self.DateRules.Every(DayOfWeek.Monday),
                self.TimeRules.AfterMarketOpen(self.tickers[0], 30),
                self.Rebalance
            )
        else:
            # Monthly, bimonthly, or quarterly
            self.Schedule.On(
                self.DateRules.MonthStart(self.tickers[0]),
                self.TimeRules.AfterMarketOpen(self.tickers[0], 30),
                self.Rebalance
            )
    
    def InitializeCharts(self):
        """Initialize custom charts for tracking"""
        # Momentum score chart
        momentum_chart = Chart("Momentum Scores")
        for ticker in self.tickers:
            momentum_chart.AddSeries(Series(ticker, SeriesType.Line, ""))
        self.AddChart(momentum_chart)
        
        # Holdings chart
        holdings_chart = Chart("Current Holding")
        holdings_chart.AddSeries(Series("Holding Value", SeriesType.Line, "$"))
        self.AddChart(holdings_chart)
    
    def CalculateMomentumScore(self, symbol, name):
        """
        Calculate momentum score for a given symbol
        
        Returns:
            dict with momentum score and component returns, or None if calculation fails
        """
        try:
            # Get historical prices (get extra to be safe)
            history_days = self.lookback_6m + 20
            history = self.History(symbol, history_days, Resolution.Daily)
            
            if history.empty or len(history) < self.lookback_6m:
                return None
            
            # Get closing prices
            closes = history['close']
            current_price = closes.iloc[-1]
            
            # Calculate returns for each period
            returns = {}
            
            # 1-month return
            if len(closes) >= self.lookback_1m:
                past_price = closes.iloc[-self.lookback_1m]
                returns['1m'] = (current_price - past_price) / past_price
            else:
                returns['1m'] = 0
            
            # 3-month return
            if len(closes) >= self.lookback_3m:
                past_price = closes.iloc[-self.lookback_3m]
                returns['3m'] = (current_price - past_price) / past_price
            else:
                returns['3m'] = 0
            
            # 6-month return
            if len(closes) >= self.lookback_6m:
                past_price = closes.iloc[-self.lookback_6m]
                returns['6m'] = (current_price - past_price) / past_price
            else:
                returns['6m'] = 0
            
            # Calculate weighted momentum score
            momentum_score = (
                returns['1m'] * self.weight_1m +
                returns['3m'] * self.weight_3m +
                returns['6m'] * self.weight_6m
            )
            
            return {
                'symbol': symbol,
                'score': momentum_score,
                'returns': returns,
                'current_price': current_price
            }
            
        except Exception as e:
            self.Debug(f"Error calculating momentum for {name}: {str(e)}")
            return None
    
    def Rebalance(self):
        """Calculate momentum scores and rebalance portfolio"""
        # Skip if warming up
        if self.IsWarmingUp:
            return
        
        # Only check months if NOT weekly
        if self.rebalance_frequency != "weekly":
            if self.Time.month not in self.rebalance_months:
                return
        
        self.Debug(f"\n{'='*60}")
        self.Debug(f"REBALANCING - {self.Time.strftime('%Y-%m-%d')}")
        self.Debug(f"{'='*60}")
        
        # Calculate momentum scores for all symbols
        momentum_scores = {}
        
        for name, symbol in self.symbols.items():
            result = self.CalculateMomentumScore(symbol, name)
            
            if result is not None:
                momentum_scores[name] = result
                
                # Log details
                returns = result['returns']
                self.Debug(
                    f"{name:6s}: Score={result['score']:7.4f} | "
                    f"1m={returns['1m']:7.2%} | "
                    f"3m={returns['3m']:7.2%} | "
                    f"6m={returns['6m']:7.2%}"
                )
                
                # Plot momentum score
                self.Plot("Momentum Scores", name, result['score'])
        
        # Find the symbol with highest momentum score
        if not momentum_scores:
            self.Debug("No momentum scores calculated, skipping rebalance")
            return
        
        best_name = max(momentum_scores.items(), key=lambda x: x[1]['score'])[0]
        best_data = momentum_scores[best_name]
        best_symbol = best_data['symbol']
        best_score = best_data['score']
        
        self.Debug(f"\n{'*'*60}")
        self.Debug(f"WINNER: {best_name} with momentum score: {best_score:.4f}")
        self.Debug(f"{'*'*60}\n")
        
        # Store momentum history for analysis
        self.momentum_history.append({
            'date': self.Time,
            'winner': best_name,
            'score': best_score,
            'all_scores': {k: v['score'] for k, v in momentum_scores.items()}
        })
        
        # Execute trade if needed
        if self.current_holding != best_symbol:
            # Liquidate current position
            if self.current_holding is not None:
                old_name = [n for n, s in self.symbols.items() if s == self.current_holding][0]
                self.Liquidate(self.current_holding)
                self.Debug(f"→ Liquidated {old_name}")
            
            # Enter new position (100% of portfolio)
            self.SetHoldings(best_symbol, 1.0)
            self.current_holding = best_symbol
            self.Debug(f"→ Entered {best_name} at 100%")
            
        else:
            self.Debug(f"→ No change, continuing to hold {best_name}")
        
        # Plot current holding value
        if self.current_holding:
            holding_value = self.Portfolio[self.current_holding].HoldingsValue
            self.Plot("Current Holding", "Holding Value", holding_value)
    
    def OnData(self, data):
        """OnData event - all logic in scheduled rebalance"""
        pass
    
    def OnEndOfAlgorithm(self):
        """Called at the end of the algorithm"""
        self.Debug(f"\n{'='*60}")
        self.Debug("ALGORITHM SUMMARY")
        self.Debug(f"{'='*60}")
        self.Debug(f"Start Date: {self.StartDate.strftime('%Y-%m-%d')}")
        self.Debug(f"End Date: {self.EndDate.strftime('%Y-%m-%d')}")
        self.Debug(f"Initial Capital: ${100000:,.2f}")
        self.Debug(f"Final Portfolio Value: ${self.Portfolio.TotalPortfolioValue:,.2f}")
        
        total_return = (self.Portfolio.TotalPortfolioValue - 100000) / 100000
        self.Debug(f"Total Return: {total_return:.2%}")
        
        if self.current_holding:
            final_stock = [n for n, s in self.symbols.items() if s == self.current_holding][0]
            self.Debug(f"Final Holding: {final_stock}")
        
        # Analyze momentum history
        if self.momentum_history:
            self.Debug(f"\nTotal Rebalances: {len(self.momentum_history)}")
            
            # Count holdings
            holdings_count = {}
            for entry in self.momentum_history:
                winner = entry['winner']
                holdings_count[winner] = holdings_count.get(winner, 0) + 1
            
            self.Debug("\nHolding Frequency:")
            for ticker, count in sorted(holdings_count.items(), key=lambda x: x[1], reverse=True):
                pct = count / len(self.momentum_history) * 100
                self.Debug(f"  {ticker}: {count} times ({pct:.1f}%)")
        
        self.Debug(f"{'='*60}\n")