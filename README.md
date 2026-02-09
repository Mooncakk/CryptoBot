# CryptoBot - Automated Cryptocurrency Trading System

A comprehensive data engineering pipeline and automated trading bot for cryptocurrency markets using Hyperliquid exchange. This system orchestrates data ingestion, data modeling, and automated trading decisions.
## 🏗️ Architecture Overview

This project follows a modern data engineering architecture pattern, similar to how a factory assembly line processes raw materials into finished products:

- **Raw Data Collection**: Like gathering raw materials from suppliers
- **Data Processing**: Similar to quality control and refinement processes
- **Data Storage**: Acts as a warehouse for processed inventory
- **Trading Bot**: The automated decision-maker, like a smart factory supervisor

## 🔧 Core Components

### 1. Data Pipeline (`etl/`)
- **`extraction.py`**: Collects OHLCV (Open, High, Low, Close, Volume) data from Hyperliquid
- **`transformation.py`**: Processes raw data and converts timestamps
- **`load.py`**: Loads processed data into Snowflake data warehouse

### 2. Trading Bot (`bot/`)
- **`trading_bot.py`**: Automated trading logic using RSI (Relative Strength Index) indicators
- Supports both long and short positions
- Implements risk management with position sizing

### 3. Workflow Orchestration (`workflow_etl1.py`)
- Apache Airflow DAG for automated pipeline execution
- Runs every 2 hours with retry logic and notifications
- Sequential execution: Extract → Transform → Load → Trade

### 4. Utilities (`utils/`)
- **`notifications.py`**: Email notifications for pipeline status and failures

## 🚀 Features

### Data Engineering
- **Multi-source data collection** from Hyperliquid API
- **S3 integration** for data lake storage
- **Snowflake data warehouse** for structured data storage
- **Automated data quality checks** and transformations

### Trading Capabilities
- **RSI-based trading strategy** (Buy when RSI > 60, Sell when RSI < 60)
- **Position management** with automatic sizing
- **Risk controls** and balance monitoring
- **Multi-coin support** through configuration

### Infrastructure
- **Dockerized Airflow** for workflow orchestration
- **AWS S3** for data storage
- **Snowflake** for data warehousing
- **Dbt for data modeling
- **Email notifications** for monitoring

## 📋 Prerequisites

- Python 3.11+
- Apache Airflow 3.x
- AWS Account with S3 access
- Snowflake account
- Dbt
- Hyperliquid API credentials
- SMTP server for notifications

## ⚙️ Configuration

### Snowflake Setup
- Warehouse: `cryptobot_wh`
- Database: `cryptobot_db`
- Schemas : `raw`, `staging`
- Table : `raw_ohclv`
- External stage for data loading

### Trading Parameters
- **RSI Period**: 14 periods
- **Trading Frequency**: Every 2 hours
- **Position Size**: 100% of available balance
- **RSI Thresholds**: Buy > 60, Sell < 60

## 🔄 Workflow

1. **Data Collection** (Every 2 hours)
   - Fetches last 14 hours of 1-hour OHLCV data
   - Stores raw data in S3

2. **Data Processing**
   - with dbt

3. **Data Loading**
   - Loads processed data into Snowflake
   - Updates cryptocurrency tables

4. **Trading Execution**
   - Calculates RSI indicators
   - Makes buy/sell decisions
   - Executes trades on Hyperliquid

## 📊 Monitoring

### Email Notifications
- **Success notifications**: Sent when DAG completes successfully
- **Failure alerts**: Sent when tasks fail or retry
- **Trading alerts**: Logs all trading decisions

### Logging
- Comprehensive logging throughout the pipeline
- Balance and position monitoring
- Trade execution details

## 🔒 Security Considerations

- Store sensitive credentials in environment variables or secure vaults
- Use IAM roles for AWS access
- Implement proper error handling for API failures
- Monitor for unusual trading patterns

## 📈 Trading Strategy

The bot implements a simple RSI-based momentum strategy:

```python
# Buy Signal: RSI > 60 (momentum building)
if rsi > 60 and no_position:
    buy_order = place_market_order()

# Sell Signal: RSI < 60 (momentum weakening)  
if rsi < 60 and has_position:
    sell_order = close_position()
```

## 🚧 Development

### Adding New Cryptocurrencies
1. Update `crypto_wallet` in `utils.json`
2. Ensure the pair is available on Hyperliquid
3. The system will automatically create tables and start trading

### Modifying Trading Strategy
- Edit the trading logic in `bot/trading_bot.py`
- Consider backtesting before deployment
- Update risk parameters as needed

## 📝 Troubleshooting

### Common Issues
- **Connection failures**: Check API credentials and network connectivity
- **Data quality issues**: Verify Hyperliquid API response format
- **Trading errors**: Check account balance and position limits
- **Airflow issues**: Review DAG configuration and dependencies

### Debugging
- Check Airflow logs for detailed error messages
- Verify S3 bucket permissions
- Test Snowflake connectivity
- Validate Hyperliquid API access

## ⚠️ Disclaimer

This trading bot is for educational purposes. Cryptocurrency trading involves substantial risk of loss. Use at your own risk and never invest more than you can afford to lose.
