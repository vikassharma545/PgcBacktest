import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


def moving_cross_over(data, short_type, short_period, long_type, long_period):
    
    short_period, long_period = int(short_period), int(long_period)

    if short_period > long_period: raise Exception("Sort Period > Long Period")
    
    if short_type == "NORMAL":
        data['short'] = data['close'].rolling(window=short_period).mean()
    elif short_type == "EXPONENTIAL":
        data['short'] = data['close'].ewm(span=short_period, adjust=False).mean()

    if long_type == "NORMAL":
        data['long'] = data['close'].rolling(window=long_period).mean()
    elif long_type == "EXPONENTIAL":
        data['long'] = data['close'].ewm(span=long_period, adjust=False).mean()

    data.dropna(inplace=True)
    data['signal'] = 0
    data['signal'] = (data['short'] > data['long'])*1.0
    data['positions'] = data['signal'].diff()
    data['BUY'] = data['positions'].replace({-1:'PE', 1:'CE', 0: ''})
    data['SELL'] = data['positions'].replace({-1:'CE', 1:'PE', 0: ''})
    return data


def plot_moving_crossover_signals(data, title="Moving Average Crossover Signals"):
    plt.figure(figsize=(14, 7))

    # Plot close price and moving averages
    plt.plot(data['datetime'], data['close'], label='Close Price', color='gray', alpha=0.5)
    plt.plot(data['datetime'], data['short'], label='Short MA', color='blue')
    plt.plot(data['datetime'], data['long'], label='Long MA', color='red')

    # Identify crossover points
    cross_points = data[data['positions'].isin([1.0, -1.0])]

    # Draw vertical lines and print time on x-axis
    for _, row in cross_points.iterrows():
        plt.axvline(x=row['datetime'], color='black', linestyle='--', alpha=0.3)
        plt.text(row['datetime'], data['close'].min(), row['datetime'].strftime('%H:%M'),
                rotation=90, fontsize=8, va='bottom', ha='center', color='black')

    # Basic chart settings
    plt.title(title)
    plt.xlabel('Datetime')
    plt.ylabel('Price')
    plt.xticks(rotation=45)
    plt.legend(loc='best')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    
    
def ATR(df, period=10):
    
    high = df['high'].to_numpy()
    low = df['low'].to_numpy()
    close = df['close'].to_numpy()

    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan

    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)

    tr = np.nanmax(np.vstack([tr1, tr2, tr3]), axis=0)

    atr = np.full_like(tr, fill_value=np.nan)
    atr[period - 1] = np.nanmean(tr[:period])

    for i in range(period, len(tr)):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

    return pd.Series(atr, index=df.index)


def supertrend(df, period=10, multiplier=3):
    atr = ATR(df, period).to_numpy()
    
    high = df['high'].to_numpy()
    low = df['low'].to_numpy()
    close = df['close'].to_numpy()
    hl2 = (high + low) / 2

    upperband = hl2 + multiplier * atr
    lowerband = hl2 - multiplier * atr

    supertrend = np.full(len(df), np.nan)
    trend = np.full(len(df), np.nan)
    trend[period - 1] = True

    for i in range(period, len(df)):
        if close[i] > upperband[i - 1]:
            trend[i] = True
        elif close[i] < lowerband[i - 1]:
            trend[i] = False
        else:
            trend[i] = trend[i - 1]
            if trend[i]:
                lowerband[i] = max(lowerband[i], lowerband[i - 1])
            else:
                upperband[i] = min(upperband[i], upperband[i - 1])

        supertrend[i] = lowerband[i] if trend[i] else upperband[i]

    df['supertrend'] = supertrend
    df['supertrend_direction'] = trend
    
    df['signal'] = trend.astype(float)  
    df['positions'] = df['signal'].diff()

    df['BUY'] = df['positions'].replace({1.0: 'CE', -1.0: 'PE', 0.0: ''})
    df['SELL'] = df['positions'].replace({1.0: 'PE', -1.0: 'CE', 0.0: ''})

    return df


def plot_supertrend_signals(data, title="SuperTrend Signals"):
    plt.figure(figsize=(14, 7))

    # Plot close price
    plt.plot(data['datetime'], data['close'], label='Close Price', color='gray', alpha=0.5)

    # Color-coded SuperTrend line
    for i in range(1, len(data)):
        if not np.isnan(data['supertrend'].iloc[i - 1]) and not np.isnan(data['supertrend'].iloc[i]):
            color = 'green' if data['supertrend_direction'].iloc[i] else 'red'
            plt.plot(data['datetime'].iloc[i - 1:i + 1],
                     data['supertrend'].iloc[i - 1:i + 1],
                     color=color, linewidth=2)

    # Signal timestamps (optional)
    cross_points = data[data['positions'].isin([1.0, -1.0])]
    for _, row in cross_points.iterrows():
        color = 'green' if row['positions'] == 1.0 else 'red'
        plt.axvline(x=row['datetime'], color=color, linestyle='--', alpha=0.3)
        plt.text(row['datetime'], data['close'].min(), row['datetime'].strftime('%H:%M'),
                 rotation=90, fontsize=8, va='bottom', ha='center', color=color)

    # Custom legend
    custom_lines = [
        Line2D([0], [0], color='gray', lw=2, label='Close Price'),
        Line2D([0], [0], color='green', lw=2, label='SuperTrend (Uptrend)'),
        Line2D([0], [0], color='red', lw=2, label='SuperTrend (Downtrend)')
    ]
    plt.legend(handles=custom_lines, loc='best')

    # Chart settings
    plt.title(title)
    plt.xlabel('Datetime')
    plt.ylabel('Price')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def rsi(df, period=14, upper=70, lower=30):
    close = df['close'].to_numpy()
    delta = np.diff(close, prepend=np.nan)

    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)

    avg_gain = np.empty_like(close)
    avg_loss = np.empty_like(close)
    avg_gain[:period] = np.nan
    avg_loss[:period] = np.nan

    avg_gain[period] = np.mean(gain[1:period + 1])
    avg_loss[period] = np.mean(loss[1:period + 1])

    for i in range(period + 1, len(close)):
        avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gain[i]) / period
        avg_loss[i] = (avg_loss[i - 1] * (period - 1) + loss[i]) / period

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    df['rsi'] = rsi
    df['rsi_upper'] = upper
    df['rsi_lower'] = lower

    df['signal'] = np.where(rsi < lower, 1.0, np.where(rsi > upper, -1.0, 0.0))
    df['signal'] = np.where(np.isnan(rsi), 0.0, df['signal'])

    df['positions'] = df['signal'].diff()

    df['BUY'] = df['positions'].replace({1.0: 'CE', -1.0: '', 0.0: ''})
    df['SELL'] = df['positions'].replace({1.0: '', -1.0: 'PE', 0.0: ''})

    return df

def plot_rsi_signals(df, title="RSI (Relative Strength Index)"):
    upper = df['rsi_upper'].iloc[-1] if 'rsi_upper' in df else 70
    lower = df['rsi_lower'].iloc[-1] if 'rsi_lower' in df else 30

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True, gridspec_kw={'height_ratios': [2, 1]})

    # Plot close price in top chart
    ax1.plot(df['datetime'], df['close'], label='Close Price', color='black', linewidth=1)
    ax1.set_title("Close Price")
    ax1.set_ylabel("Price")
    ax1.grid(True)
    ax1.legend(loc='upper left')

    # Draw vertical lines and time labels only on close price chart
    if 'positions' in df.columns:
        cross_points = df[df['positions'].isin([1.0, -1.0])]
        for _, row in cross_points.iterrows():
            ax1.axvline(x=row['datetime'], color='black', linestyle='--', alpha=0.3)
            ax1.text(row['datetime'], df['close'].min(), row['datetime'].strftime('%H:%M'),
                     rotation=90, fontsize=8, va='bottom', ha='center', color='black')

    # Plot RSI in bottom chart
    ax2.plot(df['datetime'], df['rsi'], label='RSI', color='blue', linewidth=1.5)
    ax2.axhline(upper, color='red', linestyle='--', linewidth=1, label=f'Overbought ({upper})')
    ax2.axhline(lower, color='green', linestyle='--', linewidth=1, label=f'Oversold ({lower})')
    ax2.axhline(50, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)

    ax2.set_title(title)
    ax2.set_xlabel("Datetime")
    ax2.set_ylabel("RSI")
    ax2.set_ylim(0, 100)
    ax2.grid(True)
    ax2.legend(loc='upper left')

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()