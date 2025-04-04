import pandas as pd
import matplotlib.pyplot as plt


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

