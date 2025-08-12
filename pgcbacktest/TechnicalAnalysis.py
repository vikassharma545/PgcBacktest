import numpy as np
import pandas as pd


def moving_cross_over(data, short_type, short_period, long_type, long_period):
    
    data = data.copy()
    short_period, long_period = int(short_period), int(long_period)

    if short_period > long_period:
        raise Exception("Sort Period > Long Period")
    
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
    data['BUY'] = data['positions'].replace({-1: 'PE', 1: 'CE', 0: ''})
    data['SELL'] = data['positions'].replace({-1: 'CE', 1: 'PE', 0: ''})
    return data


def ATR(df, period=10):
    
    df = df.copy()
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
    
    df = df.copy()
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


def rsi(df, period=14, upper=70, lower=30):
    
    df = df.copy()
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


def macd(df, fast=12, slow=26, signal=9):
    
    df = df.copy()
    close = df['close']

    # Step 1: EMAs
    ema_fast = close.ewm(span=fast, adjust=False, min_periods=fast).mean()
    ema_slow = close.ewm(span=slow, adjust=False, min_periods=slow).mean()

    # Step 2: MACD Line
    macd_line = ema_fast - ema_slow

    # Delay MACD output to match TA-Lib
    macd_line[:slow + signal - 2] = np.nan

    # Step 3: Signal Line
    signal_line = macd_line.ewm(span=signal, adjust=False, min_periods=signal).mean()

    # Step 4: Histogram
    hist = macd_line - signal_line

    signal_line[:slow + signal - 2] = np.nan
    hist[:slow + signal - 2] = np.nan

    # Assign to DataFrame
    df['macd'] = macd_line
    df['macd_signal'] = signal_line
    df['macd_hist'] = hist

    # Generate crossover signals
    df['signal'] = 0.0
    crossover_up = (df['macd'] > df['macd_signal']) & (df['macd'].shift(1) <= df['macd_signal'].shift(1))
    crossover_down = (df['macd'] < df['macd_signal']) & (df['macd'].shift(1) >= df['macd_signal'].shift(1))
    df.loc[crossover_up, 'signal'] = 1.0
    df.loc[crossover_down, 'signal'] = -1.0

    df['positions'] = df['signal']
    df['BUY'] = df['positions'].replace({1.0: 'CE', -1.0: '', 0.0: ''})
    df['SELL'] = df['positions'].replace({1.0: '', -1.0: 'PE', 0.0: ''})

    return df

