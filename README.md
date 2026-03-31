# PGCBacktest — Options Backtesting SDK

PGCBacktest is a Python SDK for backtesting **intraday and weekly options selling/buying strategies** on Indian (NSE/BSE), US, and MCX markets. It uses per-minute OHLC futures data and full option chain data to simulate strategies like straddle selling, strangle selling, re-entry on SL, decay-based entry, trailing stop-loss, portfolio-level SL, and more — across millions of parameter combinations.

---

## Quick Start

### Install

```bash
# Install Git (Windows PowerShell)
winget install --id Git.Git -e --source winget

# Install PGCBacktest
pip install --upgrade --force-reinstall git+https://vikassharma545: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@github.com/vikassharma545/PgcBacktest.git
```

### Dependencies

```
pandas==2.3.3, polars==1.39.0, plotly==6.5.2, numpy==2.2.6, dask==2026.1.2,
numba==0.64.0, streamlit==1.55.0, requests, tqdm
```

Requires Python >= 3.10.

---

## Architecture Overview

```
PGCBacktest/
├── pgcbacktest/                   ← CORE SDK (pip-installable library)
│   ├── __init__.py                ← Re-exports BacktestOptions + BtParameters
│   ├── BacktestOptions.py         ← Main engine: IntradayBacktest, WeeklyBacktest
│   ├── BtParameters.py            ← Parameter loading & Cartesian product generation
│   └── TechnicalAnalysis.py       ← Indicators: MA crossover, SuperTrend, RSI, MACD
│
├── ENGINE/                        ← Orchestration & post-processing tools
│   ├── Run Code.py                ← Converts notebooks → parallel terminal workers
│   ├── DashBoard FileMaker.py     ← Aggregates raw outputs into dashboard Parquets
│   ├── DashBoard FileMakerParquetNormalised.py
│   ├── PgcDashboardExcelDuckDB.py ← Streamlit + DuckDB interactive dashboard
│   └── Combine By ParameterWise.py
│
├── INTRADAY CODES/                ← ~67 intraday strategy notebooks + CSVs
├── WEEKLY CODES/                  ← ~23 weekly strategy notebooks + CSVs
├── TECHNICAL CODES/               ← Indicator testing notebooks
└── setup.py
```

### Data Flow

```
Market Data (.parquet/.pkl)           Parameter CSV        MetaData CSV
  ├── Futures: {date}_{index}_future    (grid values)       (index, DTE, dates)
  └── Options: {date}_{index}              │                      │
              │                            │                      │
              ▼                            ▼                      ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │  Strategy Notebook (.ipynb)                                     │
    │   1. Load parameters → Cartesian product of all columns        │
    │   2. Load metadata → list of dates to process                  │
    │   3. For each date:                                            │
    │      a. bt = IntradayBacktest(pickle_path, index, date, dte...)│
    │      b. For each param combination → run strategy function     │
    │      c. save_chunk_data() → Parquet files                      │
    └─────────────────────────────────────────────────────────────────┘
              │
              ▼
    Output Parquet files → Dashboard FileMaker → Streamlit Dashboard
```

---

## Supported Markets & Indices

| Market | Indices |
|--------|---------|
| **NSE** | NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY |
| **BSE** | SENSEX, BANKEX |
| **MCX** | CRUDEOIL, CRUDEOILM, NATGASMINI, NATURALGAS, COPPER, SILVER, GOLD, SILVERM, GOLDM, ZINC |
| **US** | AAPL, AMD, AMZN, GOOGL, META, MSFT, NVDA, TSLA, SPY (daily variants), SPXW (daily variants), QQQ (daily variants), XSP, VIX, and more |

### Market-Specific Constants

| Index | Strike Gap | Slippage | OTM Step |
|-------|-----------|----------|----------|
| NIFTY | Auto-detected | 1.0% | 1000 |
| BANKNIFTY | Auto-detected | 1.25% | 5000 |
| SENSEX | Auto-detected | 1.25% | 5000 |
| CRUDEOIL | Auto-detected | default 1% | 500 |
| SPXW | Auto-detected | default 1% | 500 |

Tick size: 0.10 for CRUDEOIL, 0.05 for all others.

---

## Data Format Requirements

### Directory Structure (Pickle Path)

```
PICKLE/                          ← Intraday (DTE < 7 days)
├── DTE.csv                      ← Days-to-expiry calendar
├── Nifty Future/                ← Future OHLC per date
│   ├── 2024-01-15_nifty_future.parquet
│   └── ...
├── Nifty Options/               ← Option chain per date
│   ├── 2024-01-15_nifty.parquet
│   └── ...
├── BN Future/                   ← BANKNIFTY futures
├── BN Options/                  ← BANKNIFTY options
└── ...

MPICKLE/                         ← Monthly/weekly (DTE >= 7)
MCXPICKLE/                       ← MCX commodities
USPICKLE/                        ← US intraday (DTE < 7)
MUSPICKLE/                       ← US monthly (DTE >= 7)
```

### Folder Name Prefixes

| Index | Prefix for folders |
|-------|--------------------|
| NIFTY | `Nifty Future/`, `Nifty Options/` |
| BANKNIFTY | `BN Future/`, `BN Options/` |
| FINNIFTY | `FN Future/`, `FN Options/` |
| MIDCPNIFTY | `MCN Future/`, `MCN Options/` |
| SENSEX | `SX Future/`, `SX Options/` |
| BANKEX | `BX Future/`, `BX Options/` |
| Others | `{INDEX} Future/`, `{INDEX} Options/` |

### Future Data File Format

File: `{date}_{index}_future.parquet` (or `.pkl`)

| Column | Type | Description |
|--------|------|-------------|
| date_time | datetime | Per-minute timestamp |
| open | float | Open price |
| high | float | High price |
| low | float | Low price |
| close | float | Close price |

### Option Chain Data File Format

File: `{date}_{index}.parquet` (or `.pkl`)

| Column | Type | Description |
|--------|------|-------------|
| scrip | string | Strike + option type, e.g. `"21500CE"`, `"21500PE"` |
| date_time | datetime | Per-minute timestamp |
| open | float | Open price |
| high | float | High price |
| low | float | Low price |
| close | float | Close price |

### DTE.csv Format

| Column | Description |
|--------|-------------|
| Date | Trading date (index column, format DD-MM-YYYY) |
| NIFTY | Days to expiry for NIFTY on that date |
| BANKNIFTY | Days to expiry for BANKNIFTY on that date |
| ... | One column per index |

---

## Core SDK Reference

### Imports

```python
from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *
```

This gives you access to all classes and functions: `IntradayBacktest`, `WeeklyBacktest`, `get_parameter_data`, `get_meta_data`, `get_meta_row_data`, `is_file_exists`, `claim_date`, `save_chunk_data`, `chunk_size`, `get_pm_time_index`, `set_pm_time_index`, `cv`, `cal_percent`.

---

### Class: `IntradayBacktest`

The main backtesting engine. Loads all futures + options data for **one trading day** and provides methods for strike selection, SL/TP checking, decay checking, and MTM tracking.

#### Constructor

```python
bt = IntradayBacktest(pickle_path, index, current_date, dte, start_time, end_time)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| pickle_path | str | Path to data directory, e.g. `"P:/PGC Data/PICKLE/"` |
| index | str | Index name, e.g. `"NIFTY"`, `"BANKNIFTY"`, `"SENSEX"` |
| current_date | datetime | The trading date (from date_lists) |
| dte | int | Days to expiry filter value |
| start_time | datetime.time | Market data start time, e.g. `time(9,15)` |
| end_time | datetime.time | Market data end time, e.g. `time(15,29)` |

#### Properties available after init

| Property | Description |
|----------|-------------|
| `bt.future_data` | DataFrame with OHLC indexed by datetime |
| `bt.options` | Full option chain DataFrame |
| `bt.gap` | Strike gap (auto-detected, e.g. 50 for NIFTY) |
| `bt.market` | Market string: `'NSE'`, `'BSE'`, `'MCX'`, `'US'` |
| `bt.current_date` | The date being processed |
| `bt.index` | The index name |
| `bt.dte` | Days to expiry |
| `bt.tick_size` | Minimum price tick (0.05 default, 0.10 for CRUDEOIL) |

---

### Strike Selection Methods

All strike methods return: `(ce_scrip, pe_scrip, ce_price, pe_price, future_price, entry_datetime)` — returns all `None` if no valid strike found.

#### `bt.get_strike(start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1, only=None, SDroundoff=False)`

**Unified strike selector.** Routes to straddle/strangle/UT based on `om` format:

| `om` value | Behavior | Example |
|-----------|----------|---------|
| `0` or `None` | ATM Straddle | `om=0` |
| `> 0` (float) | OTM Strangle (om × one_OTM premium) | `om=0.3` |
| `"0.5SD"` | SD-offset straddle (shift by 0.5 × straddle premium) | `om="0.5SD"` |
| `"-0.5SD"` | Negative SD-offset (widen strikes) | `om="-0.5SD"` |
| `"ATM"` | Pure ATM straddle | `om="ATM"` |
| `"ATM2"` | ATM + 2 strikes OTM | `om="ATM2"` |
| `"ATM-1"` | ATM - 1 strike ITM | `om="ATM-1"` |
| `"ATM5%"` | ATM + 5% of future price offset | `om="ATM5%"` |
| `"50%"` | Target = 50% of one OTM premium | `om="50%"` |

**`only` parameter:** If `only="CE"`, returns `(ce_scrip, ce_price, future_price, entry_dt)`. Same for `only="PE"`.

**`tf` parameter:** Target factor for strangle. Default 1.

```python
# ATM Straddle
ce, pe, ce_p, pe_p, fut, entry_dt = bt.get_strike(start_dt, end_dt, om=0)

# OTM Strangle (0.3x OTM)
ce, pe, ce_p, pe_p, fut, entry_dt = bt.get_strike(start_dt, end_dt, om=0.3)

# Only CE leg
ce_scrip, ce_price, fut, entry_dt = bt.get_strike(start_dt, end_dt, om=0, only="CE")
```

#### `bt.get_straddle_strike(start_dt, end_dt, sd=0, atm=None, SDroundoff=False)`

Find ATM straddle using **synthetic future**:
1. Round future price to nearest strike gap
2. Calculate synthetic future = CE_price - PE_price + strike
3. Test 3 candidate strikes around synthetic ATM
4. Pick pair with minimum `|CE_price - PE_price|`
5. Apply SD offset or ATM±N offset if requested

#### `bt.get_strangle_strike(start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1)`

Find OTM strangle where each leg has premium >= target (derived from `om × one_OTM`). Balanced so `|CE_price - PE_price|` is minimized and combined premium is between 2x and 3x target.

#### `bt.get_ut_strike(start_dt, end_dt, om=None, target=None)`

Find cheapest options with premium >= target. Used for "ultra-target" strike selection.

---

### Stop-Loss & Exit Methods

#### `bt.sl_check_single_leg(start_dt, end_dt, scrip, ...)`

Checks SL/Target on a **single option leg** (CE or PE separately).

```python
sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl = bt.sl_check_single_leg(
    start_dt, end_dt, scrip,
    o=None,                   # Entry price (None = use close at start_dt)
    sl=30,                    # SL as % (e.g. 30 = exit if premium rises 30%)
    intra_sl=0,               # Intra-candle SL % (checked against high, not close)
    sl_price=None,            # Explicit SL price (overrides sl%)
    target_price=None,        # Explicit target price
    from_candle_close=False,  # True = check SL on candle close; False = on high/low
    orderside='SELL',         # 'SELL' or 'BUY'
    from_next_minute=True,    # Skip entry candle for SL check
    with_ohlc=False,          # Return extra OHLC data
    pl_with_slipage=True,     # Include slippage in PNL
    per_minute_mtm=False,     # Return minute-by-minute MTM series
    roundtick=False           # Round SL/target to tick size
)
```

**Return values depend on flags:**

| Flags | Returns |
|-------|---------|
| Default | `(sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)` |
| `with_ohlc=True` | `(open, high, low, close, sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)` |
| `per_minute_mtm=True` | `(exit_time, pd.Series)` — Series indexed by datetime with per-minute MTM |
| `with_ohlc=True` + `per_minute_mtm=True` | `(open, high, low, close, sl_price, exit_time, pd.Series)` |

**SL Logic for SELL orders:**
- `sl_price = entry × (1 + sl/100)`
- SL hit when `high >= sl_price` (or `close >= sl_price` if `from_candle_close=True`)
- PNL = `entry - exit_price - slippage`

**SL Logic for BUY orders:**
- `sl_price = entry × (1 - sl/100)`
- SL hit when `low <= sl_price` (or `close <= sl_price` if `from_candle_close=True`)
- PNL = `exit_price - entry - slippage`

#### `bt.sl_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, ...)`

Checks SL/Target on **combined straddle/strangle premium** (CE close + PE close).

```python
# Default return:
sl_price, intra_sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl = bt.sl_check_combine_leg(
    start_dt, end_dt, ce_scrip, pe_scrip,
    o=None,                   # Combined entry price
    sl=30,                    # Combined SL %
    intra_sl=0,               # Intra-candle SL % (checked on combined high)
    sl_price=None,            # Explicit SL price
    intra_sl_price=None,      # Explicit intra-SL price
    target_price=None,        # Explicit target price
    orderside='SELL',
    from_next_minute=True,
    with_ohlc=False,          # Returns (o, h, l, c, sl_price, intra_sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
    pl_with_slipage=True,
    per_minute_mtm=False,
    roundtick=False
)
```

**Combined high/low calculation:**
```
combined_high = max(CE_high + PE_low, CE_low + PE_high)
combined_low  = min(CE_high + PE_low, CE_low + PE_high)
combined_close = CE_close + PE_close
```

**For combine leg SL:** SL is checked against `close` (not high), because combined high/low is an estimate.

#### `bt.sl_check_single_leg_with_sl_trail(start_dt, end_dt, scrip, ...)`

Single leg with **trailing stop-loss**. The SL ratchets in your favor as the premium moves.

```python
sl_price, sl_flag, trail_flag, exit_time, pnl = bt.sl_check_single_leg_with_sl_trail(
    start_dt, end_dt, scrip,
    o=None, sl=30,
    sl_price=None,
    trail=20,          # Profit trigger % to ratchet SL
    sl_trail=10,       # How much to tighten SL when trail triggers
    from_candle_close=False,
    orderside='SELL',
    from_next_minute=True,
    with_ohlc=False,
    pl_with_slipage=True,
    per_minute_mtm=False,
    roundtick=False
)
```

**Trail logic (SELL):** If premium drops to `trail%` of entry → new SL = `old_SL × (1 - sl_trail/100)`. The trail and SL keep ratcheting each time the trail trigger is hit again.

**Special case:** When `trail=0` and `sl_trail != 0` and `sl == sl_trail`, it does **per-minute trailing** — SL is recalculated based on each candle's low/close.

#### `bt.sl_check_combine_leg_with_sl_trail(start_dt, end_dt, ce_scrip, pe_scrip, ...)`

Combined leg version of trailing SL.

```python
sl_price, intra_sl_price, sl_flag, intra_sl_flag, trail_flag, exit_time, pnl = bt.sl_check_combine_leg_with_sl_trail(
    start_dt, end_dt, ce_scrip, pe_scrip,
    o=None, sl=30, intra_sl=0,
    sl_price=None, intra_sl_price=None,
    trail=20, sl_trail=10,
    orderside='SELL',
    from_next_minute=True,
    with_ohlc=False,
    pl_with_slipage=True,
    per_minute_mtm=False,
    roundtick=False
)
```

---

### Decay Check Methods

#### `bt.decay_check_single_leg(start_dt, end_dt, scrip, ...)`

Checks if an option premium **decays to a target level**. Used for "enter after premium drops" strategies.

```python
decay_price, decay_flag, decay_time = bt.decay_check_single_leg(
    start_dt, end_dt, scrip,
    decay=20,                 # Decay target as % (e.g. 20 = wait for 20% drop)
    decay_price=None,         # Explicit decay price (overrides decay%)
    from_candle_close=False,  # Check on close vs low
    orderside='SELL',
    from_next_minute=True,
    with_ohlc=False,          # Returns (o, h, l, c, decay_price, decay_flag, decay_time)
    roundtick=False
)
```

**Decay logic (SELL):** `decay_price = entry × (1 - decay/100)`. Decay hit when `low <= decay_price` (or `close` if `from_candle_close`).

**Special:** If `decay=0` or `decay_price=-1`, decay is immediate (returns entry time).

---

### Straddle Indicator

#### `bt.straddle_indicator(start_dt, end_dt, si_indicator, si_buffer, buffer_min)`

Checks if straddle premium drops below a threshold based on recent history.

| Parameter | Type | Description |
|-----------|------|-------------|
| si_indicator | str | `'LOW'`, `'HIGH'`, or `'AVG'` — how to calculate reference premium |
| si_buffer | float | Buffer multiplier (e.g. `0.05` = 5% above reference) |
| buffer_min | int | Number of minutes to look back for reference |

Returns: `(flag: bool, entry_time: datetime)` — `True` if premium drops to threshold.

---

### Per-Minute MTM Helpers

```python
# Build time index for a date
time_index = get_pm_time_index(date, start_time, end_time)

# Reindex a MTM series to the full time index (forward-fill)
full_mtm = set_pm_time_index(mtm_series, time_index)
```

---

### Class: `WeeklyBacktest`

Inherits from `IntradayBacktest`. Loads data for **multiple days** (an entire expiry week).

```python
wbt = WeeklyBacktest(pickle_path, index, week_dates, from_dte, to_dte, start_time, end_time)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| week_dates | list[datetime] | All trading dates in the week |
| from_dte | int | Start DTE of the week slice |
| to_dte | int | End DTE of the week slice |

All `IntradayBacktest` methods are available, plus:

| Method | Description |
|--------|-------------|
| `wbt.get_EOD_straddle_strike(current_date)` | Find ATM straddle at end-of-day (scans backward up to 15 min from close) |
| `wbt.get_sl_range(strike, premium, range_sl, intra_range_sl)` | Calculate price range band for range-based SL |
| `wbt.get_synthetic_future(strike, ce_price, pe_price)` | Calculate synthetic future from straddle prices |
| `wbt.sl_range_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, lower_range, upper_range, intra_lower_range, intra_upper_range, straddle_strike, ...)` | Range-based SL check on combined legs — triggers SL when synthetic future breaches a price range band |
| `wbt.sl_range_trail_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, lower_range, upper_range, intra_lower_range, intra_upper_range, straddle_strike, straddle_price, sl, intra_sl, ...)` | Range-based SL with trailing — combines range band SL with traditional trailing SL logic |

**Range-based SL usage example (weekly strategies):**
```python
lower, upper, intra_lower, intra_upper = wbt.get_sl_range(straddle_strike, straddle_premium, range_sl=2.0, intra_range_sl=1.5)
result = wbt.sl_range_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip,
    lower, upper, intra_lower, intra_upper, straddle_strike,
    orderside='SELL', per_minute_mtm=True)
```

Weekly notebooks use `wbt.current_week_dates[0]` for start date and `wbt.current_week_dates[-1]` for end date.

---

## Parameter System

### Parameter CSV Format

Each column contains possible values. The SDK generates the **Cartesian product** of all columns. Empty cells are ignored per column.

Example `Parameter_B120.csv`:
```csv
entry_time,exit_time,orderside,method,sl,ut_sl,om
09:20:00,15:25:00,sell,HL,20,50,0
09:30:00,,,,30,70,0.1
09:40:00,,,,40,80,0.2
09:50:00,,,,50,90,0.3
10:00:00,,,,60,100,0.4
10:10:00,,,,70,,
```

This generates: `28 entry_times × 1 exit_time × 1 orderside × 1 method × 6 sl_values × 5 ut_sl_values × 5 om_values = 4,200 combinations`.

**Automatic filters applied:**
- `entry_time < exit_time - 5 minutes` (always)
- For PSL codes: `entry_time < last_trade_time - 5min` AND `last_trade_time < exit_time - 5min`
- For B120/B120W: if `sl=0` → force `ut_sl=0` and `method='HL'`

### Parameter Columns by Strategy

| Strategy | Columns |
|----------|---------|
| **B120** | entry_time, exit_time, orderside, method, sl, ut_sl, om |
| **SRE** | entry_time, exit_time, orderside, sl, intra_sl, om |
| **DT** | entry_time, exit_time, orderside, method, decay, sl, om |
| **NRE** | entry_time, exit_time, orderside, method, sl, re_sl, om |
| **RED** | entry_time, exit_time, orderside, method, decay, sl, om |
| **B120_PSL** | entry_time, exit_time, last_trade_time_and_interval, orderside, method, sl, ut_sl, om |
| **DT_TRAIL** | entry_time, exit_time, orderside, method, decay, sl, om, trail_profit, trail_sl |
| **B120_SI** | entry_time, exit_time, orderside, method, sl, ut_sl, om, std_indicator |
| **IRONFLY** | entry_time, exit_time, orderside, sl, om |

**Column value conventions:**
- `orderside`: `sell` or `buy` (case-insensitive, uppercased internally)
- `method`: `HL` (High/Low based SL) or `CC` (Candle Close based SL)
- `sl`, `decay`, `trail_profit`, `trail_sl`: numeric percentages
- `ut_sl`: numeric % or `TTC` (Trade-To-Close: SL at the untouched leg's entry price)
- `intra_sl`: numeric or `sl+10` format (relative to SL)
- `om`: see Strike Selection table above
- `std_indicator`: `LOW`, `HIGH`, or `AVG`

### MetaData CSV Format

Defines which indices, DTEs, and date ranges to backtest.

**Intraday MetaData:**
```csv
index,dte,from_date,to_date,start_time,end_time,run
NIFTY,1,01-03-2019,31-03-2026,09:15:00,15:29:00,TRUE
NIFTY,2,01-03-2019,31-03-2026,09:15:00,15:29:00,TRUE
SENSEX,1,01-01-2024,31-03-2026,09:15:00,15:29:00,TRUE
```

**Weekly MetaData:**
```csv
index,from_dte,to_dte,from_date,to_date,start_time,end_time,run
NIFTY,6,1,01-03-2019,31-03-2026,09:15:00,15:29:00,TRUE
```

| Column | Description |
|--------|-------------|
| index | Index name (NIFTY, BANKNIFTY, SENSEX, etc.) |
| dte | Days to expiry (intraday: exact match; weekly: not used directly) |
| from_dte / to_dte | Weekly only: DTE range for the week slice |
| from_date / to_date | Date range to backtest (DD-MM-YYYY or YYYY-MM-DD) |
| start_time / end_time | Data window (e.g. 09:15:00 to 15:29:00) |
| run | TRUE/FALSE — whether to process this row |

---

## Building a Strategy Notebook — Complete Guide

Every strategy notebook follows a **3-cell pattern**:

### Cell 1: Setup (always identical structure)

```python
code = 'MY_STRATEGY'
pickle_path = 'P:/PGC Data/PICKLE/'
parameter_path = f'Parameter_{code}.csv'
meta_data_path = f"Parameter_{code}_MetaData.csv"
output_csv_path = f'{code}_output/'

from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *

try:
    parameter, parameter_len = get_parameter_data(code, parameter_path)
    meta_data, meta_row_nos = get_meta_data(code, meta_data_path)
    os.makedirs(output_csv_path, exist_ok=True)
except Exception as e:
    input(str(e))
```

### Cell 2: Strategy Function

The strategy function receives a `bt` (IntradayBacktest) object plus all parameters for one combination, and returns a flat list of results.

**Template:**
```python
def MY_STRATEGY(bt, start_time, end_time, param1, param2, ...):
    try:
        start_dt = datetime.datetime.combine(bt.current_date, start_time)
        end_dt = datetime.datetime.combine(bt.current_date, end_time)

        # 1. Find strikes
        ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
        if ce_scrip is None: return None

        entry_time = start_dt

        # 2. Apply strategy logic (SL checks, decay, re-entry, etc.)
        # ... (see strategy patterns below)

        # 3. Return flat list: [parameters... , metadata... , results...]
        return [code, bt.index, start_time, end_time, param1, param2,
                bt.current_date.date(), bt.current_date.day_name(), bt.dte,
                entry_time.time(), future_price,
                # ... strategy-specific results
                total_pnl]
    except Exception as e:
        print(e, [bt.index, bt.current_date, ...])
        return
```

### Cell 3: Execution Loop (always identical structure)

**Intraday:**
```python
for row_idx in range(len(meta_data)):
    if row_idx in meta_row_nos and meta_data.loc[row_idx, 'run']:
        try:
            meta_row = meta_data.iloc[row_idx]
            index, dte, from_date, to_date, start_time, end_time, date_lists = get_meta_row_data(meta_row, pickle_path)

            log_cols = ('P_Param1/P_Param2/.../Date/Day/DTE/.../Total.PNL').split('/')

            for current_date in date_lists:
                file_name = f"{index} {current_date.date()} {code}"
                if not is_file_exists(output_csv_path, file_name, parameter_len):
                    print(f"Row-{row_idx} | File-{file_name} | Total-{parameter_len}")
                    bt = IntradayBacktest(pickle_path, index, current_date, dte, start_time, end_time)

                    for idx, i in enumerate(range(0, parameter_len, chunk_size), start=1):
                        chunck_file_name = f"{output_csv_path}{file_name} No-{idx}.parquet"
                        chunk_parameter = parameter.iloc[i:i+chunk_size]
                        chunk = [MY_STRATEGY(bt, row['entry_time'], row['exit_time'], row['param1'], row['param2'])
                                 for _, row in tqdm(chunk_parameter.iterrows(), total=len(chunk_parameter), colour='GREEN')]
                        save_chunk_data(chunk, log_cols, chunck_file_name)
                        del chunk, chunk_parameter
                        gc.collect()
                    del bt
                    gc.collect()
        except Exception as e:
            input(str(e))
```

**Weekly:**
```python
for row_idx in range(len(meta_data)):
    if row_idx in meta_row_nos and meta_data.loc[row_idx, 'run']:
        try:
            meta_row = meta_data.iloc[row_idx]
            index, from_dte, to_dte, from_date, to_date, start_time, end_time, week_lists = get_meta_row_data(meta_row, pickle_path, weekly=True)

            log_cols = ('P_.../Start.Date/End.Date/Start.DTE/End.DTE/DayCount/...').split('/')

            for week_dates in week_lists:
                file_name = f"{index} {week_dates[0].date()} {week_dates[-1].date()} {from_dte}-{to_dte} {code}"
                if not is_file_exists(output_csv_path, file_name, parameter_len):
                    wbt = WeeklyBacktest(pickle_path, index, week_dates, from_dte, to_dte, start_time, end_time)
                    # ... same chunk loop using wbt instead of bt ...
        except Exception as e:
            input(str(e))
```

---

## Strategy Patterns (Real Examples)

### Pattern 1: B120 — Separate Leg SL with Untouched-Leg Tracking

Sell straddle → track CE and PE SL separately → when one leg hits SL, monitor the untouched leg (UT) with its own SL.

```python
def b120(bt, start_time, end_time, orderside, method, sl, ut_sl, om):
    start_dt = datetime.datetime.combine(bt.current_date, start_time)
    end_dt = datetime.datetime.combine(bt.current_date, end_time)
    end_dt_1m = end_dt + datetime.timedelta(minutes=10)

    ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
    if ce_scrip is None: return None

    from_candle_close = True if method == 'CC' else False
    entry_time = start_dt

    # Check SL on each leg separately
    ce_open, ce_high, ce_low, ce_close, ce_sl_price, ce_sl_flag, _, _, ce_sl_time, ce_pnl = \
        bt.sl_check_single_leg(start_dt, end_dt, ce_scrip, sl=sl, with_ohlc=True, orderside=orderside, from_candle_close=from_candle_close)
    pe_open, pe_high, pe_low, pe_close, pe_sl_price, pe_sl_flag, _, _, pe_sl_time, pe_pnl = \
        bt.sl_check_single_leg(start_dt, end_dt, pe_scrip, sl=sl, with_ohlc=True, orderside=orderside, from_candle_close=from_candle_close)

    ce_sl_time = ce_sl_time if ce_sl_time else end_dt_1m
    pe_sl_time = pe_sl_time if pe_sl_time else end_dt_1m

    # Track untouched leg after first SL
    ut_sl = ut_sl if str(ut_sl) == 'TTC' else float(ut_sl)
    B_PL, TT_PL_at_SL, UT_PL_at_SL = 0, 0, 0

    if ce_sl_time < pe_sl_time:  # CE hit SL first → monitor PE
        ut_sl_price = pe_price if str(ut_sl) == 'TTC' else None
        ut_open, _, _, _, _, ut_sl_flag, _, _, ut_sl_time, ut_pnl = \
            bt.sl_check_single_leg(ce_sl_time, end_dt, pe_scrip, sl=ut_sl, sl_price=ut_sl_price,
                                   with_ohlc=True, pl_with_slipage=False, orderside=orderside, from_candle_close=from_candle_close)
        if ut_open:
            TT_PL_at_SL = ce_pnl
            UT_PL_at_SL = pe_price - ut_open - bt.Cal_slipage(pe_price)
        else:
            B_PL = ce_pnl + pe_pnl
    elif pe_sl_time < ce_sl_time:  # PE hit SL first → monitor CE
        # ... mirror logic ...
    else:
        B_PL = ce_pnl + pe_pnl

    total_pnl = B_PL + TT_PL_at_SL + UT_PL_at_SL + ut_pnl
    return [code, bt.index, ...all data..., total_pnl]
```

### Pattern 2: SRE — Straddle Re-Entry

Sell straddle → if combined SL hit → re-enter fresh straddle → repeat up to N times.

```python
def SRE(bt, start_time, end_time, orderside, sl, intra_sl, om):
    start_dt = datetime.datetime.combine(bt.current_date, start_time)
    end_dt = datetime.datetime.combine(bt.current_date, end_time)

    ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
    if ce_scrip is None: return None

    entry_time = start_dt
    # First straddle with combined SL
    std_open, _, _, _, _, _, std_sl_flag, std_intra_sl_flag, _, std_sl_time, std_pnl = \
        bt.sl_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, sl=sl, intra_sl=intra_sl, orderside=orderside, with_ohlc=True)

    first_straddle = [f"({ce_scrip}, {pe_scrip})", std_open, std_sl_flag, std_intra_sl_flag, std_sl_time, std_pnl]

    re_straddles = []
    for re_no in range(max_re):  # max_re = 7 typically
        if std_sl_time and re_no < re_entries and (std_sl_time < end_dt - datetime.timedelta(minutes=5)):
            start_dt = std_sl_time  # Re-enter at SL time
            ce_scrip, pe_scrip, ce_price, pe_price, _, start_dt = bt.get_strike(start_dt, end_dt, om=om)
            if ce_scrip is None:
                std_sl_time = ''
                re_straddles.extend(['', '', False, False, '', 0])
                continue
            std_open, _, _, _, _, _, std_sl_flag, std_intra_sl_flag, _, std_sl_time, std_pnl = \
                bt.sl_check_combine_leg(start_dt, end_dt, ce_scrip, pe_scrip, sl=sl, intra_sl=intra_sl, orderside=orderside, with_ohlc=True)
            re_straddles.extend([f"({ce_scrip}, {pe_scrip})", std_open, std_sl_flag, std_intra_sl_flag, std_sl_time, std_pnl])
        else:
            re_straddles.extend(['', '', False, False, '', 0])

    return [code, bt.index, ...] + first_straddle + re_straddles
```

### Pattern 3: DT — Decay Then Trade

Wait for premium to decay by X% → then enter with SL. Separate legs.

```python
def DT(bt, start_time, end_time, orderside, method, decay, sl, om):
    start_dt = datetime.datetime.combine(bt.current_date, start_time)
    end_dt = datetime.datetime.combine(bt.current_date, end_time)

    ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = bt.get_strike(start_dt, end_dt, om=om)
    if ce_scrip is None: return None

    from_candle_close = True if method == 'CC' else False

    # Wait for decay on each leg
    ce_open, ce_high, ce_low, ce_close, ce_decay_price, ce_decay_flag, ce_decay_time = \
        bt.decay_check_single_leg(start_dt, end_dt, ce_scrip, decay=decay, from_candle_close=from_candle_close, orderside=orderside, with_ohlc=True)
    pe_open, pe_high, pe_low, pe_close, pe_decay_price, pe_decay_flag, pe_decay_time = \
        bt.decay_check_single_leg(start_dt, end_dt, pe_scrip, decay=decay, from_candle_close=from_candle_close, orderside=orderside, with_ohlc=True)

    # Then check SL from decay time onward
    ce_sl_price, ce_sl_flag, _, _, ce_sl_time, ce_pnl = \
        bt.sl_check_single_leg(ce_decay_time, end_dt, ce_scrip,
                               o=(None if method == 'CC' else ce_decay_price),
                               sl=sl, orderside=orderside, from_candle_close=from_candle_close)
    pe_sl_price, pe_sl_flag, _, _, pe_sl_time, pe_pnl = \
        bt.sl_check_single_leg(pe_decay_time, end_dt, pe_scrip,
                               o=(None if method == 'CC' else pe_decay_price),
                               sl=sl, orderside=orderside, from_candle_close=from_candle_close)

    total_pnl = ce_pnl + pe_pnl
    return [code, bt.index, ...all data..., total_pnl]
```

### Pattern 4: NRE — N Re-Entry on Same Strike

SL hit → wait for premium to decay back to entry price → re-enter same strike → repeat N times.

```python
def NRE(bt, start_time, end_time, orderside, method, sl, re_sl, om):
    # ... find strike, initial SL check on each leg ...

    ce_re_entries = []
    for re_no in range(max_re):
        if ce_sl_time and re_no < re_entries:
            # Wait for CE premium to return to original entry price
            _, ce_decay_flag, ce_decay_time = bt.decay_check_single_leg(
                ce_sl_time, end_dt, ce_scrip, decay_price=ce_price,  # decay_price = original entry
                from_candle_close=from_candle_close, orderside=orderside)

            if ce_decay_flag:
                # Re-enter with re_sl (may differ from initial sl)
                _, ce_sl_flag, _, _, ce_sl_time, ce_pnl = bt.sl_check_single_leg(
                    ce_decay_time, end_dt, ce_scrip,
                    o=(None if method == 'CC' else ce_price),
                    sl=re_sl, orderside=orderside, from_candle_close=from_candle_close)
            # ... collect results ...
```

### Pattern 5: RED — Re-Entry on Decay (New Strike)

SL hit → find NEW strike → wait for decay → enter with SL → repeat.

```python
# After SL on CE:
ce_scrip, ce_price, _, start_dt = bt.get_strike(ce_sl_time, end_dt, om=om, only='CE')  # Get new CE strike
ce_decay_price, ce_decay_flag, ce_decay_time = bt.decay_check_single_leg(start_dt, end_dt, ce_scrip, decay=decay, ...)
if ce_decay_flag:
    ce_sl_price, ce_sl_flag, _, _, ce_sl_time, ce_pnl = bt.sl_check_single_leg(ce_decay_time, end_dt, ce_scrip, ...)
```

### Pattern 6: DT_TRAIL — Decay + Trailing SL

```python
# After decay, use trailing SL instead of fixed SL:
if trail_sl == 0 and trail_profit == 0:
    # Normal SL
    ce_sl_price, ce_sl_flag, _, ce_trail_flag, ce_sl_time, ce_pnl = bt.sl_check_single_leg(...)
else:
    # Trailing SL
    ce_sl_price, ce_sl_flag, ce_trail_flag, ce_sl_time, ce_pnl = bt.sl_check_single_leg_with_sl_trail(
        ce_decay_time, end_dt, ce_scrip, sl=sl, trail=trail_profit, sl_trail=trail_sl, ...)
```

### Pattern 7: PSL — Portfolio Stop-Loss

Multiple entries over time → combine per-minute MTM → check portfolio-level SL.

```python
def b120_per_minute_mtm(bt, start_time, end_time, orderside, method, sl, ut_sl, om):
    # ... returns per-minute MTM series for one trade ...
    # Uses per_minute_mtm=True flag on sl_check methods

def b120_PSL(bt, start_time, end_time, last_trade_time, trade_interval, ...):
    time_range = pd.date_range(start_dt, last_trade_dt, freq=trade_interval.lower()).time
    per_minute_trades = [b120_per_minute_mtm(bt, t, end_time, ...) for t in time_range]
    per_minute_mtm = np.sum(per_minute_trades, axis=0)  # Sum all trades' MTMs

    # Check at which minute total MTM crosses various % thresholds
    for mtm_percent, check_mtm in check_mtms.items():
        condition = np.where(per_minute_mtm_total > check_mtm)[0]  # or < for losses
        # ... find first crossing time ...
```

### Pattern 8: Weekly Strategy

```python
def SREW(bt, start_time, end_time, orderside, sl, intra_sl, om):
    # Use bt.current_week_dates for multi-day span
    start_dt = datetime.datetime.combine(bt.current_week_dates[0], start_time)
    end_dt = datetime.datetime.combine(bt.current_week_dates[-1], end_time)
    # ... same SRE logic but across multiple days ...
```

---

## Output Column Naming Convention

All output columns follow a consistent naming pattern:

| Prefix | Meaning |
|--------|---------|
| `P_*` | Parameter columns (input values) |
| `Date`, `Day`, `DTE` | Date metadata |
| `EntryTime`, `Future` | Entry metadata |
| `CE.*`, `PE.*` | Per-leg data |
| `STD0.*`, `STD1.*` | Straddle #0, #1, etc. (re-entry strategies) |
| `UT.*` | Untouched leg data |
| `*.Strike` | Option scrip name |
| `*.Open/High/Low/Close` | OHLC of the option |
| `*.SL.Price` | Stop-loss trigger price |
| `*.SL.Flag` | Whether SL was hit (bool) |
| `*.IntraSL` | Whether intra-candle SL was hit (bool) |
| `*.SL.Time` | Time when SL was hit |
| `*.PNL` | Profit/Loss for that leg/trade |
| `*.Decay`, `*.Decay.Flag`, `*.Decay.Time` | Decay-related columns |
| `*.Trail.Flag` | Whether trailing SL was triggered |
| `Total.PNL`, `BPL`, `TT.PL.AT.SL`, `UT.PL.AT.SL` | PNL components |

---

## File Management & Parallel Execution

### Chunking

Results are saved in chunks of `chunk_size = 100,000` parameter combinations per file.

File naming: `"{index} {date} {code} No-{chunk_idx}.parquet"`
Weekly: `"{index} {start_date} {end_date} {from_dte}-{to_dte} {code} No-{chunk_idx}.parquet"`

### Resume Support

```python
if not is_file_exists(output_csv_path, file_name, parameter_len):
    # ... process this date ...
```

`is_file_exists()` checks if all expected chunks exist. If a run is interrupted, restarting automatically skips completed dates.

### Distributed Locking (for parallel workers)

```python
with claim_date(output_csv_path, file_name, parameter_len) as claimed:
    if not claimed: continue
    # ... process date (only one worker gets here per date) ...
```

### Running with ENGINE/Run Code.py

1. Navigate to a strategy folder
2. Run `python "../../ENGINE/Run Code.py"`
3. Select notebook, parameter CSV, metadata CSV
4. Engine converts notebook → temp .py, patches paths, adds `claim_date()`
5. Launches N terminal workers (CPU auto-scaled)
6. Each worker processes different metadata rows / dates

---

## Technical Analysis Module

```python
from pgcbacktest.TechnicalAnalysis import moving_cross_over, supertrend, rsi, macd, ATR
```

All functions take a DataFrame with `datetime`, `open`, `high`, `low`, `close` columns and return the same DataFrame with added signal columns: `BUY` ('CE' or ''), `SELL` ('PE' or ''), `positions`, `signal`.

| Function | Parameters | Signals |
|----------|-----------|---------|
| `moving_cross_over(df, short_type, short_period, long_type, long_period)` | `short_type/long_type`: "NORMAL" or "EXPONENTIAL" | BUY=CE when short crosses above long |
| `supertrend(df, period=10, multiplier=3)` | ATR-based trend | BUY=CE on uptrend, SELL=PE on downtrend |
| `rsi(df, period=14, upper=70, lower=30)` | RSI oscillator | BUY=CE when RSI crosses below lower |
| `macd(df, fast=12, slow=26, signal=9)` | MACD crossover | BUY=CE on bullish crossover |
| `ATR(df, period=10)` | Returns ATR Series | Helper for SuperTrend |

Each has a corresponding `plot_*()` function for matplotlib visualization.

---

## Utility Functions

| Function | Description |
|----------|-------------|
| `cv(x)` | Converts value to string-float if numeric, else keeps as string. Used for `ut_sl` which can be "TTC" or a number. |
| `cal_percent(price, percent)` | Returns `price × percent / 100` |
| `get_strike(scrip)` | Extracts strike price from scrip name, e.g. `get_strike("21500CE")` → `21500` |
| `bt.Cal_slipage(price)` | Returns `price × slippage_rate` |
| `bt.get_one_om(future_price)` | Returns one-OTM premium target based on future price and index step |
| `bt.round_to_ticksize(value, orderside, ordertype)` | Rounds to tick size with correct ceil/floor based on order direction |
| `bt.get_gap()` | Auto-detects strike gap from option chain |
| `get_dte_file(pickle_path)` | Loads `DTE.csv` from the pickle path directory, returns a DataFrame indexed by Date with expiry-week groupings |

---

## Author

**Vikas Sharma** — [LinkedIn](https://www.linkedin.com/in/vikas-sharma-coder/)
