import os
import gc
import math
import time
import rarfile
import datetime
import requests
import tempfile
import traceback
import subprocess
import numpy as np
import pandas as pd
from tqdm import tqdm
from time import sleep
import concurrent.futures
from filelock import FileLock
from functools import lru_cache
from contextlib import contextmanager
pd.set_option('future.no_silent_downcasting', True)

NSE_INDICES = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']
BSE_INDICES = ['BANKEX', 'SENSEX']
MCX_INDICES = ['CRUDEOIL', 'CRUDEOILM', 'NATGASMINI', 'NATURALGAS', 'COPPER', 'SILVER', 'GOLD', 'SILVERM', 'GOLDM', 'ZINC']
US_INDICES = ['AAPL', 'AMD', 'AMZN', 'BABA', 'GOOGL', 'HOOD', 'INTC', 'MARA', 'META', 'MSFT', 'MSTR', 'NVDA', 'PLTR', 'QQQ_FRI', 'QQQ_MON', 'QQQ_THU', 'QQQ_TUE', 'QQQ_WED', 'SMCI', 'SOFI', 'SPXW_FRI', 'SPXW_MON', 'SPXW_THU', 'SPXW_TUE', 'SPXW_WED', 'SPY_FRI', 'SPY_MON', 'SPY_THU', 'SPY_TUE', 'SPY_WED', 'TSLA', 'UVIX', 'UVXY', 'VIX', 'VXX', 'XSP_FRI', 'XSP_MON', 'XSP_THU', 'XSP_TUE', 'XSP_WED']

class DataEmptyError(Exception):
    pass

def get_pm_time_index(dates, meta_start_time, meta_end_time):
    
    if isinstance(dates, (list, tuple, pd.Series)):
        arrays = []
        for date in dates:
            daily_index = pd.date_range(
                start=datetime.datetime.combine(date, meta_start_time),
                end=datetime.datetime.combine(date, meta_end_time),
                freq='1min'
            )
            arrays.append(daily_index.values)
        time_index = pd.DatetimeIndex(np.concatenate(arrays))
    else:
        date = dates
        time_index = pd.date_range(datetime.datetime.combine(date, meta_start_time), datetime.datetime.combine(date, meta_end_time), freq='1min')
    
    return time_index

def set_pm_time_index(data, time_index):
    n = len(data)
    if n == 0:
        return np.zeros(len(time_index), dtype=np.float64)
    padded = np.empty(n + 1, dtype=np.float64)
    padded[0] = 0.0
    padded[1:] = data.values
    idx = np.searchsorted(data.index.values, time_index.values, side='right')
    return padded[idx]


cv = lambda x: str(float(x)) if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '', 1).isdigit()) else x

def cal_percent(price, percent):
    return price * percent/100

def get_strike(scrip):
    strike = float(scrip[:-2])
    strike = int(strike) if strike.is_integer() else strike
    return strike

chunk_size = 100_000
def is_file_exists(output_csv_path, file_name, parameter_size, dir_files=None, include_rar=False):
    total_chunks = (parameter_size - 1) // chunk_size + 1

    if dir_files is None:
        dir_files = set(os.listdir(output_csv_path)) if os.path.exists(output_csv_path) else set()
        
        if include_rar:
            output_dir_name = os.path.dirname(output_csv_path)
            rar_path = f"{output_dir_name}.rar"
            lock_path = f"{rar_path}.lock"

            if os.path.exists(rar_path):
                with FileLock(lock_path):
                    while True:
                        try:
                            rar_files = set(f.filename.split("/")[-1] for f in rarfile.RarFile(rar_path).infolist() if f.filename.endswith(".parquet"))
                            dir_files = dir_files.union(rar_files)
                            break
                        except Exception as e:
                            print(f"RAR read failed ({e}), retrying in 5s...")
                            time.sleep(5)

    elif not isinstance(dir_files, set):
        dir_files = set(dir_files)

    return all(f"{file_name} No-{idx}.parquet" in dir_files for idx in range(1, total_chunks + 1))

@contextmanager
def claim_date(output_csv_path, file_name, parameter_size, dir_files=None, include_rar=False):
    """
    Usage:
        with claim_date(output_csv_path, file_name, parameter_len) as claimed:
            if not claimed: continue
            # ... process date ...
    
    Auto-releases lock on exit (normal, continue, or exception).
    """
    # Step 1: Fast skip — cache or lock exists
    if is_file_exists(output_csv_path, file_name, parameter_size, dir_files=dir_files, include_rar=include_rar):
        yield False
        return
    
    # Step 2: Try lock
    lock_path = os.path.join(output_csv_path, f"{file_name}.lock")
    try:
        lock = FileLock(lock_path, timeout=0)
        lock.acquire()
    except Exception:
        yield False
        return
    
    # Step 3: Real disk check after lock
    if is_file_exists(output_csv_path, file_name, parameter_size, include_rar=include_rar):
        lock.release()
        try: os.remove(lock_path)
        except Exception: pass
        yield False
        return
    
    # Claimed — process date
    try:
        yield True
    finally:
        lock.release()
        try: os.remove(lock_path)
        except Exception: pass

def save_chunk_data(chunk, log_cols, chunk_file_name, save_in_rar=False):
    chunk = [d for d in chunk if d is not None]
    log_data_chunk = pd.DataFrame(chunk, columns=log_cols)
    log_data_chunk.replace('', np.nan, inplace=True)
    
    dir_path = os.path.dirname(chunk_file_name)
    while True:
        try:
            if not os.path.exists(dir_path):
                raise FileNotFoundError(f"Directory {dir_path} not available")

            log_data_chunk.to_parquet(chunk_file_name, index=False)
            
            if save_in_rar:
                rar_exe = r"C:\Program Files\WinRAR\rar.exe"
                rar_file_path = f"{dir_path}.rar"
                lock_path = f"{rar_file_path}.lock"
                folder_inside_rar = os.path.basename(dir_path)

                with FileLock(lock_path):
                    subprocess.run([rar_exe, "a", "-ep", f"-ap{folder_inside_rar}", rar_file_path, chunk_file_name], capture_output=True, text=True, check=True)
                    sleep(0.3)
                
                if os.path.exists(chunk_file_name):
                    os.remove(chunk_file_name)
            return
        except Exception as e:
            print(f"Save failed ({e}), retrying in 5s...")
            time.sleep(5)

class IntradayBacktest:
    
    PREFIX = {'nifty': 'Nifty', 'banknifty': 'BN', 'finnifty': 'FN', 'midcpnifty': 'MCN', 'sensex': 'SX','bankex': 'BX'}
    SLIPAGES = {'nifty': 0.01, 'banknifty': 0.0125, 'finnifty': 0.01, 'midcpnifty': 0.0125, 'sensex': 0.0125, 'bankex': 0.0125}
    STEPS = {'nifty': 1000, 'banknifty': 5000, 'finnifty': 1000, 'midcpnifty': 1000, 'sensex': 5000,'bankex': 5000, 'spxw': 500, 'xsp': 50}
    STEPS.update({'crudeoil':500, 'crudeoilm':500, 'natgasmini':50, 'naturalgas':50})
    TICKS = {'crudeoil':0.10} # Except all 0.05
    
    ROUNDING_MAP = {
        ('SELL', 'STOPLOSS'): math.ceil,
        ('SELL', 'TARGET'): math.floor,
        ('SELL', 'DECAY'): math.floor,
        ('BUY', 'STOPLOSS'): math.floor,
        ('BUY', 'TARGET'): math.ceil,
        ('BUY', 'DECAY'): math.ceil,
    }

    token, group_id = '5156026417:AAExQbrMAPrV0qI8tSYplFDjZltLBzXTm1w', '-607631145'

    def __init__(self, pickle_path, index, current_date, dte, start_time, end_time):
        
        self.pickle_path, self.index, self.current_date, self.dte, self.meta_start_time, self.meta_end_time = pickle_path, index, current_date, dte, start_time, end_time
        self.__future_pickle_path, self.__option_pickle_path = self.get_future_option_path(index)

        future_parquet_path = self.__future_pickle_path.format(date=self.current_date.date(), extn='parquet')
        future_pickle_path = self.__future_pickle_path.format(date=self.current_date.date(), extn='pkl')

        if os.path.exists(future_parquet_path):
            self.future_data = pd.read_parquet(future_parquet_path).set_index(['date_time'])
        elif os.path.exists(future_pickle_path):
            self.future_data = pd.read_pickle(future_pickle_path).set_index(['date_time'])
        else:
            raise FileNotFoundError(f"Future data file not found for {self.index} on {self.current_date.date()}")
            
        self.future_data = self.future_data[["open", "high", "low", "close"]]
        
        option_parquet_path = self.__option_pickle_path.format(date=self.current_date.date(), extn='parquet')
        option_pickle_path = self.__option_pickle_path.format(date=self.current_date.date(), extn='pkl')
        
        if os.path.exists(option_parquet_path):
            self.options = pd.read_parquet(option_parquet_path)
        elif os.path.exists(option_pickle_path):
            self.options = pd.read_pickle(option_pickle_path)
        else:
            raise FileNotFoundError(f"Option data file not found for {self.index} on {self.current_date.date()}")
        
        self.options = self.options[["scrip", "date_time", "open", "high", "low", "close"]]
        self.options = self.options[(self.options['date_time'].dt.time >= self.meta_start_time) & (self.options['date_time'].dt.time <= self.meta_end_time)]
        self.options_data = self.options.set_index(['date_time', 'scrip'])
        self.gap = self.get_gap()
        self.tick_size = self.TICKS.get(self.index.lower(), 0.05)
        self._slipage_rate = self.SLIPAGES.get(self.index.lower(), 0.01)
        
        self._scrip_data = {}
        for scrip, group in self.options.groupby('scrip', sort=False, observed=True):
            sorted_group = group.sort_values('date_time').reset_index(drop=True)
            self._scrip_data[scrip] = {
                'df': sorted_group,
                'dt_ns': sorted_group['date_time'].values.astype('int64'),
            }
        
        # Replaces slow multi-index .loc[] lookups in get_straddle_strike/get_strangle_strike
        self._price_lookup = dict(zip(self.options_data.index, self.options_data['close']))
        
        # Pre-group options by datetime for fast strangle/ut_strike lookups
        self._options_by_dt = {}
        for dt, group in self.options.groupby('date_time', sort=False, observed=True):
            self._options_by_dt[dt] = group
            
        self.spot_data = self.get_spot_data(index, date=self.current_date)

        if self.index in NSE_INDICES:
            self.market = 'NSE'
        elif self.index in BSE_INDICES:
            self.market = 'BSE'
        elif self.index in MCX_INDICES:
            self.market = 'MCX'
        elif self.index in US_INDICES:
            self.market = 'US'
        else:
            self.market = 'OTHER'

        self.get_single_leg_data = lru_cache(maxsize=16384)(self._get_single_leg_data)
        self.get_straddle_data = lru_cache(maxsize=16384)(self._get_straddle_data)
        self.get_strike = lru_cache(maxsize=16384)(self._get_strike)
        self.sl_check_single_leg = lru_cache(maxsize=16384)(self._sl_check_single_leg)
        self.sl_check_combine_leg = lru_cache(maxsize=16384)(self._sl_check_combine_leg)
        self.decay_check_single_leg = lru_cache(maxsize=16384)(self._decay_check_single_leg)
        self.sl_check_single_leg_with_sl_trail = lru_cache(maxsize=16384)(self._sl_check_single_leg_with_sl_trail)
        self.sl_check_combine_leg_with_sl_trail = lru_cache(maxsize=16384)(self._sl_check_combine_leg_with_sl_trail)
        self.straddle_indicator = lru_cache(maxsize=16384)(self._straddle_indicator)

    def get_future_option_path(self, index):
        index_lower = index.lower()
        future_pickle_path = f'{self.pickle_path}{self.PREFIX.get(index_lower, index)} Future/{{date}}_{index_lower}_future.{{extn}}'
        option_pickle_path = f'{self.pickle_path}{self.PREFIX.get(index_lower, index)} Options/{{date}}_{index_lower}.{{extn}}'
        return future_pickle_path, option_pickle_path
    
    def get_spot_data(self, index, date=None):
        try:
            spot_pickle_path = f'{self.pickle_path}/_indices/{index}.parquet'
            spot_data = pd.read_parquet(spot_pickle_path).set_index('date_time')
            spot_data = spot_data[["open", "high", "low", "close"]]
            
            if date:
                date = pd.to_datetime(date)
                spot_data = spot_data[spot_data.index.date == date.date()]
            
        except:
            spot_data = pd.DataFrame(columns=['scrip', 'date_time', 'open', 'high', 'low', 'close', 'volume', 'openinterest']).set_index('date_time')
            
        return spot_data

    def Cal_slipage(self, price):
        return price * self._slipage_rate
    
    def send_tg_msg(self, msg):
        print(msg)
        try:
            requests.get(f'https://api.telegram.org/bot{self.token}/sendMessage?chat_id={self.group_id}&text={msg}')
        except:
            pass

    def get_gap(self):
        try:
            strike = self.options.scrip.str[:-2].astype(float).unique()
            strike.sort()
            differences = np.diff(strike)
            min_gap = float(differences.min())
            min_gap = int(min_gap) if min_gap.is_integer() else min_gap
            return min_gap
        except Exception as e:
            print(e)

    def get_one_om(self, future_price=None):

        future_price = self.future_data['close'].iloc[0] if future_price is None else future_price

        if self.index.lower() in self.STEPS:
            step = self.STEPS[self.index.lower()]
            return ((int(future_price/step)*step)/100)
        else:
            return cal_percent(round(future_price), 1)

    def round_to_ticksize(self, value, orderside, ordertype): 
        round_func = self.ROUNDING_MAP[(orderside, ordertype)]
        return round(round_func(value / self.tick_size) * self.tick_size, 2)

    def _get_single_leg_data(self, start_dt, end_dt, scrip):
        entry = self._scrip_data.get(scrip)
        if entry is None:
            return pd.DataFrame(columns=["scrip", "date_time", "open", "high", "low", "close"])
        dt_int = entry['dt_ns']
        start_i = np.searchsorted(dt_int, np.datetime64(start_dt, 'ns').view('int64'), side='left')
        end_i = np.searchsorted(dt_int, np.datetime64(end_dt, 'ns').view('int64'), side='right')
        return entry['df'].iloc[start_i:end_i]

    def _get_straddle_data(self, start_dt, end_dt, ce_scrip, pe_scrip, seperate=False):

        ce_data = self.get_single_leg_data(start_dt, end_dt, ce_scrip)
        pe_data = self.get_single_leg_data(start_dt, end_dt, pe_scrip)
        
        ce_dt = ce_data['date_time'].values
        pe_dt = pe_data['date_time'].values
        common_dt = np.intersect1d(ce_dt, pe_dt)
        
        ce_mask = np.isin(ce_dt, common_dt)
        pe_mask = np.isin(pe_dt, common_dt)
        ce_data = ce_data[ce_mask]
        pe_data = pe_data[pe_mask]
        
        if seperate:
            return ce_data, pe_data
        else:
            straddle_data = pd.DataFrame({
                'date_time': common_dt,
                'high': np.maximum(ce_data['high'].values + pe_data['low'].values, ce_data['low'].values + pe_data['high'].values),
                'low': np.minimum(ce_data['high'].values + pe_data['low'].values, ce_data['low'].values + pe_data['high'].values),
                'close': ce_data['close'].values + pe_data['close'].values,
            })
            
            return straddle_data

    def get_straddle_strike(self, start_dt, end_dt, sd=0, atm=None, SDroundoff=False):

        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                # find strike nearest to future price
                if atm and ("ATMS" in str(atm).upper()):
                    future_price = self.spot_data.loc[current_dt,'close']
                    round_future_price = round(future_price/self.gap)*self.gap
                    ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                    ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                
                else:
                    future_price = self.future_data.loc[current_dt,'close']
                    round_future_price = round(future_price/self.gap)*self.gap

                    ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                    ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                    
                    # Synthetic future
                    syn_future = ce_price - pe_price + round_future_price
                    round_syn_future = round(syn_future/self.gap)*self.gap

                    # Scrip lists
                    ce_scrip_list = [f"{round_syn_future}CE", f"{round_syn_future+self.gap}CE", f"{round_syn_future-self.gap}CE"]
                    pe_scrip_list = [f"{round_syn_future}PE", f"{round_syn_future+self.gap}PE", f"{round_syn_future-self.gap}PE"]
                    
                    scrip_index, min_value = None, float("inf")
                    for i in range(3):
                        try:
                            ce_price = self._price_lookup[(current_dt, ce_scrip_list[i])]
                            pe_price = self._price_lookup[(current_dt, pe_scrip_list[i])]
                            diff = abs(ce_price-pe_price)
                            if min_value > diff:
                                min_value = diff
                                scrip_index = i
                        except:
                            pass
                            
                    # Required scrip and their price
                    ce_scrip, pe_scrip = ce_scrip_list[scrip_index], pe_scrip_list[scrip_index]
                    ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
        
                if sd:
                    sd_range = abs((ce_price+pe_price)*sd)
                    
                    if SDroundoff:
                        sd_range = round(sd_range/self.gap)*self.gap
                    else:
                        sd_range = max(self.gap, round(sd_range/self.gap)*self.gap)

                    if sd < 0:
                        ce_scrip, pe_scrip = f"{get_strike(ce_scrip) - sd_range}CE", f"{get_strike(pe_scrip) + sd_range}PE"
                    else:
                        ce_scrip, pe_scrip = f"{get_strike(ce_scrip) + sd_range}CE", f"{get_strike(pe_scrip) - sd_range}PE"

                elif atm is not None and "ATM" in str(atm).upper():
                    
                    atm = str(atm).upper().replace(' ', '')
                    if (atm == "ATM") or (atm == "ATMS"):
                        pass
                    elif "%" in str(atm):
                        pct_val = float(atm.replace("ATMS", "").replace("ATM", "").replace("%", ""))
                        target_value = future_price * (pct_val/100.0)
                        target_value = round(target_value/self.gap) * self.gap
                        
                        ce_scrip = f"{get_strike(ce_scrip) + target_value}CE"
                        pe_scrip = f"{get_strike(pe_scrip) - target_value}PE"
                    else:
                        steps = int(atm.replace("ATMS", "").replace("ATM", ""))
                        target_value = steps*self.gap
                        
                        ce_scrip = f"{get_strike(ce_scrip) + target_value}CE"
                        pe_scrip = f"{get_strike(pe_scrip) - target_value}PE"

                ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            
            except (IndexError, KeyError, ValueError, TypeError):
                continue
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                continue

        return None, None, None, None, None, None

    def get_strangle_strike(self, start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1):

        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                future_price = self.future_data.loc[current_dt,'close']
                one_om = self.get_one_om(future_price)
                target = one_om * om if target is None else target
                dt_options = self._options_by_dt.get(current_dt)
                if dt_options is None: continue
                target_od = dt_options[dt_options['close'] >= target * tf].sort_values(by=['close'])
                
                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]
                
                ce_scrip_list = [ce_scrip, f"{get_strike(ce_scrip)-self.gap}CE", f"{get_strike(ce_scrip)+self.gap}CE"]
                pe_scrip_list = [pe_scrip, f"{get_strike(pe_scrip)-self.gap}PE", f"{get_strike(pe_scrip)+self.gap}PE"]
                        
                call_list_prices, put_list_prices = [], []
                for z in range(3):
                    try:
                        call_list_prices.append(self._price_lookup[(current_dt, ce_scrip_list[z])])
                    except:
                        call_list_prices.append(0)
                    try:
                        put_list_prices.append(self._price_lookup[(current_dt, pe_scrip_list[z])])
                    except:
                        put_list_prices.append(0)
                
                call, put, min_diff = call_list_prices[0], put_list_prices[0], float('inf')
                target_2, target_3 = target*2*tf, target*3

                diff = abs(put-call)
                required_call, required_put = None, None
                if (put+call >= target_2) & (min_diff > diff) & (put+call <= target_3):
                    min_diff = diff
                    required_call, required_put = call, put            

                for i in range(1,3):
                    if (min_diff > abs(put_list_prices[i] - call)) & (put_list_prices[i]+call >= target_2) & (put_list_prices[i]+call <= target_3):
                        min_diff = abs(put_list_prices[i] - call)
                        required_call, required_put = call, put_list_prices[i]
                    if (min_diff > abs(call_list_prices[i] - put)) & (call_list_prices[i]+put >= target_2) & (call_list_prices[i]+put <= target_3):
                        min_diff = abs(call_list_prices[i] - put)
                        required_call, required_put = call_list_prices[i], put

                ce_scrip, pe_scrip = ce_scrip_list[call_list_prices.index(required_call)], pe_scrip_list[put_list_prices.index(required_put)]
                ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                
                if get_strike(ce_scrip) < get_strike(pe_scrip) and check_inverted:
                    return self.get_straddle_strike(current_dt, end_dt)
                else:
                    return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                continue
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                continue
                                
        return None, None, None, None, None, None
    
    def get_ut_strike(self, start_dt, end_dt, om=None, target=None, above_target_only=True):

        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                future_price = self.future_data.loc[current_dt,'close']
                one_om = self.get_one_om(future_price)
                target = one_om * om if target is None else target
                dt_options = self._options_by_dt.get(current_dt)
                if dt_options is None: continue

                if above_target_only:
                    target_od = dt_options[dt_options['close'] >= target].sort_values(by=['close'])
                else:
                    target_od = dt_options[dt_options['close'] <= target].sort_values(by=['close'], ascending=False)

                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]
                
                ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]

                return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                continue
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                continue
            
        return None, None, None, None, None, None

    def _get_strike(self, start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1, only=None, obove_target_only=None, SDroundoff=False):
        
        if (obove_target_only is not None) or ("P<" in str(om).upper().replace(' ', '')) or ("P>" in str(om).upper().replace(' ', '')):
            
            if ("P<" in str(om).upper().replace(' ', '')):
                target = float(str(om).upper().replace(' ', '').replace("P<", ""))
                om = None
                obove_target_only = False
            elif ("P>" in str(om).upper().replace(' ', '')):
                target = float(str(om).upper().replace(' ', '').replace("P>", ""))
                om = None
                obove_target_only = True

            ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_ut_strike(start_dt, end_dt, om=om, target=target, above_target_only=obove_target_only)  
        else:
            if 'SD' in str(om).upper().replace(' ', ''):
                sd = float(om.upper().replace(' ', '').replace('SD', ''))
                om = None
                atm = None
            elif "ATM" in str(om).upper().replace(' ', ''):
                sd = 0
                atm = str(om).upper().replace(' ', '')
                om = None
            else:
                sd = 0
                atm = None
                om = float(om) if om else om

            if (om is None or om <= 0) and target is None:
                ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_straddle_strike(start_dt, end_dt, sd=sd, atm=atm, SDroundoff=SDroundoff)
            else:
                ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_strangle_strike(start_dt, end_dt, om=om, target=target, check_inverted=check_inverted, tf=tf)
                
        if only is None:
            return ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt
        else:
            if only == "CE":
                return ce_scrip, ce_price, future_price, start_dt
            elif only == "PE":
                return pe_scrip, pe_price, future_price, start_dt
            
    def sl_check_by_given_data(self, scrip_df, o=None, sl=0, intra_sl=0, sl_price=None, target_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0

        try:
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]

            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (h + 1)
                intra_sl_price = ((100 + intra_sl) / 100) * o if intra_sl else (h + 1)
                target_price = target_price if target_price is not None else (l - 1)

                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['high'] >= intra_sl_price
                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= sl_price_val
                mask_target = scrip_df['low'] <= target_price

            elif orderside == 'BUY':
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (l - 1)
                intra_sl_price = ((100 - intra_sl) / 100) * o if intra_sl else (l - 1)
                target_price = target_price if target_price is not None else (h + 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['low'] <= intra_sl_price
                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= sl_price_val
                mask_target = scrip_df['high'] >= target_price

            combined_mask = mask_intra_sl | mask_sl | mask_target

            if combined_mask.any():
                exit_row = scrip_df.loc[combined_mask.idxmax()]
                exit_time = exit_row['date_time']

                if orderside == 'SELL':
                    if exit_row['high'] >= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_row['close'] if from_candle_close else exit_row['high']) >= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close'] if from_candle_close else sl_price_val 
                    elif exit_row['low'] <= target_price:
                        target_flag = True
                        exit_price = target_price
                elif orderside == 'BUY':
                    if exit_row['low'] <= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_row['close'] if from_candle_close else exit_row['low']) <= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close'] if from_candle_close else sl_price_val
                    elif exit_row['high'] >= target_price:
                        target_flag = True
                        exit_price = target_price
            else:
                exit_price = c

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''
        except Exception as e:
            print('sl_check_single_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''

        sl_price = sl_price_val if (sl or sl_price) else ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)

    def _sl_check_single_leg(self, start_dt, end_dt, scrip, o=None, sl=0, intra_sl=0, sl_price=None, target_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0

        try:
            scrip_df = self.get_single_leg_data(start_dt, end_dt, scrip)
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            highs = scrip_df['high'].values
            lows = scrip_df['low'].values
            closes = scrip_df['close'].values
            datetimes = scrip_df['date_time'].values

            h, l, c = highs.max(), lows.min(), closes[-1]

            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (h + 1)
                intra_sl_price = ((100 + intra_sl) / 100) * o if intra_sl else (h + 1)
                target_price = target_price if target_price is not None else (l - 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = highs >= intra_sl_price
                mask_sl = (closes if from_candle_close else highs) >= sl_price_val
                mask_target = lows <= target_price

            elif orderside == 'BUY':
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (l - 1)
                intra_sl_price = ((100 - intra_sl) / 100) * o if intra_sl else (l - 1)
                target_price = target_price if target_price is not None else (h + 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = lows <= intra_sl_price
                mask_sl = (closes if from_candle_close else lows) <= sl_price_val
                mask_target = highs >= target_price

            combined_mask = mask_intra_sl | mask_sl | mask_target

            if combined_mask.any():
                idx = np.argmax(combined_mask)
                exit_time = pd.Timestamp(datetimes[idx])
                exit_high, exit_low, exit_close = highs[idx], lows[idx], closes[idx]

                if orderside == 'SELL':
                    if exit_high >= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_close if from_candle_close else exit_high) >= sl_price_val:
                        sl_flag = True
                        exit_price = exit_close if from_candle_close else sl_price_val 
                    elif exit_low <= target_price:
                        target_flag = True
                        exit_price = target_price
                elif orderside == 'BUY':
                    if exit_low <= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_close if from_candle_close else exit_low) <= sl_price_val:
                        sl_flag = True
                        exit_price = exit_close if from_candle_close else sl_price_val
                    elif exit_high >= target_price:
                        target_flag = True
                        exit_price = target_price
            else:
                exit_price = c

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''
        except Exception as e:
            print('sl_check_single_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''

        sl_price = sl_price_val if (sl or sl_price) else ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)

    def _sl_check_combine_leg(self, start_dt, end_dt, ce_scrip, pe_scrip, o=None, sl=0, intra_sl=0, sl_price=None, intra_sl_price=None, target_price=None, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0

        try:
            scrip_df = self.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip)
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            highs = scrip_df['high'].values
            lows = scrip_df['low'].values
            closes = scrip_df['close'].values
            datetimes = scrip_df['date_time'].values

            h, l, cl, ch, c = highs.max(), lows.min(), closes.min(), closes.max(), closes[-1]

            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (ch + 1)
                intra_sl_price_val = (((100 + intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (h + 1)
                target_price = target_price if target_price is not None else (cl - 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = highs >= intra_sl_price_val
                mask_sl = closes >= sl_price_val
                mask_target = closes <= target_price

            elif orderside == 'BUY':
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (cl - 1)
                intra_sl_price_val = (((100 - intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (l - 1)
                target_price = target_price if target_price is not None else (ch + 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = lows <= intra_sl_price_val
                mask_sl = closes <= sl_price_val
                mask_target = closes >= target_price

            combined_mask = mask_intra_sl | mask_sl | mask_target

            if combined_mask.any():
                idx = np.argmax(combined_mask)
                exit_time = pd.Timestamp(datetimes[idx])
                exit_high, exit_low, exit_close = highs[idx], lows[idx], closes[idx]

                if orderside == 'SELL':
                    if exit_high >= intra_sl_price_val:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price_val
                    elif exit_close >= sl_price_val:
                        sl_flag = True
                        exit_price = exit_close
                    elif exit_close <= target_price:
                        target_flag = True
                        exit_price = exit_close
                elif orderside == 'BUY':
                    if exit_low <= intra_sl_price_val:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price_val
                    elif exit_close <= sl_price_val:
                        sl_flag = True
                        exit_price = exit_close
                    elif exit_close >= target_price:
                        target_flag = True
                        exit_price = exit_close
            else:
                exit_price = c

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val, intra_sl_price_val = '', ''
        except Exception as e:
            print('sl_check_combine_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val, intra_sl_price_val = '', ''

        sl_price = sl_price_val if (sl or sl_price) else ''
        intra_sl_price = intra_sl_price_val if (intra_sl or intra_sl_price) else ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price, intra_sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, intra_sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
            
    def decay_check_by_given_data(self, scrip_df, decay=None, decay_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, roundtick=False):
        
        decay_flag, decay_time = False, ''
        
        try:
            if scrip_df.empty: raise DataEmptyError

            start_dt = scrip_df['date_time'].iloc[0]
            o = scrip_df['close'].iloc[0]

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError
                
            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]
            
            if decay == 0 or decay_price == -1:
                decay_price = o
                decay_flag = True
                decay_time = start_dt
            else:
                if orderside == 'SELL':
                    decay_price = ((100 - decay)/100) * o if decay_price is None else decay_price
                    
                    if roundtick or self.market == 'MCX':
                        decay_price = self.round_to_ticksize(decay_price, orderside, 'DECAY')

                    mask_decay = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= decay_price

                elif orderside == 'BUY':
                    decay_price = ((100 + decay)/100) * o if decay_price is None else decay_price
                    
                    if roundtick or self.market == 'MCX':
                        decay_price = self.round_to_ticksize(decay_price, orderside, 'DECAY')

                    mask_decay = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= decay_price

                if mask_decay.any():
                    decay_flag = True
                    decay_time = scrip_df.loc[mask_decay.idxmax(), 'date_time']

        except DataEmptyError:
            decay_flag, decay_time = False, ''
            o, h, l, c = '', '', '', ''
        except Exception as e:
            print('decay_check_single_leg', e)
            traceback.print_exc()
            decay_flag, decay_time = False, ''
            o, h, l, c = '', '', '', ''

        if with_ohlc:
            return o, h, l, c, decay_price, decay_flag, decay_time
        else:
            return decay_price, decay_flag, decay_time

    def _decay_check_single_leg(self, start_dt, end_dt, scrip, decay=None, decay_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, roundtick=False):
        
        decay_flag, decay_time = False, ''
        
        try:
            scrip_df = self.get_single_leg_data(start_dt, end_dt, scrip)
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0]

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError
            
            highs = scrip_df['high'].values
            lows = scrip_df['low'].values
            closes = scrip_df['close'].values
            datetimes = scrip_df['date_time'].values
                
            h, l, c = highs.max(), lows.min(), closes[-1]
            
            if decay == 0 or decay_price == -1:
                decay_price = o
                decay_flag = True
                decay_time = start_dt
            else:
                if orderside == 'SELL':
                    decay_price = ((100 - decay)/100) * o if decay_price is None else decay_price
                    
                    if roundtick or self.market == 'MCX':
                        decay_price = self.round_to_ticksize(decay_price, orderside, 'DECAY')

                    mask_decay = (closes if from_candle_close else lows) <= decay_price

                elif orderside == 'BUY':
                    decay_price = ((100 + decay)/100) * o if decay_price is None else decay_price
                    
                    if roundtick or self.market == 'MCX':
                        decay_price = self.round_to_ticksize(decay_price, orderside, 'DECAY')

                    mask_decay = (closes if from_candle_close else highs) >= decay_price

                if mask_decay.any():
                    decay_flag = True
                    idx = np.argmax(mask_decay)
                    decay_time = pd.Timestamp(datetimes[idx])

        except DataEmptyError:
            decay_flag, decay_time = False, ''
            o, h, l, c = '', '', '', ''
        except Exception as e:
            print('decay_check_single_leg', e)
            traceback.print_exc()
            decay_flag, decay_time = False, ''
            o, h, l, c = '', '', '', ''

        if with_ohlc:
            return o, h, l, c, decay_price, decay_flag, decay_time
        else:
            return decay_price, decay_flag, decay_time
        
    def _sl_check_single_leg_with_sl_trail(self, start_dt, end_dt, scrip, o=None, sl=0, sl_price=None, trail=0, sl_trail=0, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, trail_flag, exit_time, pnl = False, False, '', 0

        try:
            scrip_df = self.get_single_leg_data(start_dt, end_dt, scrip)
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, c = scrip_df['high'].values.max(), scrip_df['low'].values.min(), scrip_df['close'].values[-1]
            
            exit_price = None
            
            if orderside == 'SELL':
                sl_price = ((100 + sl) / 100) * o if sl_price is None else sl_price
                
                if roundtick or self.market == 'MCX':
                    sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')

                if (trail != 0) and (sl_trail != 0):
                    
                    trail_price = ((100 - trail) / 100) * o

                    if roundtick or self.market == 'MCX':
                        trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')
                        
                    for row in scrip_df.itertuples():
                        
                        if (from_candle_close and row.close >= sl_price) or (not from_candle_close and row.high >= sl_price):
                            sl_flag = True
                            exit_time = row.date_time
                            exit_price = row.close if from_candle_close else sl_price
                            break
                        elif (from_candle_close and row.close <= trail_price) or (not from_candle_close and row.low <= trail_price):
                            trail_flag = True
                                
                            sl_price = sl_price * (1 - (sl_trail/100))
                            trail_price = trail_price * (1 - (trail/100))
                            
                            if roundtick or self.market == 'MCX':
                                sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                                trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')
                            
                elif (trail == 0) and (sl_trail != 0) and (sl == sl_trail):
                    # trailing at every minute
                    
                    for row in scrip_df.itertuples():
                        
                        if (from_candle_close and row.close >= sl_price) or (not from_candle_close and row.high >= sl_price):
                            sl_flag = True
                            exit_time = row.date_time
                            exit_price = row.close if from_candle_close else sl_price
                            break
                        else:
                            sl_price = min(sl_price, ((100 + sl) / 100) * row.close) if from_candle_close else min(sl_price, ((100 + sl) / 100) * row.low)
                            if roundtick or self.market == 'MCX':
                                sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                    
            elif orderside == 'BUY':
                
                sl_price = ((100 - sl) / 100) * o if sl_price is None else sl_price
                if roundtick or self.market == 'MCX':
                    sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                
                if (trail != 0) and (sl_trail != 0):
                    
                    trail_price = ((100 + trail) / 100) * o

                    if roundtick or self.market == 'MCX':
                        trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                    for row in scrip_df.itertuples():
                        
                        if (from_candle_close and row.close <= sl_price) or (not from_candle_close and row.low <= sl_price):
                            sl_flag = True
                            exit_time = row.date_time
                            exit_price = row.close if from_candle_close else sl_price
                            break
                        elif (from_candle_close and row.close >= trail_price) or (not from_candle_close and row.high >= trail_price):
                            trail_flag = True
                                
                            sl_price = sl_price * (1 + (sl_trail/100))
                            trail_price = trail_price * (1 + (trail/100))
                            
                            if roundtick or self.market == 'MCX':
                                sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                                trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                elif (trail == 0) and (sl_trail != 0) and (sl == sl_trail):
                    # trailing at every minute

                    for row in scrip_df.itertuples():
                        
                        if (from_candle_close and row.close <= sl_price) or (not from_candle_close and row.low <= sl_price):
                            sl_flag = True
                            exit_time = row.date_time
                            exit_price = row.close if from_candle_close else sl_price
                            break
                        else:
                            sl_price = max(sl_price, ((100 - sl) / 100) * row.close) if from_candle_close else max(sl_price, ((100 - sl) / 100) * row.high)
                            if roundtick or self.market == 'MCX':
                                sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')

            exit_price = c if exit_price is None else exit_price

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, trail_flag, exit_time, pnl = False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price = ''
        except Exception as e:
            print('sl_check_single_leg_with_sl_trail', e)
            traceback.print_exc()
            sl_flag, trail_flag, exit_time, pnl = False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price = ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, trail_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, sl_flag, trail_flag, exit_time, pnl)

    def _sl_check_combine_leg_with_sl_trail(self, start_dt, end_dt, ce_scrip, pe_scrip, o=None, sl=0, intra_sl=0, sl_price=None, intra_sl_price=None, trail=0, sl_trail=0, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, trail_flag, exit_time, pnl = False, False, False, '', 0

        try:
            scrip_df = self.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip)
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, cl, ch, c = scrip_df['high'].values.max(), scrip_df['low'].values.min(), scrip_df['close'].values.min(), scrip_df['close'].values.max(), scrip_df['close'].values[-1]
            
            exit_price = None
            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (ch + 1)
                intra_sl_price_val = (((100 + intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (h + 1)

                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                
                if (trail != 0) and (sl_trail != 0):
                    
                    trail_price = ((100 - trail) / 100) * o
                    
                    if roundtick or self.market == 'MCX':
                        trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')
                    
                    for row in scrip_df.itertuples():

                        if ((sl or sl_price) and row.close >= sl_price_val) or ((intra_sl or intra_sl_price) and row.high >= intra_sl_price_val):
                            sl_flag = True
                            intra_sl_flag = True if ((intra_sl or intra_sl_price) and row.high >= intra_sl_price_val) else False
                            exit_time = row.date_time
                            exit_price = intra_sl_price_val if ((intra_sl or intra_sl_price) and row.high >= intra_sl_price_val) else row.close
                            break

                        elif ((sl or sl_price) and row.close <= trail_price) or ((intra_sl or intra_sl_price) and row.low <= trail_price):
                            trail_flag = True
                            
                            sl_price_val = sl_price_val * (1 - (sl_trail/100))
                            intra_sl_price_val = intra_sl_price_val * (1 - (sl_trail/100))
                            trail_price = trail_price * (1 - (trail/100))
                            
                            if roundtick or self.market == 'MCX':
                                sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                                intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                                trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')
                            
                elif (trail == 0) and (sl_trail != 0) and ((sl == sl_trail) or (intra_sl == sl_trail)):
                    # trailing at every minute
                    
                    for row in scrip_df.itertuples():

                        if ((sl or sl_price) and row.close >= sl_price_val) or ((intra_sl or intra_sl_price) and row.high >= intra_sl_price_val):
                            sl_flag = True
                            intra_sl_flag = True if ((intra_sl or intra_sl_price) and row.high >= intra_sl_price_val) else False
                            exit_time = row.date_time
                            exit_price = intra_sl_price_val if ((intra_sl or intra_sl_price) and row.high >= intra_sl_price_val) else row.close
                            break
                        else:
                            sl_price_val = min(sl_price_val, ((100 + sl) / 100) * row.close) if sl else (ch + 1)
                            intra_sl_price_val = min(intra_sl_price_val, ((100 + intra_sl) / 100) * row.low) if intra_sl else (h + 1)

                            if roundtick or self.market == 'MCX':
                                sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                                intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')                    
                    
            elif orderside == 'BUY':
                
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (cl - 1)
                intra_sl_price_val = (((100 - intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (l - 1)

                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                
                if (trail != 0) and (sl_trail != 0):
                    
                    trail_price = ((100 + trail) / 100) * o
                    
                    if roundtick or self.market == 'MCX':
                        trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')
                    
                    for row in scrip_df.itertuples():

                        if ((sl or sl_price) and row.close <= sl_price_val) or ((intra_sl or intra_sl_price) and row.low <= intra_sl_price_val):
                            sl_flag = True
                            intra_sl_flag = True if ((intra_sl or intra_sl_price) and row.low <= intra_sl_price_val) else False
                            exit_time = row.date_time
                            exit_price = intra_sl_price_val if ((intra_sl or intra_sl_price) and row.low <= intra_sl_price_val) else row.close
                            break

                        elif ((sl or sl_price) and row.close >= trail_price) or ((intra_sl or intra_sl_price) and row.high >= trail_price):
                            trail_flag = True
                            
                            sl_price_val = sl_price_val * (1 + (sl_trail/100))
                            intra_sl_price_val = intra_sl_price_val * (1 + (sl_trail/100))
                            trail_price = trail_price * (1 + (trail/100))
                            
                            if roundtick or self.market == 'MCX':
                                sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                                intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                                trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')
                            
                elif (trail == 0) and (sl_trail != 0) and ((sl == sl_trail) or (intra_sl == sl_trail)):
                    # trailing at every minute
                    
                    for row in scrip_df.itertuples():

                        if ((sl or sl_price) and row.close <= sl_price_val) or ((intra_sl or intra_sl_price) and row.low <= intra_sl_price_val):
                            sl_flag = True
                            intra_sl_flag = True if ((intra_sl or intra_sl_price) and row.low <= intra_sl_price_val) else False
                            exit_time = row.date_time
                            exit_price = intra_sl_price_val if ((intra_sl or intra_sl_price) and row.low <= intra_sl_price_val) else row.close
                            break
                        else:
                            sl_price_val = max(sl_price_val, ((100 - sl) / 100) * row.close) if sl else (cl - 1)
                            intra_sl_price_val = max(intra_sl_price_val, ((100 - intra_sl) / 100) * row.high) if intra_sl else (l - 1)

                            if roundtick or self.market == 'MCX':
                                sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                                intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')

            exit_price = c if exit_price is None else exit_price

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, trail_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val, intra_sl_price_val = '', ''
        except Exception as e:
            print('sl_check_combine_leg_with_sl_trail', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, trail_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val, intra_sl_price_val = '', ''

        sl_price = sl_price_val if (sl or sl_price) else ''
        intra_sl_price = intra_sl_price_val if (intra_sl or intra_sl_price) else ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price, intra_sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, intra_sl_flag, trail_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, intra_sl_price, sl_flag, intra_sl_flag, trail_flag, exit_time, pnl)

    def _straddle_indicator(self, start_dt, end_dt, si_indicator, si_buffer, buffer_min):

        buffer_start = max(datetime.datetime.combine(start_dt.date(), self.meta_start_time), start_dt - datetime.timedelta(minutes=buffer_min))
        buffer_range = pd.date_range(buffer_start, start_dt - datetime.timedelta(minutes=1), freq='1min')
        
        std_prices = [self.get_strike(dt, dt+datetime.timedelta(minutes=1))[2:4] for dt in buffer_range]
        valid_std_prices = [(ce + pe) for ce, pe in std_prices if ce is not None and pe is not None]
        
        if not valid_std_prices:
            return False, ''
        
        if si_indicator == 'LOW':
            extreme = np.min(valid_std_prices)
        elif si_indicator == 'HIGH':
            extreme = np.max(valid_std_prices)
        elif si_indicator == 'AVG':
            extreme = np.mean(valid_std_prices)

        threshold = float(extreme) * (1 + si_buffer)
        for dt in pd.date_range(start_dt, end_dt - datetime.timedelta(minutes=5), freq='1min'):
            
            _, _, ce_price, pe_price, _, entry_time = self.get_strike(dt, dt+datetime.timedelta(minutes=1))
            if entry_time is not None:
                if (ce_price + pe_price) <= threshold:
                    return True, entry_time

        return False, ''


class WeeklyBacktest(IntradayBacktest):

    def __init__(self, pickle_path, index, week_dates, from_dte, to_dte, start_time, end_time):
        
        self.pickle_path, self.index, self.week_dates, self.from_dte, self.to_dte, self.meta_start_time, self.meta_end_time = pickle_path, index, week_dates, from_dte, to_dte, start_time, end_time
        
        self.current_week_dates = sorted(set(([self.week_dates[0]] * (99 - len(self.week_dates)) + self.week_dates)[-from_dte : None if to_dte == 1 else -to_dte + 1]))
        self.__future_pickle_path, self.__option_pickle_path = self.get_future_option_path(index)
        
        future_data_list = []
        for current_date in self.current_week_dates:
            future_parquet_path = self.__future_pickle_path.format(date=current_date.date(), extn='parquet')
            future_pickle_path = self.__future_pickle_path.format(date=current_date.date(), extn='pkl')

            if os.path.exists(future_parquet_path):
                future_data_list.append(pd.read_parquet(future_parquet_path))
            elif os.path.exists(future_pickle_path):
                future_data_list.append(pd.read_pickle(future_pickle_path))

        if not future_data_list:
            raise FileNotFoundError(f"No future data files found for {self.index} for the given week.")
        
        self.future_data = pd.concat(future_data_list)
        self.future_data.sort_values(by='date_time', inplace=True)
        self.future_data.set_index('date_time', inplace=True)
        self.future_data = self.future_data[["open", "high", "low", "close"]]
        
        option_data_list = []
        for current_date in self.current_week_dates:
            option_parquet_path = self.__option_pickle_path.format(date=current_date.date(), extn='parquet')
            option_pickle_path = self.__option_pickle_path.format(date=current_date.date(), extn='pkl')

            if os.path.exists(option_parquet_path):
                option_data_list.append(pd.read_parquet(option_parquet_path))
            elif os.path.exists(option_pickle_path):
                option_data_list.append(pd.read_pickle(option_pickle_path))
        
        self.options = pd.concat(option_data_list)
        self.options = self.options[(self.options['date_time'].dt.time >= start_time) & (self.options['date_time'].dt.time <= end_time)]
        self.options = self.options[["scrip", "date_time", "open", "high", "low", "close"]]
        self.options_data = self.options.set_index(['date_time', 'scrip'])
        self.gap = self.get_gap()
        self.tick_size = self.TICKS.get(index.lower(), 0.05)
        self._slipage_rate = self.SLIPAGES.get(self.index.lower(), 0.01)

        self._scrip_data = {}
        for scrip, group in self.options.groupby('scrip', sort=False, observed=True):
            sorted_group = group.sort_values('date_time').reset_index(drop=True)
            self._scrip_data[scrip] = {
                'df': sorted_group,
                'dt_ns': sorted_group['date_time'].values.astype('int64'),
            }
        
        # Replaces slow multi-index .loc[] lookups in get_straddle_strike/get_strangle_strike
        self._price_lookup = dict(zip(self.options_data.index, self.options_data['close']))

        # Pre-group options by datetime for fast strangle/ut_strike lookups
        self._options_by_dt = {}
        for dt, group in self.options.groupby('date_time', sort=False, observed=True):
            self._options_by_dt[dt] = group

        if self.index in NSE_INDICES:
            self.market = 'NSE'
        elif self.index in BSE_INDICES:
            self.market = 'BSE'
        elif self.index in MCX_INDICES:
            self.market = 'MCX'
        elif self.index in US_INDICES:
            self.market = 'US'
        else:
            self.market = 'OTHER'

        self.get_single_leg_data = lru_cache(maxsize=16384)(self._get_single_leg_data)
        self.get_straddle_data = lru_cache(maxsize=16384)(self._get_straddle_data)
        self.get_strike = lru_cache(maxsize=16384)(self._get_strike)
        self.sl_check_single_leg = lru_cache(maxsize=16384)(self._sl_check_single_leg)
        self.sl_check_combine_leg = lru_cache(maxsize=16384)(self._sl_check_combine_leg)
        self.decay_check_single_leg = lru_cache(maxsize=16384)(self._decay_check_single_leg)
        self.sl_check_single_leg_with_sl_trail = lru_cache(maxsize=16384)(self._sl_check_single_leg_with_sl_trail)
        self.sl_check_combine_leg_with_sl_trail = lru_cache(maxsize=16384)(self._sl_check_combine_leg_with_sl_trail)
        self.straddle_indicator = lru_cache(maxsize=16384)(self._straddle_indicator)

        self.get_EOD_straddle_strike = lru_cache(maxsize=16384)(self._get_EOD_straddle_strike)
        self.sl_range_check_combine_leg = lru_cache(maxsize=16384)(self._sl_range_check_combine_leg)
        self.sl_range_trail_check_combine_leg = lru_cache(maxsize=16384)(self._sl_range_trail_check_combine_leg)

    def get_synthetic_future(self, straddle_strike, ce_price, pe_price):
        synthetic_future = straddle_strike + ce_price - pe_price
        return synthetic_future
        
    def get_sl_range(self, strike, premium, range_sl, intra_range_sl):
        range_limit = premium * (range_sl/100)
        lower_range = strike - range_limit
        upper_range = strike + range_limit
        
        if intra_range_sl:
            intra_range_limit = premium * (intra_range_sl/100)
            intra_lower_range = strike - intra_range_limit
            intra_upper_range = strike + intra_range_limit
            return lower_range, upper_range, intra_lower_range, intra_upper_range
        else:
            return lower_range, upper_range
        
    def get_straddle_strike(self, start_dt, end_dt, sd=0, atm=None, SDroundoff=False):

        current_date = start_dt.date()
        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                if atm and ("ATMS" in str(atm).upper()):
                    future_price = self.spot_data.loc[current_dt,'close']
                    round_future_price = round(future_price/self.gap)*self.gap
                    ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                    ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                    
                else:
                    future_price = self.future_data.loc[current_dt,'close']
                    round_future_price = round(future_price/self.gap)*self.gap

                    ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                    ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                    
                    syn_future = ce_price - pe_price + round_future_price
                    round_syn_future = round(syn_future/self.gap)*self.gap

                    ce_scrip_list = [f"{round_syn_future}CE", f"{round_syn_future+self.gap}CE", f"{round_syn_future-self.gap}CE"]
                    pe_scrip_list = [f"{round_syn_future}PE", f"{round_syn_future+self.gap}PE", f"{round_syn_future-self.gap}PE"]
                    
                    scrip_index, min_value = None, float("inf")
                    for i in range(3):
                        try:
                            ce_price = self._price_lookup[(current_dt, ce_scrip_list[i])]
                            pe_price = self._price_lookup[(current_dt, pe_scrip_list[i])]
                            diff = abs(ce_price-pe_price)
                            if min_value > diff:
                                min_value = diff
                                scrip_index = i
                        except:
                            pass
                            
                    ce_scrip, pe_scrip = ce_scrip_list[scrip_index], pe_scrip_list[scrip_index]
                    ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
        
                if sd:
                    sd_range = abs((ce_price+pe_price)*sd)
                    
                    if SDroundoff:
                        sd_range = round(sd_range/self.gap)*self.gap
                    else:
                        sd_range = max(self.gap, round(sd_range/self.gap)*self.gap)

                    if sd < 0:
                        ce_scrip, pe_scrip = f"{get_strike(ce_scrip) - sd_range}CE", f"{get_strike(pe_scrip) + sd_range}PE"
                    else:
                        ce_scrip, pe_scrip = f"{get_strike(ce_scrip) + sd_range}CE", f"{get_strike(pe_scrip) - sd_range}PE"

                elif atm is not None and "ATM" in str(atm).upper():
                    
                    atm = str(atm).upper().replace(' ', '')
                    if (atm == "ATM") or (atm == "ATMS"):
                        pass
                    elif "%" in str(atm):
                        pct_val = float(atm.replace("ATMS", "").replace("ATM", "").replace("%", ""))
                        target_value = future_price * (pct_val/100.0)
                        target_value = round(target_value/self.gap) * self.gap
                        
                        ce_scrip = f"{get_strike(ce_scrip) + target_value}CE"
                        pe_scrip = f"{get_strike(pe_scrip) - target_value}PE"
                    else:
                        steps = int(atm.replace("ATMS", "").replace("ATM", ""))
                        target_value = steps*self.gap
                        
                        ce_scrip = f"{get_strike(ce_scrip) + target_value}CE"
                        pe_scrip = f"{get_strike(pe_scrip) - target_value}PE"

                ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                if current_dt.date() != current_date: break
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                if current_dt.date() != current_date: break

        return None, None, None, None, None, None
    
    def _get_EOD_straddle_strike(self, current_date):
        
        check_limit = 15 #min
        start_dt = datetime.datetime.combine(current_date, self.meta_end_time)
        end_dt = start_dt - datetime.timedelta(minutes=check_limit)
        
        while start_dt > end_dt:
            try:
                future_price = self.future_data.loc[start_dt,'close']
                round_future_price = round(future_price/self.gap)*self.gap

                ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                ce_price, pe_price = self._price_lookup[(start_dt, ce_scrip)], self._price_lookup[(start_dt, pe_scrip)]
                
                syn_future = ce_price - pe_price + round_future_price
                round_syn_future = round(syn_future/self.gap)*self.gap

                ce_scrip_list = [f"{round_syn_future}CE", f"{round_syn_future+self.gap}CE", f"{round_syn_future-self.gap}CE"]
                pe_scrip_list = [f"{round_syn_future}PE", f"{round_syn_future+self.gap}PE", f"{round_syn_future-self.gap}PE"]
                
                scrip_index, min_value = None, float("inf")
                for i in range(3):
                    try:
                        ce_price = self._price_lookup[(start_dt, ce_scrip_list[i])]
                        pe_price = self._price_lookup[(start_dt, pe_scrip_list[i])]
                        diff = abs(ce_price-pe_price)
                        if min_value > diff:
                            min_value = diff
                            scrip_index = i
                    except:
                        pass
                        
                ce_scrip, pe_scrip = ce_scrip_list[scrip_index], pe_scrip_list[scrip_index]
                ce_price, pe_price = self._price_lookup[(start_dt, ce_scrip)], self._price_lookup[(start_dt, pe_scrip)]
                
                return ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt
            except (IndexError, KeyError, ValueError, TypeError):
                start_dt -= datetime.timedelta(minutes = 1)

            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                start_dt -= datetime.timedelta(minutes = 1)

        return None, None, None, None, None, None

    def get_strangle_strike(self, start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1):

        current_date = start_dt.date()
        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                future_price = self.future_data.loc[current_dt,'close']
                one_om = self.get_one_om(future_price)
                target = one_om * om if target is None else target
                dt_options = self._options_by_dt.get(current_dt)
                if dt_options is None: continue
                target_od = dt_options[dt_options['close'] >= target * tf].sort_values(by=['close'])
                
                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]
                
                ce_scrip_list = [ce_scrip, f"{get_strike(ce_scrip)-self.gap}CE", f"{get_strike(ce_scrip)+self.gap}CE"]
                pe_scrip_list = [pe_scrip, f"{get_strike(pe_scrip)-self.gap}PE", f"{get_strike(pe_scrip)+self.gap}PE"]
                        
                call_list_prices, put_list_prices = [], []
                for z in range(3):
                    try:
                        call_list_prices.append(self._price_lookup[(current_dt, ce_scrip_list[z])])
                    except:
                        call_list_prices.append(0)
                    try:
                        put_list_prices.append(self._price_lookup[(current_dt, pe_scrip_list[z])])
                    except:
                        put_list_prices.append(0)
                
                call, put, min_diff = call_list_prices[0], put_list_prices[0], float('inf')
                target_2, target_3 = target*2*tf, target*3

                diff = abs(put-call)
                required_call, required_put = None, None
                if (put+call >= target_2) & (min_diff > diff) & (put+call <= target_3):
                    min_diff = diff
                    required_call, required_put = call, put            

                for i in range(1,3):
                    if (min_diff > abs(put_list_prices[i] - call)) & (put_list_prices[i]+call >= target_2) & (put_list_prices[i]+call <= target_3):
                        min_diff = abs(put_list_prices[i] - call)
                        required_call, required_put = call, put_list_prices[i]
                    if (min_diff > abs(call_list_prices[i] - put)) & (call_list_prices[i]+put >= target_2) & (call_list_prices[i]+put <= target_3):
                        min_diff = abs(call_list_prices[i] - put)
                        required_call, required_put = call_list_prices[i], put

                ce_scrip, pe_scrip = ce_scrip_list[call_list_prices.index(required_call)], pe_scrip_list[put_list_prices.index(required_put)]
                ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]
                
                if get_strike(ce_scrip) < get_strike(pe_scrip) and check_inverted:
                    return self.get_straddle_strike(current_dt, end_dt)
                else:
                    return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                if current_dt.date() != current_date: break
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                if current_dt.date() != current_date: break
                
        return None, None, None, None, None, None
    
    def get_ut_strike(self, start_dt, end_dt, om=None, target=None, above_target_only=True):
        
        current_date = start_dt.date()
        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                future_price = self.future_data.loc[current_dt,'close']
                one_om = self.get_one_om(future_price)
                target = one_om * om if target is None else target
                dt_options = self._options_by_dt.get(current_dt)
                if dt_options is None: continue

                if above_target_only:
                    target_od = dt_options[dt_options['close'] >= target].sort_values(by=['close'])
                else:
                    target_od = dt_options[dt_options['close'] <= target].sort_values(by=['close'], ascending=False)

                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]

                ce_price, pe_price = self._price_lookup[(current_dt, ce_scrip)], self._price_lookup[(current_dt, pe_scrip)]

                return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                if current_dt.date() != current_date: break
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                if current_dt.date() != current_date: break

        return None, None, None, None, None, None

    def _get_strike(self, start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1, only=None, obove_target_only=None, SDroundoff=False):

        if (obove_target_only is not None) or ("P<" in str(om).upper().replace(' ', '')) or ("P>" in str(om).upper().replace(' ', '')):
            
            if ("P<" in str(om).upper().replace(' ', '')):
                target = float(str(om).upper().replace(' ', '').replace("P<", ""))
                om = None
                obove_target_only = False
            elif ("P>" in str(om).upper().replace(' ', '')):
                target = float(str(om).upper().replace(' ', '').replace("P>", ""))
                om = None
                obove_target_only = True

            ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_ut_strike(start_dt, end_dt, om=om, target=target, above_target_only=obove_target_only)
        else:
            if 'SD' in str(om).upper().replace(' ', ''):
                sd = float(om.upper().replace(' ', '').replace('SD', ''))
                om = None
                atm = None
            elif "ATM" in str(om).upper().replace(' ', ''):
                sd = 0
                atm = str(om).upper().replace(' ', '')
                om = None
            else:
                sd = 0
                atm = None
                om = float(om) if om else om

            if (om is None or om <= 0) and target is None:
                ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_straddle_strike(start_dt, end_dt, sd=sd, atm=atm, SDroundoff=SDroundoff)
            else:
                ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_strangle_strike(start_dt, end_dt, om=om, target=target, check_inverted=check_inverted, tf=tf)
                
        if only is None:
            return ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt
        else:
            if only == "CE":
                return ce_scrip, ce_price, future_price, start_dt
            elif only == "PE":
                return pe_scrip, pe_price, future_price, start_dt

    def sl_check_by_given_data(self, scrip_df, o=None, sl=0, intra_sl=0, sl_price=None, target_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0

        try:
            if scrip_df.empty: raise DataEmptyError
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'high'] = scrip_df['close']
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'low'] = scrip_df['close']

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]

            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (h + 1)
                intra_sl_price = ((100 + intra_sl) / 100) * o if intra_sl else (h + 1)
                target_price = target_price if target_price is not None else (l - 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['high'] >= intra_sl_price
                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= sl_price_val
                mask_target = scrip_df['low'] <= target_price

            elif orderside == 'BUY':
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (l - 1)
                intra_sl_price = ((100 - intra_sl) / 100) * o if intra_sl else (l - 1)
                target_price = target_price if target_price is not None else (h + 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['low'] <= intra_sl_price
                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= sl_price_val
                mask_target = scrip_df['high'] >= target_price

            combined_mask = mask_intra_sl | mask_sl | mask_target

            if combined_mask.any():
                exit_row = scrip_df.loc[combined_mask.idxmax()]
                exit_time = exit_row['date_time']

                if orderside == 'SELL':
                    if exit_row['high'] >= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_row['close'] if from_candle_close else exit_row['high']) >= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close'] if from_candle_close else sl_price_val 
                    elif exit_row['low'] <= target_price:
                        target_flag = True
                        exit_price = target_price
                elif orderside == 'BUY':
                    if exit_row['low'] <= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_row['close'] if from_candle_close else exit_row['low']) <= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close'] if from_candle_close else sl_price_val
                    elif exit_row['high'] >= target_price:
                        target_flag = True
                        exit_price = target_price
            else:
                exit_price = c
                
            if sl_flag and exit_time.time() == self.meta_start_time:
                exit_price = scrip_df.loc[scrip_df['date_time'] == exit_time, 'close'].iloc[0]

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''
        except Exception as e:
            print('sl_check_single_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''

        sl_price = sl_price_val if (sl or sl_price) else ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)

    def _sl_check_single_leg(self, start_dt, end_dt, scrip, o=None, sl=0, intra_sl=0, sl_price=None, target_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0

        try:
            scrip_df = self.get_single_leg_data(start_dt, end_dt, scrip).copy()
            if scrip_df.empty: raise DataEmptyError
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'high'] = scrip_df['close']
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'low'] = scrip_df['close']

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]

            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (h + 1)
                intra_sl_price = ((100 + intra_sl) / 100) * o if intra_sl else (h + 1)
                target_price = target_price if target_price is not None else (l - 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['high'] >= intra_sl_price
                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= sl_price_val
                mask_target = scrip_df['low'] <= target_price

            elif orderside == 'BUY':
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (l - 1)
                intra_sl_price = ((100 - intra_sl) / 100) * o if intra_sl else (l - 1)
                target_price = target_price if target_price is not None else (h + 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price = self.round_to_ticksize(intra_sl_price, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['low'] <= intra_sl_price
                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= sl_price_val
                mask_target = scrip_df['high'] >= target_price

            combined_mask = mask_intra_sl | mask_sl | mask_target

            if combined_mask.any():
                exit_row = scrip_df.loc[combined_mask.idxmax()]
                exit_time = exit_row['date_time']

                if orderside == 'SELL':
                    if exit_row['high'] >= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_row['close'] if from_candle_close else exit_row['high']) >= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close'] if from_candle_close else sl_price_val 
                    elif exit_row['low'] <= target_price:
                        target_flag = True
                        exit_price = target_price
                elif orderside == 'BUY':
                    if exit_row['low'] <= intra_sl_price:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price
                    elif (exit_row['close'] if from_candle_close else exit_row['low']) <= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close'] if from_candle_close else sl_price_val
                    elif exit_row['high'] >= target_price:
                        target_flag = True
                        exit_price = target_price
            else:
                exit_price = c
                
            if sl_flag and exit_time.time() == self.meta_start_time:
                exit_price = scrip_df.loc[scrip_df['date_time'] == exit_time, 'close'].iloc[0]

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''
        except Exception as e:
            print('sl_check_single_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val = ''

        sl_price = sl_price_val if (sl or sl_price) else ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)

    def _sl_check_combine_leg(self, start_dt, end_dt, ce_scrip, pe_scrip, o=None, sl=0, intra_sl=0, sl_price=None, intra_sl_price=None, target_price=None, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0

        try:
            scrip_df = self.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip).copy()
            if scrip_df.empty: raise DataEmptyError
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'high'] = scrip_df['close']
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'low'] = scrip_df['close']

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, cl, ch, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].min(), scrip_df['close'].max() , scrip_df['close'].iloc[-1]

            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (ch + 1)
                intra_sl_price_val = (((100 + intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (h + 1)
                target_price = target_price if target_price is not None else (cl - 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['high'] >= intra_sl_price_val
                mask_sl = scrip_df['close'] >= sl_price_val
                mask_target = scrip_df['close'] <= target_price

            elif orderside == 'BUY':
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (cl - 1)
                intra_sl_price_val = (((100 - intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (l - 1)
                target_price = target_price if target_price is not None else (ch + 1)
                
                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                    target_price = self.round_to_ticksize(target_price, orderside, 'TARGET')

                mask_intra_sl = scrip_df['low'] <= intra_sl_price_val
                mask_sl = scrip_df['close'] <= sl_price_val
                mask_target = scrip_df['close'] >= target_price

            combined_mask = mask_intra_sl | mask_sl | mask_target

            if combined_mask.any():
                exit_row = scrip_df.loc[combined_mask.idxmax()]
                exit_time = exit_row['date_time']

                if orderside == 'SELL':
                    if exit_row['high'] >= intra_sl_price_val:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price_val
                    elif exit_row['close'] >= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close']
                    elif exit_row['close'] <= target_price:
                        target_flag = True
                        exit_price = exit_row['close']
                elif orderside == 'BUY':
                    if exit_row['low'] <= intra_sl_price_val:
                        sl_flag, intra_sl_flag = True, True
                        exit_price = intra_sl_price_val
                    elif exit_row['close'] <= sl_price_val:
                        sl_flag = True
                        exit_price = exit_row['close']
                    elif exit_row['close'] >= target_price:
                        target_flag = True
                        exit_price = exit_row['close']
            else:
                exit_price = c

            if sl_flag and exit_time.time() == self.meta_start_time:
                exit_price = scrip_df.loc[scrip_df['date_time'] == exit_time, 'close'].iloc[0]

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val, intra_sl_price_val = '', ''
        except Exception as e:
            print('sl_check_combine_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, target_flag, exit_time, pnl = False, False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            sl_price_val, intra_sl_price_val = '', ''

        sl_price = sl_price_val if (sl or sl_price) else ''
        intra_sl_price = intra_sl_price_val if (intra_sl or intra_sl_price) else ''

        if with_ohlc:
            ohlc_data = (o, h, l, c, sl_price, intra_sl_price)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                return (*ohlc_data, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                return (sl_price, intra_sl_price, sl_flag, intra_sl_flag, target_flag, exit_time, pnl)
            
    def _sl_range_check_combine_leg(self, start_dt, end_dt, ce_scrip, pe_scrip, lower_range, upper_range, intra_lower_range, intra_upper_range, straddle_strike, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, eod_modify=False, range_sl=None, intra_range_sl=None, is_on_synthetic=False, need_day_wise_mtm=False):
        sl_flag, intra_sl_flag, exit_time, pnl = False, False, '', 0
        day_wise_mtm, day_wise_mtm2 = {}, {}

        try:
            scrip_df = self.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip)
            if scrip_df.empty: raise DataEmptyError
            
            o = scrip_df['close'].iloc[0]
            
            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, cl, ch, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].min(), scrip_df['close'].max() , scrip_df['close'].iloc[-1]
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0
            
            dstart, dstartprice = scrip_df['date_time'].iloc[0], o
            current_dt = scrip_df['date_time'].iloc[0]
            for idx in range(len(scrip_df)-1):
                
                data_row = scrip_df.iloc[idx]
                current_dt = data_row['date_time']
                current_close, current_high, current_low = data_row['close'], data_row['high'], data_row['low']

                try:
                    ce_std_data_row = self.options_data.loc[(current_dt, f"{straddle_strike}CE")]
                    pe_std_data_row = self.options_data.loc[(current_dt, f"{straddle_strike}PE")]
                    future_data_row = self.future_data.loc[current_dt]
                    
                    if is_on_synthetic:
                        future_high = straddle_strike + ce_std_data_row['high'] - pe_std_data_row['low']
                        future_low = straddle_strike + ce_std_data_row['low'] - pe_std_data_row['high']
                        future_close = straddle_strike + ce_std_data_row['close'] - pe_std_data_row['close']
                    else:
                        future_high, future_low, future_close = future_data_row['high'], future_data_row['low'], future_data_row['close']

                    if (current_dt.time() != self.meta_start_time) and (intra_upper_range <= future_high or future_low <= intra_lower_range):
                        sl_flag = True
                        intra_sl_flag = True
                        exit_time = current_dt
                        exit_price = current_high if orderside == 'SELL' else current_low
                        break
                
                    elif upper_range <= future_close or future_close <= lower_range:
                        sl_flag = True
                        exit_time = current_dt
                        exit_price = current_close
                        break
                except:
                    pass

                if eod_modify and current_dt.date() != scrip_df['date_time'].iloc[idx + 1].date():
                    try:
                        _, _, std_tce_price, std_tpe_price, _, _ = self.get_EOD_straddle_strike(current_dt.date())
                        lower_range, upper_range, intra_lower_range, intra_upper_range = self.get_sl_range(straddle_strike, std_tce_price+std_tpe_price, range_sl, intra_range_sl)
                    except:
                        pass

                if need_day_wise_mtm and current_dt.date() != scrip_df['date_time'].iloc[idx + 1].date():
                    dend, dendprice = current_dt, current_close
                    dendpnl = (dstartprice - dendprice) if (orderside == 'SELL') else (dendprice - dstartprice)
                    day_wise_mtm[dend.date()] = day_wise_mtm.get(dend.date(), 0) + dendpnl
                    day_wise_mtm2[(dstart, dend)] = dendpnl
                    dstart, dstartprice = dend, dendprice
                    
            if not sl_flag:
                exit_price = c

            if need_day_wise_mtm:
                dend = current_dt
                dendprice = exit_price if sl_flag else c
                dendpnl = (dstartprice - dendprice) if (orderside == 'SELL') else (dendprice - dstartprice)
                day_wise_mtm[dend.date()] = day_wise_mtm.get(dend.date(), 0) + dendpnl - slipage
                day_wise_mtm2[(dstart, dend)] = dendpnl - slipage

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, exit_time, pnl = False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            day_wise_mtm, day_wise_mtm2 = {}, {}
        except Exception as e:
            print('sl_check_combine_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, exit_time, pnl = False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            day_wise_mtm, day_wise_mtm2 = {}, {}

        if with_ohlc:
            ohlc_data = (o, h, l, c)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                if need_day_wise_mtm:
                    return (*ohlc_data, sl_flag, intra_sl_flag, exit_time, day_wise_mtm, day_wise_mtm2, pnl)
                else:
                    return (*ohlc_data, sl_flag, intra_sl_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                if need_day_wise_mtm:
                    return (sl_flag, intra_sl_flag, exit_time, day_wise_mtm, day_wise_mtm2, pnl)
                else:
                    return (sl_flag, intra_sl_flag, exit_time, pnl)
                
    def _sl_range_trail_check_combine_leg(self, start_dt, end_dt, ce_scrip, pe_scrip, lower_range, upper_range, intra_lower_range, intra_upper_range, straddle_strike, straddle_price, sl, intra_sl, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, eod_modify=False, range_sl=None, intra_range_sl=None, is_on_synthetic=False, need_day_wise_mtm=False):
        sl_flag, intra_sl_flag, exit_time, pnl = False, False, '', 0
        day_wise_mtm, day_wise_mtm2 = {}, {}

        try:
            scrip_df = self.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip)
            if scrip_df.empty: raise DataEmptyError
            
            o = scrip_df['close'].iloc[0]
            
            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, cl, ch, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].min(), scrip_df['close'].max() , scrip_df['close'].iloc[-1]
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0
            
            dstart, dstartprice = scrip_df['date_time'].iloc[0], o
            current_dt = scrip_df['date_time'].iloc[0]
            for idx in range(len(scrip_df)-1):
                
                data_row = scrip_df.iloc[idx]
                current_dt = data_row['date_time']
                current_close, current_high, current_low = data_row['close'], data_row['high'], data_row['low']

                try:
                    ce_std_data_row = self.options_data.loc[(current_dt, f"{straddle_strike}CE")]
                    pe_std_data_row = self.options_data.loc[(current_dt, f"{straddle_strike}PE")]
                    future_data_row = self.future_data.loc[current_dt]
                    
                    _, _, temp_std_ce_price, temp_std_pe_price, _, temp_std_current_dt = self.get_strike(current_dt, current_dt, om=0)

                    if (temp_std_current_dt == current_dt) and ((temp_std_ce_price + temp_std_pe_price) < straddle_price):
                        straddle_price = temp_std_ce_price + temp_std_pe_price
                        lower_range, upper_range, intra_lower_range, intra_upper_range = self.get_sl_range(straddle_strike, straddle_price, sl, intra_sl)
                    
                    if is_on_synthetic:
                        future_high = straddle_strike + ce_std_data_row['high'] - pe_std_data_row['low']
                        future_low = straddle_strike + ce_std_data_row['low'] - pe_std_data_row['high']
                        future_close = straddle_strike + ce_std_data_row['close'] - pe_std_data_row['close']
                    else:
                        future_high, future_low, future_close = future_data_row['high'], future_data_row['low'], future_data_row['close']

                    if (current_dt.time() != self.meta_start_time) and (intra_upper_range <= future_high or future_low <= intra_lower_range):
                        sl_flag = True
                        intra_sl_flag = True
                        exit_time = current_dt
                        exit_price = current_high if orderside == 'SELL' else current_low
                        break
                
                    elif upper_range <= future_close or future_close <= lower_range:
                        sl_flag = True
                        exit_time = current_dt
                        exit_price = current_close
                        break
                except:
                    pass

                if eod_modify and current_dt.date() != scrip_df['date_time'].iloc[idx + 1].date():
                    try:
                        _, _, std_tce_price, std_tpe_price, _, _ = self.get_EOD_straddle_strike(current_dt.date())
                        lower_range, upper_range, intra_lower_range, intra_upper_range = self.get_sl_range(straddle_strike, std_tce_price+std_tpe_price, range_sl, intra_range_sl)
                    except:
                        pass

                if need_day_wise_mtm and current_dt.date() != scrip_df['date_time'].iloc[idx + 1].date():
                    dend, dendprice = current_dt, current_close
                    dendpnl = (dstartprice - dendprice) if (orderside == 'SELL') else (dendprice - dstartprice)
                    day_wise_mtm[dend.date()] = day_wise_mtm.get(dend.date(), 0) + dendpnl
                    day_wise_mtm2[(dstart, dend)] = dendpnl
                    dstart, dstartprice = dend, dendprice
                    
            if not sl_flag:
                exit_price = c

            if need_day_wise_mtm:
                dend = current_dt
                dendprice = exit_price if sl_flag else c
                dendpnl = (dstartprice - dendprice) if (orderside == 'SELL') else (dendprice - dstartprice)
                day_wise_mtm[dend.date()] = day_wise_mtm.get(dend.date(), 0) + dendpnl - slipage
                day_wise_mtm2[(dstart, dend)] = dendpnl - slipage

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                dt_vals = scrip_df['date_time'].values
                close_vals = scrip_df['close'].values
                if exit_time:
                    mtm_mask = dt_vals <= np.datetime64(exit_time, 'ns')
                    dt_vals = dt_vals[mtm_mask]
                    close_vals = close_vals[mtm_mask]
                
                mtm_vals = (o - close_vals) if orderside == 'SELL' else (close_vals - o)
                mtm_vals = mtm_vals - slipage
                mtm_vals[-1] = pnl
                per_minute_mtm_series = pd.Series(mtm_vals, index=pd.DatetimeIndex(dt_vals))

        except DataEmptyError:
            sl_flag, intra_sl_flag, exit_time, pnl = False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            day_wise_mtm, day_wise_mtm2 = {}, {}
        except Exception as e:
            print('sl_check_combine_leg', e)
            traceback.print_exc()
            sl_flag, intra_sl_flag, exit_time, pnl = False, False, '', 0
            o, h, l, c = '', '', '', ''
            per_minute_mtm_series = pd.Series()
            day_wise_mtm, day_wise_mtm2 = {}, {}

        if with_ohlc:
            ohlc_data = (o, h, l, c)
            if per_minute_mtm:
                return (*ohlc_data, exit_time, per_minute_mtm_series)
            else:
                if need_day_wise_mtm:
                    return (*ohlc_data, sl_flag, intra_sl_flag, exit_time, day_wise_mtm, day_wise_mtm2, pnl)
                else:
                    return (*ohlc_data, sl_flag, intra_sl_flag, exit_time, pnl)
        else:
            if per_minute_mtm:
                return (exit_time, per_minute_mtm_series)
            else:
                if need_day_wise_mtm:
                    return (sl_flag, intra_sl_flag, exit_time, day_wise_mtm, day_wise_mtm2, pnl)
                else:
                    return (sl_flag, intra_sl_flag, exit_time, pnl)                


    def _decay_check_single_leg(self, start_dt, end_dt, scrip, decay=None, decay_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, roundtick=False):

        decay_flag, decay_time = False, ''
        
        try:
            scrip_df = self.get_single_leg_data(start_dt, end_dt, scrip).copy()
            if scrip_df.empty: raise DataEmptyError
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'high'] = scrip_df['close']
            scrip_df.loc[scrip_df['date_time'].dt.time == self.meta_start_time, 'low'] = scrip_df['close']

            o = scrip_df['close'].iloc[0]

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError
                
            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]
            
            if decay == 0 or decay_price == -1:
                decay_price = o
                decay_flag = True
                decay_time = start_dt
            else:
                if orderside == 'SELL':
                    decay_price = ((100 - decay)/100) * o if decay_price is None else decay_price
                    
                    if roundtick or self.market == 'MCX':
                        decay_price = self.round_to_ticksize(decay_price, orderside, 'DECAY')
                    
                    mask_decay = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= decay_price

                elif orderside == 'BUY':
                    decay_price = ((100 + decay)/100) * o if decay_price is None else decay_price
                    
                    if roundtick or self.market == 'MCX':
                        decay_price = self.round_to_ticksize(decay_price, orderside, 'DECAY')
                    
                    mask_decay = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= decay_price

                if mask_decay.any():
                    decay_flag = True
                    decay_time = scrip_df.loc[mask_decay.idxmax(), 'date_time']

        except DataEmptyError:
            decay_flag, decay_time = False, ''
            o, h, l, c = '', '', '', ''
        except Exception as e:
            print('decay_check_single_leg', e)
            traceback.print_exc()
            decay_flag, decay_time = False, ''
            o, h, l, c = '', '', '', ''

        if with_ohlc:
            return o, h, l, c, decay_price, decay_flag, decay_time
        else:
            return decay_price, decay_flag, decay_time