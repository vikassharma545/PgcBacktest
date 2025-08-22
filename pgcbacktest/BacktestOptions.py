import os
import gc
import math
import datetime
import requests
import traceback
import numpy as np
import pandas as pd
from tqdm import tqdm
from time import sleep
import concurrent.futures
from functools import lru_cache
pd.set_option('future.no_silent_downcasting', True)

NSE_INDICES = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']
BSE_INDICES = ['BANKEX', 'SENSEX']
MCX_INDICES = ['CRUDEOIL', 'CRUDEOILM', 'NATGASMINI', 'NATURALGAS']
US_INDICES = ['AAPL', 'AMD', 'AMZN', 'BABA', 'GOOGL', 'HOOD', 'INTC', 'MARA', 'META', 'MSFT', 'MSTR', 'NVDA', 'PLTR', 'QQQ_FRI', 'QQQ_MON', 'QQQ_THU', 'QQQ_TUE', 'QQQ_WED', 'SMCI', 'SOFI', 'SPXW_FRI', 'SPXW_MON', 'SPXW_THU', 'SPXW_TUE', 'SPXW_WED', 'SPY_FRI', 'SPY_MON', 'SPY_THU', 'SPY_TUE', 'SPY_WED', 'TSLA', 'UVIX', 'UVXY', 'VIX', 'VXX', 'XSP_FRI', 'XSP_MON', 'XSP_THU', 'XSP_TUE', 'XSP_WED']

class DataEmptyError(Exception):
    pass

def get_pm_time_index(date, meta_start_time, meta_end_time):
    time_index = pd.date_range(datetime.datetime.combine(date, meta_start_time), datetime.datetime.combine(date, meta_end_time), freq='1min')
    return time_index

def set_pm_time_index(data, time_index):
    if data.empty:
        return pd.Series(0, index=time_index)
    return data.reindex(index=time_index, method='ffill', fill_value=0, copy=True)

cv = lambda x: str(float(x)) if isinstance(x, (int, float)) or (isinstance(x, str) and x.replace('.', '', 1).isdigit()) else x

def cal_percent(price, percent):
    return price * percent/100

def get_strike(scrip):
    strike = float(scrip[:-2])
    strike = int(strike) if strike.is_integer() else strike
    return strike

chunk_size = 100_000
def is_file_exists(output_csv_path, file_name, parameter_size, dir_files=None, cache=False):
    
    total_chunks = (parameter_size - 1) // chunk_size + 1

    if cache:
        if not hasattr(is_file_exists, '_cached_dir_files'):
            is_file_exists._cached_dir_files = set(os.listdir(output_csv_path)) if os.path.exists(output_csv_path) else set()
            dir_files = is_file_exists._cached_dir_files
        else:
            dir_files = is_file_exists._cached_dir_files if dir_files is None else dir_files

    if dir_files is None:
        return all(os.path.exists(f"{output_csv_path}{file_name} No-{idx}.parquet") for idx in range(1, total_chunks + 1))
    else:
        return all(f"{file_name} No-{idx}.parquet" in dir_files for idx in range(1, total_chunks + 1))

def save_chunk_data(chunk, log_cols, chunck_file_name):
    chunk = [d for d in chunk if d is not None]
    log_data_chunk = pd.DataFrame(chunk, columns=log_cols)
    log_data_chunk.replace('', np.nan, inplace=True)
    log_data_chunk.to_parquet(chunck_file_name, index=False)
    
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
        self.future_data = pd.read_pickle(self.__future_pickle_path.format(date=self.current_date.date())).set_index(['date_time'])
        self.future_data = self.future_data[["open", "high", "low", "close"]]
        self.options = pd.read_pickle(self.__option_pickle_path.format(date=self.current_date.date()))
        self.options = self.options[["scrip", "date_time", "open", "high", "low", "close"]]
        self.options = self.options[(self.options['date_time'].dt.time >= self.meta_start_time) & (self.options['date_time'].dt.time <= self.meta_end_time)]
        self.options_data = self.options.set_index(['date_time', 'scrip'])
        self.gap = self.get_gap()
        self.tick_size = self.TICKS.get(self.index.lower(), 0.05)
        
        if self.index in NSE_INDICES:
            self.market = 'NSE'
        elif self.index in BSE_INDICES:
            self.market = 'BSE'
        elif self.index in MCX_INDICES:
            self.market = 'MCX'
        elif self.index in US_INDICES:
            self.market = 'US'

        self.get_single_leg_data = lru_cache(maxsize=4096)(self._get_single_leg_data)
        self.get_straddle_data = lru_cache(maxsize=4096)(self._get_straddle_data)
        self.get_strike = lru_cache(maxsize=4096)(self._get_strike)
        self.sl_check_single_leg = lru_cache(maxsize=4096)(self._sl_check_single_leg)
        self.sl_check_combine_leg = lru_cache(maxsize=4096)(self._sl_check_combine_leg)
        self.decay_check_single_leg = lru_cache(maxsize=4096)(self._decay_check_single_leg)
        self.sl_check_single_leg_with_sl_trail = lru_cache(maxsize=4096)(self._sl_check_single_leg_with_sl_trail)
        self.sl_check_combine_leg_with_sl_trail = lru_cache(maxsize=4096)(self._sl_check_combine_leg_with_sl_trail)
        self.straddle_indicator = lru_cache(maxsize=4096)(self._straddle_indicator)

    def get_future_option_path(self, index):
        index_lower = index.lower()
        future_pickle_path = f'{self.pickle_path}{self.PREFIX.get(index_lower, index)} Future/{{date}}_{index_lower}_future.pkl'
        option_pickle_path = f'{self.pickle_path}{self.PREFIX.get(index_lower, index)} Options/{{date}}_{index_lower}.pkl'
        return future_pickle_path, option_pickle_path

    def Cal_slipage(self, price):
        return price * self.SLIPAGES.get(self.index.lower(), 0.01)
    
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
        data = self.options[(self.options.scrip == scrip) & (self.options['date_time'] >= start_dt) & (self.options['date_time'] <= end_dt)].copy()
        return data.reset_index(drop=True)

    def _get_straddle_data(self, start_dt, end_dt, ce_scrip, pe_scrip, seperate=False):

        ce_data = self.get_single_leg_data(start_dt, end_dt, ce_scrip).copy()
        pe_data = self.get_single_leg_data(start_dt, end_dt, pe_scrip).copy()
        
        ce_data = ce_data[ce_data['date_time'].isin(pe_data['date_time'])]
        pe_data = pe_data[pe_data['date_time'].isin(ce_data['date_time'])]

        ce_data.sort_values(by='date_time', ignore_index=True, inplace=True)
        pe_data.sort_values(by='date_time', ignore_index=True, inplace=True)
        
        if seperate:
            return ce_data, pe_data
        else:
            straddle_data = pd.DataFrame()
            straddle_data['date_time'] = ce_data['date_time']

            ce_high, ce_low, ce_close = ce_data['high'].to_numpy(), ce_data['low'].to_numpy(), ce_data['close'].to_numpy()
            pe_high, pe_low, pe_close = pe_data['high'].to_numpy(), pe_data['low'].to_numpy(), pe_data['close'].to_numpy()
                        
            straddle_data['high'] = np.maximum(ce_high + pe_low, ce_low + pe_high)
            straddle_data['low'] = np.minimum(ce_high + pe_low, ce_low + pe_high)
            straddle_data['close'] = ce_close + pe_close
            
            return straddle_data

    def get_straddle_strike(self, start_dt, end_dt, sd=0, SDroundoff=False):

        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                # find strike nearest to future price
                future_price = self.future_data.loc[current_dt,'close']
                round_future_price = round(future_price/self.gap)*self.gap

                ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                ce_price, pe_price = self.options_data.loc[(current_dt, ce_scrip),'close'], self.options_data.loc[(current_dt, pe_scrip),'close']
                
                # Synthetic future
                syn_future = ce_price - pe_price + round_future_price
                round_syn_future = round(syn_future/self.gap)*self.gap

                # Scrip lists
                ce_scrip_list = [f"{round_syn_future}CE", f"{round_syn_future+self.gap}CE", f"{round_syn_future-self.gap}CE"]
                pe_scrip_list = [f"{round_syn_future}PE", f"{round_syn_future+self.gap}PE", f"{round_syn_future-self.gap}PE"]
                
                scrip_index, min_value = None, float("inf")
                for i in range(3):
                    try:
                        ce_price = self.options_data.loc[(current_dt,ce_scrip_list[i]),'close']
                        pe_price = self.options_data.loc[(current_dt,pe_scrip_list[i]),'close']
                        diff = abs(ce_price-pe_price)
                        if min_value > diff:
                            min_value = diff
                            scrip_index = i
                    except:
                        pass
                        
                # Required scrip and their price
                ce_scrip, pe_scrip = ce_scrip_list[scrip_index], pe_scrip_list[scrip_index]
                ce_price, pe_price = self.options_data.loc[(current_dt,ce_scrip),'close'], self.options_data.loc[(current_dt,pe_scrip),'close']
        
                if sd:
                    sd_range = (ce_price+pe_price)*sd
                    
                    if SDroundoff:
                        sd_range = round(sd_range/self.gap)*self.gap
                    else:
                        sd_range = max(self.gap, round(sd_range/self.gap)*self.gap)

                    ce_scrip, pe_scrip = f"{get_strike(ce_scrip) + sd_range}CE", f"{get_strike(pe_scrip) - sd_range}PE"
                    ce_price, pe_price = self.options_data.loc[(current_dt,ce_scrip),'close'], self.options_data.loc[(current_dt,pe_scrip),'close']
                
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
                target_od = self.options[(self.options['date_time'] == current_dt) & (self.options['close'] >= target * tf)].sort_values(by=['close']).copy()
                
                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]
                
                ce_scrip_list = [ce_scrip, f"{get_strike(ce_scrip)-self.gap}CE", f"{get_strike(ce_scrip)+self.gap}CE"]
                pe_scrip_list = [pe_scrip, f"{get_strike(pe_scrip)-self.gap}PE", f"{get_strike(pe_scrip)+self.gap}PE"]
                        
                call_list_prices, put_list_prices = [], []
                for z in range(3):
                    try:
                        call_list_prices.append(self.options_data.loc[(current_dt, ce_scrip_list[z]), 'close'])
                    except:
                        call_list_prices.append(0)
                    try:
                        put_list_prices.append(self.options_data.loc[(current_dt, pe_scrip_list[z]), 'close'])
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
                ce_price, pe_price = self.options_data.loc[(current_dt, ce_scrip), 'close'], self.options_data.loc[(current_dt, pe_scrip), 'close']
                
                if get_strike(ce_scrip) < get_strike(pe_scrip) and check_inverted:
                    return self.get_straddle_strike(current_dt)
                else:
                    return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                continue
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                continue
                                
        return None, None, None, None, None, None
    
    def get_ut_strike(self, start_dt, end_dt, om=None, target=None):

        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                future_price = self.future_data.loc[current_dt,'close']
                one_om = self.get_one_om(future_price)
                target = one_om * om if target is None else target
                target_od = self.options[(self.options['date_time'] == current_dt) & (self.options['close'] >= target)].sort_values(by=['close']).copy()
                
                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]
                
                ce_price, pe_price = self.options_data.loc[(current_dt, ce_scrip),'close'], self.options_data.loc[(current_dt, pe_scrip),'close']

                return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                continue
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                continue
            
        return None, None, None, None, None, None

    def _get_strike(self, start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1, only=None, obove_target_only=False, SDroundoff=False):
        
        if '%' in str(om) or obove_target_only:
            
            if '%' in str(om):
                om_precent = float(om.replace('%', ''))
                future_price = self.future_data['close'].iloc[0]
                one_om = self.get_one_om(future_price)
                target = one_om*om_precent/100

            ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_ut_strike(start_dt, end_dt, om=om, target=target)  
        else:
            if 'SD' in str(om).upper():
                sd = float(om.upper().replace(' ', '').replace('SD', ''))
                om = None
            else:
                sd = 0
                om = float(om) if om else om

            if (om is None or om <= 0) and target is None:
                ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_straddle_strike(start_dt, end_dt, sd=sd, SDroundoff=SDroundoff)
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
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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

            o = scrip_df['close'].iloc[0]

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError
                
            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]

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
            scrip_df = self.get_single_leg_data(start_dt, end_dt, scrip).copy()
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0]

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError
                
            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]

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
        
    def _sl_check_single_leg_with_sl_trail(self, start_dt, end_dt, scrip, trail, sl_trail, o=None, sl=0, sl_price=None, from_candle_close=False, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, trail_flag, exit_time, pnl = False, False, '', 0

        try:
            scrip_df = self.get_single_leg_data(start_dt, end_dt, scrip).copy()
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].iloc[-1]
            
            trail_limit = o * (trail / 100)
            sl_trail_limit = trail_limit * (sl_trail / 100)

            if orderside == 'SELL':
                sl_price = ((100 + sl) / 100) * o if sl_price is None else sl_price
                trail_price = o - trail_limit
                
                if roundtick or self.market == 'MCX':
                    sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                    trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= sl_price
                mask_trail = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= trail_price

            elif orderside == 'BUY':
                sl_price = ((100 - sl) / 100) * o if sl_price is None else sl_price
                trail_price = o + trail_limit            
                
                if roundtick or self.market == 'MCX':
                    sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                    trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= sl_price
                mask_trail = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= trail_price

            combined_mask = mask_sl | mask_trail

            exit_price = None
            is_sell = orderside == 'SELL'

            while combined_mask.any():

                first_row = scrip_df.loc[combined_mask.idxmax()]

                if (is_sell and first_row['high'] >= sl_price) or (not is_sell and first_row['low'] <= sl_price):
                    sl_flag = True
                    exit_time = first_row['date_time']
                    exit_price = first_row['close'] if from_candle_close else sl_price
                    break
                else:
                    trail_flag = True
                    trail_time = first_row['date_time']

                    if orderside == 'SELL':
                        sl_price = sl_price - sl_trail_limit
                        trail_price = trail_price - trail_limit
                        
                        if roundtick or self.market == 'MCX':
                            sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                            trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                        mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= sl_price
                        mask_trail = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= trail_price
                    elif orderside == 'BUY':
                        sl_price = sl_price + sl_trail_limit
                        trail_price = trail_price + trail_limit
                        
                        if roundtick or self.market == 'MCX':
                            sl_price = self.round_to_ticksize(sl_price, orderside, 'STOPLOSS')
                            trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                        mask_sl = (scrip_df['close'] if from_candle_close else scrip_df['low']) <= sl_price
                        mask_trail = (scrip_df['close'] if from_candle_close else scrip_df['high']) >= trail_price

                    mask_time = scrip_df['date_time'] >= trail_time

                    combined_mask = (mask_sl | mask_trail) & mask_time

            exit_price = c if exit_price is None else exit_price

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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

    def _sl_check_combine_leg_with_sl_trail(self, start_dt, end_dt, ce_scrip, pe_scrip, trail, sl_trail, o=None, sl=0, intra_sl=0, sl_price=None, intra_sl_price=None, orderside='SELL', from_next_minute=True, with_ohlc=False, pl_with_slipage=True, per_minute_mtm=False, roundtick=False):
        sl_flag, intra_sl_flag, trail_flag, exit_time, pnl = False, False, False, '', 0

        try:
            scrip_df = self.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip).copy()
            if scrip_df.empty: raise DataEmptyError

            o = scrip_df['close'].iloc[0] if o is None else o
            slipage = self.Cal_slipage(o) if pl_with_slipage else 0

            if from_next_minute: scrip_df = scrip_df.iloc[1:]
            if scrip_df.empty: raise DataEmptyError

            h, l, cl, ch, c = scrip_df['high'].max(), scrip_df['low'].min(), scrip_df['close'].min(), scrip_df['close'].max() , scrip_df['close'].iloc[-1]
            
            trail_limit = o * (trail / 100)
            sl_trail_limit = trail_limit * (sl_trail / 100)

            if orderside == 'SELL':
                sl_price_val = (((100 + sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (ch + 1)
                intra_sl_price_val = (((100 + intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (h + 1)
                trail_price = o - trail_limit

                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                    trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                mask_sl = scrip_df['close'] >= sl_price_val
                mask_intra_sl = scrip_df['high'] >= intra_sl_price_val
                mask_trail = scrip_df['close'] <= trail_price

            elif orderside == 'BUY':
                sl_price_val = (((100 - sl) / 100) * o if sl_price is None else sl_price) if (sl or sl_price) else (cl - 1)
                intra_sl_price_val = (((100 - intra_sl) / 100) * o if intra_sl_price is None else intra_sl_price) if (intra_sl or intra_sl_price) else (l - 1)
                trail_price = o + trail_limit

                if roundtick or self.market == 'MCX':
                    sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                    intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                    trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                mask_sl = scrip_df['close'] <= sl_price_val
                mask_intra_sl = scrip_df['low'] <= intra_sl_price_val
                mask_trail = scrip_df['close'] >= trail_price

            combined_mask = mask_intra_sl | mask_sl | mask_trail

            exit_price = None
            is_sell = orderside == 'SELL'

            while combined_mask.any():
                first_row = scrip_df.loc[combined_mask.idxmax()]

                if (is_sell and first_row['high'] >= intra_sl_price_val) or (not is_sell and first_row['low'] <= intra_sl_price_val):
                    sl_flag = True
                    intra_sl_flag = True
                    exit_time = first_row['date_time']
                    exit_price = intra_sl_price_val
                    break
                elif (is_sell and first_row['close'] >= sl_price_val) or (not is_sell and first_row['close'] <= sl_price_val):
                    sl_flag = True
                    exit_time = first_row['date_time']
                    exit_price = first_row['close']
                    break
                else:
                    trail_flag = True
                    trail_time = first_row['date_time']

                    if orderside == 'SELL':
                        sl_price_val = sl_price_val - sl_trail_limit if (sl or sl_price) else (ch + 1)
                        intra_sl_price_val = intra_sl_price_val - sl_trail_limit if (intra_sl or intra_sl_price) else (h + 1) 
                        trail_price = trail_price - trail_limit

                        if roundtick or self.market == 'MCX':
                            sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                            intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                            trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                        mask_sl = scrip_df['close'] >= sl_price_val
                        mask_intra_sl = scrip_df['high'] >= intra_sl_price_val
                        mask_trail = scrip_df['close'] <= trail_price
                    elif orderside == 'BUY':        
                        sl_price_val = sl_price_val + sl_trail_limit if (sl or sl_price) else (cl - 1)
                        intra_sl_price_val = intra_sl_price_val + sl_trail_limit if (intra_sl or intra_sl_price) else (l - 1)
                        trail_price = trail_price + trail_limit

                        if roundtick or self.market == 'MCX':
                            sl_price_val = self.round_to_ticksize(sl_price_val, orderside, 'STOPLOSS')
                            intra_sl_price_val = self.round_to_ticksize(intra_sl_price_val, orderside, 'STOPLOSS')
                            trail_price = self.round_to_ticksize(trail_price, orderside, 'TARGET')

                        mask_sl = scrip_df['close'] <= sl_price_val
                        mask_intra_sl = scrip_df['low'] <= intra_sl_price_val
                        mask_trail = scrip_df['close'] >= trail_price

                    mask_time = scrip_df['date_time'] >= trail_time

                    combined_mask = (mask_intra_sl | mask_sl | mask_trail) & mask_time

            exit_price = c if exit_price is None else exit_price

            pnl = (exit_price - o) if orderside == 'BUY' else (o - exit_price)
            pnl = round(pnl - slipage, 2)

            if per_minute_mtm:
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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
    
    def __del__(self) -> None:
        print("Deleting instance ...", self.current_date)


class WeeklyBacktest(IntradayBacktest):

    def __init__(self, pickle_path, index, week_dates, from_dte, to_dte, start_time, end_time):
        
        self.pickle_path, self.index, self.week_dates, self.from_dte, self.to_dte, self.meta_start_time, self.meta_end_time = pickle_path, index, week_dates, from_dte, to_dte, start_time, end_time
        
        self.current_week_dates = sorted(set(([self.week_dates[0]] * (7 - len(self.week_dates)) + self.week_dates)[-from_dte : None if to_dte == 1 else -to_dte + 1]))
        self.__future_pickle_path, self.__option_pickle_path = self.get_future_option_path(index)
        self.future_data = pd.concat([pd.read_pickle(self.__future_pickle_path.format(date=current_date.date())) for current_date in self.current_week_dates])
        self.future_data.sort_values(by='date_time', inplace=True)
        self.future_data.set_index('date_time', inplace=True)
        self.future_data = self.future_data[["open", "high", "low", "close"]]
        self.options = pd.concat([pd.read_pickle(self.__option_pickle_path.format(date=current_date.date())) for current_date in self.current_week_dates])
        self.options = self.options[(self.options['date_time'].dt.time >= start_time) & (self.options['date_time'].dt.time <= end_time)]
        self.options = self.options[["scrip", "date_time", "open", "high", "low", "close"]]
        self.options_data = self.options.set_index(['date_time', 'scrip'])
        self.gap = self.get_gap()
        self.tick_size = self.TICKS.get(index.lower(), 0.05)
        
        if self.index in NSE_INDICES:
            self.market = 'NSE'
        elif self.index in BSE_INDICES:
            self.market = 'BSE'
        elif self.index in MCX_INDICES:
            self.market = 'MCX'
        elif self.index in US_INDICES:
            self.market = 'US'

        self.get_single_leg_data = lru_cache(maxsize=4096)(self._get_single_leg_data)
        self.get_straddle_data = lru_cache(maxsize=4096)(self._get_straddle_data)
        self.get_strike = lru_cache(maxsize=4096)(self._get_strike)
        self.sl_check_single_leg = lru_cache(maxsize=4096)(self._sl_check_single_leg)
        self.sl_check_combine_leg = lru_cache(maxsize=4096)(self._sl_check_combine_leg)
        self.decay_check_single_leg = lru_cache(maxsize=4096)(self._decay_check_single_leg)
        self.sl_check_single_leg_with_sl_trail = lru_cache(maxsize=4096)(self._sl_check_single_leg_with_sl_trail)
        self.sl_check_combine_leg_with_sl_trail = lru_cache(maxsize=4096)(self._sl_check_combine_leg_with_sl_trail)
        self.straddle_indicator = lru_cache(maxsize=4096)(self._straddle_indicator)

        self.get_EOD_straddle_strike = lru_cache(maxsize=4096)(self._get_EOD_straddle_strike)
        self.sl_range_check_combine_leg = lru_cache(maxsize=4096)(self._sl_range_check_combine_leg)

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
        
    def get_straddle_strike(self, start_dt, end_dt, sd=0, SDroundoff=False):

        current_date = start_dt.date()
        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                # find strike nearest to future price
                future_price = self.future_data.loc[current_dt,'close']
                round_future_price = round(future_price/self.gap)*self.gap

                ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                ce_price, pe_price = self.options_data.loc[(current_dt, ce_scrip),'close'], self.options_data.loc[(current_dt, pe_scrip),'close']
                
                # Synthetic future
                syn_future = ce_price - pe_price + round_future_price
                round_syn_future = round(syn_future/self.gap)*self.gap

                # Scrip lists
                ce_scrip_list = [f"{round_syn_future}CE", f"{round_syn_future+self.gap}CE", f"{round_syn_future-self.gap}CE"]
                pe_scrip_list = [f"{round_syn_future}PE", f"{round_syn_future+self.gap}PE", f"{round_syn_future-self.gap}PE"]
                
                scrip_index, min_value = None, float("inf")
                for i in range(3):
                    try:
                        ce_price = self.options_data.loc[(current_dt,ce_scrip_list[i]),'close']
                        pe_price = self.options_data.loc[(current_dt,pe_scrip_list[i]),'close']
                        diff = abs(ce_price-pe_price)
                        if min_value > diff:
                            min_value = diff
                            scrip_index = i
                    except:
                        pass
                        
                # Required scrip and their price
                ce_scrip, pe_scrip = ce_scrip_list[scrip_index], pe_scrip_list[scrip_index]
                ce_price, pe_price = self.options_data.loc[(current_dt,ce_scrip),'close'], self.options_data.loc[(current_dt,pe_scrip),'close']
        
                if sd:
                    sd_range = (ce_price+pe_price)*sd
                    
                    if SDroundoff:
                        sd_range = round(sd_range/self.gap)*self.gap
                    else:
                        sd_range = max(self.gap, round(sd_range/self.gap)*self.gap)

                    ce_scrip, pe_scrip = f"{get_strike(ce_scrip) + sd_range}CE", f"{get_strike(pe_scrip) - sd_range}PE"
                    ce_price, pe_price = self.options_data.loc[(current_dt,ce_scrip),'close'], self.options_data.loc[(current_dt,pe_scrip),'close']
                
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
                # find strike nearest to future price
                future_price = self.future_data.loc[start_dt,'close']
                round_future_price = round(future_price/self.gap)*self.gap

                ce_scrip, pe_scrip = f"{round_future_price}CE", f"{round_future_price}PE"
                ce_price, pe_price = self.options_data.loc[(start_dt, ce_scrip),'close'], self.options_data.loc[(start_dt, pe_scrip),'close']
                
                # Synthetic future
                syn_future = ce_price - pe_price + round_future_price
                round_syn_future = round(syn_future/self.gap)*self.gap

                # Scrip lists
                ce_scrip_list = [f"{round_syn_future}CE", f"{round_syn_future+self.gap}CE", f"{round_syn_future-self.gap}CE"]
                pe_scrip_list = [f"{round_syn_future}PE", f"{round_syn_future+self.gap}PE", f"{round_syn_future-self.gap}PE"]
                
                scrip_index, min_value = None, float("inf")
                for i in range(3):
                    try:
                        ce_price = self.options_data.loc[(start_dt,ce_scrip_list[i]),'close']
                        pe_price = self.options_data.loc[(start_dt,pe_scrip_list[i]),'close']
                        diff = abs(ce_price-pe_price)
                        if min_value > diff:
                            min_value = diff
                            scrip_index = i
                    except:
                        pass
                        
                # Required scrip and their price
                ce_scrip, pe_scrip = ce_scrip_list[scrip_index], pe_scrip_list[scrip_index]
                ce_price, pe_price = self.options_data.loc[(start_dt,ce_scrip),'close'], self.options_data.loc[(start_dt,pe_scrip),'close']
                
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
                target_od = self.options[(self.options['date_time'] == current_dt) & (self.options['close'] >= target * tf)].sort_values(by=['close']).copy()
                
                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]
                
                ce_scrip_list = [ce_scrip, f"{get_strike(ce_scrip)-self.gap}CE", f"{get_strike(ce_scrip)+self.gap}CE"]
                pe_scrip_list = [pe_scrip, f"{get_strike(pe_scrip)-self.gap}PE", f"{get_strike(pe_scrip)+self.gap}PE"]
                        
                call_list_prices, put_list_prices = [], []
                for z in range(3):
                    try:
                        call_list_prices.append(self.options_data.loc[(current_dt, ce_scrip_list[z]), 'close'])
                    except:
                        call_list_prices.append(0)
                    try:
                        put_list_prices.append(self.options_data.loc[(current_dt, pe_scrip_list[z]), 'close'])
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
                ce_price, pe_price = self.options_data.loc[(current_dt, ce_scrip), 'close'], self.options_data.loc[(current_dt, pe_scrip), 'close']
                
                if get_strike(ce_scrip) < get_strike(pe_scrip) and check_inverted:
                    return self.get_straddle_strike(current_dt)
                else:
                    return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                if current_dt.date() != current_date: break
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                if current_dt.date() != current_date: break
                
        return None, None, None, None, None, None
    
    def get_ut_strike(self, start_dt, end_dt, om=None, target=None):
        
        current_date = start_dt.date()
        valid_times = self.future_data.loc[start_dt:end_dt].index
        for current_dt in valid_times:
            try:
                future_price = self.future_data.loc[current_dt,'close']
                one_om = self.get_one_om(future_price)
                target = one_om * om if target is None else target
                target_od = self.options[(self.options['date_time'] == current_dt) & (self.options['close'] >= target)].sort_values(by=['close']).copy()
                
                ce_scrip = target_od.loc[target_od['scrip'].str.endswith('CE'), 'scrip'].iloc[0]
                pe_scrip = target_od.loc[target_od['scrip'].str.endswith('PE'), 'scrip'].iloc[0]
                
                ce_price, pe_price = self.options_data.loc[(current_dt, ce_scrip),'close'], self.options_data.loc[(current_dt, pe_scrip),'close']

                return ce_scrip, pe_scrip, ce_price, pe_price, future_price, current_dt
            except (IndexError, KeyError, ValueError, TypeError):
                if current_dt.date() != current_date: break
            except Exception as e:
                print('get_straddle_strike', e)
                traceback.print_exc()
                if current_dt.date() != current_date: break

    def _get_strike(self, start_dt, end_dt, om=None, target=None, check_inverted=False, tf=1, only=None, obove_target_only=False, SDroundoff=False):
        
        if '%' in str(om) or obove_target_only:
            
            if '%' in str(om):
                om_precent = float(om.replace('%', ''))
                future_price = self.future_data['close'].iloc[0]
                one_om = self.get_one_om(future_price)
                target = one_om*om_precent/100

            ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_ut_strike(start_dt, end_dt, om=om, target=target)  
        else:
            if 'SD' in str(om).upper():
                sd = float(om.upper().replace(' ', '').replace('SD', ''))
                om = None
            else:
                sd = 0
                om = float(om) if om else om

            if (om is None or om <= 0) and target is None:
                ce_scrip, pe_scrip, ce_price, pe_price, future_price, start_dt = self.get_straddle_strike(start_dt, end_dt, sd=sd, SDroundoff=SDroundoff)
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
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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
            scrip_df = self.get_straddle_data(start_dt, end_dt, ce_scrip, pe_scrip).copy()
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
                
                scrip_df.set_index('date_time', inplace=True)
                if exit_time:
                    scrip_df = scrip_df.loc[scrip_df.index <= exit_time]

                per_minute_mtm_series = o - scrip_df['close'] if orderside == 'SELL' else scrip_df['close'] - o
                per_minute_mtm_series = per_minute_mtm_series - slipage
                per_minute_mtm_series.iloc[-1] = pnl

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
        
    def __del__(self) -> None:
        print("Deleting instance ...")


class MonthlyBacktest(WeeklyBacktest):

    def __init__(self, pickle_path, index, month_dates, from_dte, to_dte, start_time, end_time):
        
        self.pickle_path, self.index, self.month_dates, self.from_dte, self.to_dte = pickle_path, index, month_dates, from_dte, to_dte
        
        self.current_month_dates = sorted(set(([self.month_dates[0]] * (31 - len(self.month_dates)) + self.month_dates)[-from_dte : None if to_dte == 1 else -to_dte + 1]))
        self.__future_pickle_path, self.__option_pickle_path = self.get_future_option_path(index)
        self.future_data = pd.concat([pd.read_pickle(self.__future_pickle_path.format(date=current_date.date())) for current_date in self.current_month_dates])
        self.future_data.sort_values(by='date_time', inplace=True)
        self.future_data.set_index('date_time', inplace=True)
        
        self.options = pd.concat([pd.read_pickle(self.__option_pickle_path.format(date=current_date.date())) for current_date in self.current_month_dates])
        self.options = self.options[(self.options['date_time'].dt.time >= start_time) & (self.options['date_time'].dt.time <= end_time)]
        self.options_data = self.options.set_index(['date_time', 'scrip'])
        self.gap = self.get_gap()

