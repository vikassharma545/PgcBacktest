import itertools
import pandas as pd

def get_parameter_data(code, parameter_path):
    
    parameter = pd.read_csv(parameter_path)
    for col in parameter.columns:
        if 'time' in col:
            globals()[f'{col}'] = pd.to_datetime(parameter[col].dropna().str.replace(' ', '').str[0:5], format='%H:%M').dt.time.to_list()
        else:
            globals()[f'{col}'] = parameter[col].dropna().to_list()
            
    parameter = pd.DataFrame(list(itertools.product(*[globals()[f'{col}'] for col in parameter.columns])), columns=parameter.columns)
    
    # filter - entry < (exit_time - 5min)
    parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]


    if (code == 'B120') or (code == 'B120_TTC_RE') or (code == 'B120W'):
    
        parameter.loc[parameter['sl'] == 0, 'ut_sl'] = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['ut_sl'] = parameter['ut_sl'].apply(lambda x: str(x).upper() if x == 'TTC' else str(x).upper())
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
        

    elif code == 'B120_PSL':
        
        # filter - entry < (exit_time|endtime - 5min)
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'ut_sl'] = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['ut_sl'] = parameter['ut_sl'].apply(lambda x: str(x).upper() if x == 'TTC' else str(x).upper())
        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()


    if code == 'B120_UNIVERSAL':

        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'] .split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] <= parameter['sl']))]
        
        parameter['ut_intra_sl'] = parameter.apply(lambda row: row['ut_sl'] + float(row['ut_intra_sl'] .split('+')[-1]) if '+' in str(row['ut_intra_sl']) else float(row['ut_intra_sl']), axis=1)
        parameter = parameter[~((parameter['ut_intra_sl'] != 0) & (parameter['ut_intra_sl'] <= parameter['ut_sl']))]

        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, ['intra_sl', 'ut_sl', 'ut_intra_sl']] = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        parameter.loc[parameter['ut_sl'] == 0, 'ut_intra_sl'] = 0
        
        #filter where method HL and Intra SL
        parameter.loc[parameter['method'] == 'HL', 'intra_sl'] = 0
        parameter.loc[parameter['method'] == 'HL', 'ut_intra_sl'] = 0
        
        parameter['ut_sl'] = parameter['ut_sl'].apply(lambda x: str(x).upper() if x == 'TTC' else str(x).upper())
        parameter.loc[parameter['ut_sl'] == 'TTC', 'ut_intra_sl'] = 0
        
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
        
    
    elif code == 'B120_RE':
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'ut_sl'] = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['ut_sl'] = parameter['ut_sl'].apply(lambda x: str(x).upper() if x == 'TTC' else float(str(x).upper()))
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
        
        
    elif code == 'B120_RE_PSL':
            
        # filter - entry < (exit_time|endtime - 5min)
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'ut_sl'] = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['ut_sl'] = parameter['ut_sl'].apply(lambda x: str(x).upper() if x == 'TTC' else str(x).upper())
        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()


    elif code == 'DT':
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
            
        
    elif code == 'DT_PSL':
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()
        
            
    elif code == 'NRE':
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        parameter.loc[parameter['sl'] == 0, 're_sl'] = 0
        
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()


    elif code == 'NRE_PSL':
        
        # filter - entry < (exit_time - 5min)
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()


    elif code == 'RED':
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()


    elif code == 'RED_PSL':
        
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[parameter['sl'] == 0, 'method'] = 'HL'
        
        parameter.loc[parameter['re_entries'] == 0, 'decay'] = parameter['decay'].min()
        
        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['method'] = parameter['method'].str.upper()


    elif code == 'SBS':
        # filter - where sl = 0
        parameter.loc[(parameter['sell_sl'] == 0), 'method'] = 'HL'
        
        parameter.loc[(parameter['sell_sl'] == 0) & (parameter['sell_trail'] == 0), 'buy_sl'] = 0
        parameter.loc[(parameter['sell_sl'] == 0) & (parameter['sell_trail'] == 0), 'buy_trail'] = 0
        parameter.loc[(parameter['sell_sl'] == 0) & (parameter['sell_trail'] == 0), 'buy_sl_trail'] = 0
        
        parameter.loc[parameter['sell_sl'] == 0, 'sell_sl_trail'] = 0
        parameter.loc[parameter['buy_sl'] == 0, 'buy_sl_trail'] = 0
        
        # filter - where trail = 0
        parameter.loc[parameter['sell_trail'] == 0, 'sell_sl_trail'] = 0
        parameter.loc[parameter['buy_trail'] == 0, 'buy_sl_trail'] = 0
        parameter.loc[parameter['buy_flag'] == False, ['buy_sl','buy_sl_trail','buy_trail']] = 0
        parameter.loc[parameter['buy_flag'] == False, ['sell2_flag']] = False

        parameter['method'] = parameter['method'].str.upper()
    
    
    elif code == 'SBS_PSL':
        
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        # filter - where sl = 0
        parameter.loc[(parameter['sell_sl'] == 0), 'method'] = 'HL'
        
        parameter.loc[(parameter['sell_sl'] == 0) & (parameter['sell_trail'] == 0), 'buy_sl'] = 0
        parameter.loc[(parameter['sell_sl'] == 0) & (parameter['sell_trail'] == 0), 'buy_trail'] = 0
        parameter.loc[(parameter['sell_sl'] == 0) & (parameter['sell_trail'] == 0), 'buy_sl_trail'] = 0
        
        parameter.loc[parameter['sell_sl'] == 0, 'sell_sl_trail'] = 0
        parameter.loc[parameter['buy_sl'] == 0, 'buy_sl_trail'] = 0
        
        # filter - where trail = 0
        parameter.loc[parameter['sell_trail'] == 0, 'sell_sl_trail'] = 0
        parameter.loc[parameter['buy_trail'] == 0, 'buy_sl_trail'] = 0
        parameter.loc[parameter['buy_flag'] == False, ['buy_sl','buy_sl_trail','buy_trail']] = 0
        parameter.loc[parameter['buy_flag'] == False, ['sell2_flag']] = False

        parameter['method'] = parameter['method'].str.upper()


    elif code == 'SRE':

        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'].split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] <= parameter['sl']))]

        parameter['orderside'] = parameter['orderside'].str.upper()
    
    
    elif code == 'SRE_PSL':
        
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'].split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] < parameter['sl']))]

        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
            
    
    elif code == 'SUT':

        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'].split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] < parameter['sl']))]

        # filter - where sl = 0 & intra_sl = 0
        parameter.loc[(parameter['sl'] == 0) & (parameter['intra_sl'] == 0), 'ut_sl'] = 0

        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['ut_orderside'] = parameter['orderside'].str.upper()
        parameter['ut_method'] = parameter['ut_method'].str.upper()
    
    
    elif code == 'SUT_PSL':
        
        parameter = parameter[pd.to_datetime(parameter['entry_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        parameter = parameter[pd.to_datetime(parameter['last_trade_time'], format='%H:%M:%S').dt.time < (pd.to_datetime(parameter['exit_time'], format='%H:%M:%S')-pd.Timedelta(minutes=5)).dt.time]
        
        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'].split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] < parameter['sl']))]

        # filter - where sl = 0 & intra_sl = 0
        parameter.loc[(parameter['sl'] == 0) & (parameter['intra_sl'] == 0), 'ut_sl'] = 0

        parameter['trade_interval'] = parameter['trade_interval'].str.upper()
        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['ut_orderside'] = parameter['orderside'].str.upper()
        parameter['ut_method'] = parameter['ut_method'].str.upper()
        
        
    elif code == 'SUT_SRE':
        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'].split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] < parameter['sl']))]

        parameter['sre_intra_sl'] = parameter.apply(lambda row: row['sre_sl'] + float(row['sre_intra_sl'] .split('+')[-1]) if '+' in str(row['sre_intra_sl']) else float(row['sre_intra_sl']), axis=1)
        parameter = parameter[~((parameter['sre_intra_sl'] != 0) & (parameter['sre_intra_sl'] < parameter['sre_sl']))]

        # filter - where sl = 0 & intra_sl = 0
        parameter.loc[(parameter['sl'] == 0) & (parameter['intra_sl'] == 0), 'ut_sl'] = 0
        parameter.loc[parameter['ut_sl'] == 0, 'sre_sl'] = 0
        parameter.loc[parameter['ut_sl'] == 0, 'sre_intra_sl'] = 0

        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['ut_orderside'] = parameter['orderside'].str.upper()
        parameter['ut_method'] = parameter['ut_method'].str.upper()
        
    
    elif code == 'S2UT':
        
        #filer intra sl
        parameter['intra_sl'] = parameter.apply(lambda row: row['sl'] + float(row['intra_sl'] .split('+')[-1]) if '+' in str(row['intra_sl']) else float(row['intra_sl']), axis=1)
        parameter = parameter[~((parameter['intra_sl'] != 0) & (parameter['intra_sl'] < parameter['sl']))]

        # filter - where sl = 0 & intra_sl = 0
        parameter.loc[(parameter['sl'] == 0) & (parameter['intra_sl'] == 0), 'ut_sl'] = 0

        parameter['orderside'] = parameter['orderside'].str.upper()
        parameter['ut_orderside'] = parameter['orderside'].str.upper()
        parameter['ut_method'] = parameter['ut_method'].str.upper()


    parameter.drop_duplicates(inplace=True, ignore_index=True)
    return parameter, len(parameter)

