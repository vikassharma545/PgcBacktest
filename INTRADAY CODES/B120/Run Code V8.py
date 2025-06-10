import os 
import sys
import math
import ctypes
import psutil
import requests
import nbformat
import datetime
import subprocess
import numpy as np
import pandas as pd
from time import sleep
from nbconvert import PythonExporter

# --- Telegram Bot Function ---
def send_msg_telegram(msg):
    try:
        requests.get(f'https://api.telegram.org/bot5156026417:AAExQbrMAPrV0qI8tSYplFDjZltLBzXTm1w/sendMessage?chat_id=-607631145&text={msg}')
    except Exception as e:
        print(f"Telegram Error: {e}")

def fun_timer(seconds):
    for i in range(seconds, -1, -1):
        print(f"🚀 {i} seconds 🚀", end="\r")
        sleep(1)

# --- User Menu Function ---
def menu_driver(options, msg=''):
    if len(options) == 1: return options[0]
    options.append("Exit !!!")
    while True:
        print("\nMenu:")
        for i, option in enumerate(options, start=1):
            print(f"{i}. {option}")
        try:
            choice = int(input(f"{msg} (1-{len(options)}): "))
            if 1 <= choice <= len(options):
                if choice == len(options):
                    print("Exiting...")
                    sys.exit()
                return options[choice - 1]
            else:
                print("Invalid choice.")
        except ValueError:
            print("Enter a number.")

# --- Notebook to Script Converter ---
output_csv_path = ""
def convert_notebook_to_script(notebook_path, script_path, temp_meta_data_path):
    global output_csv_path
    output_csv_path = ""
    
    with open(notebook_path, 'r', encoding='utf-8') as nb_file:
        nb_content = nbformat.read(nb_file, as_version=4)
        
    if temp_meta_data_path:
        for cell_idx, cell in enumerate(nb_content['cells']):
            if (cell['cell_type'] == 'code') and ("meta_data_path" in cell['source']):
                source = cell['source'].splitlines()
                for idx, value in enumerate(source):
                    if "meta_data_path =" in value:
                        source[idx] = f"meta_data_path = f'{temp_meta_data_path}'"
                    if ("output_csv_path =" in value):
                        output_csv_path = source[idx]
                nb_content['cells'][cell_idx]['source'] = '\n'.join(source)
        
    script, _ = PythonExporter().from_notebook_node(nb_content)
    with open(script_path, 'w', encoding='utf-8') as py_file:
        py_file.write(script)

# --- Platform-specific paths ---
cache_path = "C:/.temp"
strategy_cache_path = f"{cache_path}/{{strategy}}"
os.makedirs(cache_path, exist_ok=True)

# --- File Detection ---
all_files = os.listdir()
jupyter_files = [f for f in all_files if f.endswith('.ipynb')]
parameter_csv_files = [f for f in all_files if f.endswith('.csv') and 'metadata' not in f.lower() and "parameter" in f.lower()]
meta_data_csv_files = [f for f in all_files if f.endswith('.csv') and 'metadata' in f.lower()]

# --- Menu Selection ---
jupyter_code = menu_driver(jupyter_files, 'Choose Jupyter Code')
parameter_csv = menu_driver(parameter_csv_files, 'Choose Parameter CSV')
parameter_meta_data = menu_driver(meta_data_csv_files, 'Choose Parameter CSV')
no_of_terminal_allowed = int(input("Number of Terminal Allowed: "))
parameter_meta_data_for_run = parameter_meta_data if no_of_terminal_allowed == -1 else f"{cache_path}/{parameter_meta_data}"

code_base = jupyter_code.replace('.ipynb', '').title()

# --- Convert and Prepare ---
script_output = strategy_cache_path.format(strategy=code_base) + ".py"
convert_notebook_to_script(jupyter_code, script_output, parameter_meta_data_for_run)
code_details = f'\n#### RUN CODE ####\nCode: {jupyter_code}\nParameter: {parameter_csv} \nMetaData Parameter: {parameter_meta_data} \n##################'
print(code_details)

# --- Python Path ---
python_path = [p for p in sys.path if p.endswith("\\Lib\\site-packages")][0].replace("\\Lib\\site-packages", "") + "\\python.exe"
python_path = python_path.replace("\\", "/")
    
# --- Check Duplicate Code Execution ---
def check_duplicate_runs():
    count = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if 'python' in proc.info['name'].lower():
                for arg in proc.info.get('cmdline', []):
                    if os.path.normpath(arg) == os.path.normpath(sys.argv[0]):
                        count+=1
        except:
            pass
    return count

if check_duplicate_runs() > 1:
    print("\nRun Code Already Running for this Code :)")
    sleep(5)
    sys.exit()

from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *
pickle_path = 'C:/PICKLE/'
parameter_code = parameter_csv.split('_', maxsplit=1)[-1].split('.')[0]
_, parameter_len = get_parameter_data(parameter_code, parameter_csv)
meta_data, _ = get_meta_data(parameter_code, parameter_meta_data)
no_of_chunk = math.ceil(parameter_len/chunk_size)

output_csv_path = output_csv_path.split("f'{code}")[-1].split('\\')[0]
output_csv_path = f"{parameter_code}{output_csv_path}/"

index_dates = {}
index_dte_dates = {}
total_dates, total_pending_dates = 0, 0
for row_idx in range(len(meta_data)):
    if meta_data.loc[row_idx, 'run']:
        meta_row = meta_data.iloc[row_idx]
        index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
        total_dates += len(date_lists)
        index_dates[index] = index_dates.get(index, 0) + len(date_lists)
        files_dates = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {parameter_code}", parameter_len)]

        if files_dates:
            total_pending_dates += len(files_dates)
            index_dte_dates[(index, dte)] = sorted(index_dte_dates.get((index, dte), []) + files_dates)

file_details = f'\n####### OUTPUT FILES #######\nNo of Chunks: {no_of_chunk} \nTotals Dates: {total_dates} \nTotal Files Created: {no_of_chunk*total_dates}\nDates IndexWise: {index_dates} \n############################'
print(file_details)

print('MetaData Creating...')
if no_of_terminal_allowed > 0:
    sorted_keys = sorted(index_dte_dates.keys(), key=lambda k: len(index_dte_dates[k]), reverse=True)

    while len(sorted_keys) != no_of_terminal_allowed:
        if len(sorted_keys) < no_of_terminal_allowed:
            keys_to_split = sorted_keys[:(no_of_terminal_allowed - len(sorted_keys))]

            splited = True
            for key in index_dte_dates:
                if key in keys_to_split and len(index_dte_dates[key]) > 1:
                    splited = False
                    value = index_dte_dates[key]
                    mid = (len(value) + 1) // 2
                    first_half = value[:mid]
                    second_half = value[mid:]

                    a_key = ('a',) + key
                    b_key = ('b',) + key
                    index_dte_dates[a_key] = first_half
                    index_dte_dates[b_key] = second_half
                    del index_dte_dates[key]
                    break

            if splited:
                break

            sorted_keys = sorted(index_dte_dates.keys(), key=lambda k: len(index_dte_dates[k]), reverse=True)

        else:
            sorted_keys = sorted_keys[:no_of_terminal_allowed]
            index_dte_dates = {key: value for key, value in index_dte_dates.items() if key in sorted_keys}

    temp_meta_data = pd.DataFrame(columns=meta_data.columns)
    for key, value in index_dte_dates.items():
        temp_meta_data.loc[len(temp_meta_data)] = [key[-2], key[-1], value[0].strftime("%d-%m-%Y"), value[-1].strftime("%d-%m-%Y"), datetime.time(9,15), datetime.time(15,29), True]
    temp_meta_data.to_csv(parameter_meta_data_for_run, index=False)
else:
    meta_data.to_csv(parameter_meta_data_for_run, index=False)
print('MetaData Created')

print('\nRunning Code...\n')
# --- Read CSV and Start Codes ---
df = pd.read_csv(parameter_meta_data_for_run)
for idx, row in df.iterrows():
    if row.get('run', False):
        print(f"Running Row {idx}: {code_base}")
        subprocess.run(["start", python_path, script_output, "-r", str(idx)], shell=True)
        sleep(2)

# --- Monitoring Memory and CPU ---
terminal_title = f'{code_base} : Code Monitor: Auto Restart on High RAM'
ctypes.windll.kernel32.SetConsoleTitleW(terminal_title)

while True:
    try:
        mem_usage = psutil.virtual_memory().percent
        cpu_usage = psutil.cpu_percent()
        msg = f"{code_details}\n{file_details}\n\n🧠 RAM Used: {mem_usage}%\n🖥 CPU Used: {cpu_usage}%"
        
        code_count = 0
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            if 'python' in proc.info['name'].lower():
                for arg in proc.info.get('cmdline', []):
                    if script_output in arg:
                        try:
                            code_count += 1
                            break
                        except:
                            pass

        # High memory condition
        if mem_usage > 90 or (no_of_terminal_allowed != -1 and code_count < math.floor(no_of_terminal_allowed*0.60)):
            
            if  mem_usage > 90:
                print(f"\nHigh RAM: {mem_usage}% at {datetime.datetime.now()}\n")
            else:
                print(f"\nNumber of Terminal Running - {code_count} << {no_of_terminal_allowed}")
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                if 'python' in proc.info['name'].lower():
                    for arg in proc.info.get('cmdline', []):
                        if script_output in arg:
                            try:
                                psutil.Process(proc.info['pid']).terminate()
                                sleep(0.5)
                                psutil.Process(proc.info['pid']).terminate()
                                sleep(0.5)
                                psutil.Process(proc.info['pid']).terminate()
                                sleep(0.5)
                                psutil.Process(proc.info['pid']).terminate()
                                sleep(0.5)
                                print(f"Killed - {arg}")
                                break
                            except:
                                pass

            print('MetaData Creating...')
            index_dte_dates = {}
            total_pending_dates = 0
            for row_idx in range(len(meta_data)):
                if meta_data.loc[row_idx, 'run']:
                    meta_row = meta_data.iloc[row_idx]
                    index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                    files_dates = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {parameter_code}", parameter_len)]

                    if files_dates:
                        total_pending_dates += len(files_dates)
                        index_dte_dates[(index, dte)] = sorted(index_dte_dates.get((index, dte), []) + files_dates)
    
            if no_of_terminal_allowed > 0:
        
                sorted_keys = sorted(index_dte_dates.keys(), key=lambda k: len(index_dte_dates[k]), reverse=True)

                while len(sorted_keys) != no_of_terminal_allowed:
                    if len(sorted_keys) < no_of_terminal_allowed:
                        keys_to_split = sorted_keys[:(no_of_terminal_allowed - len(sorted_keys))]

                        splited = True
                        for key in index_dte_dates:
                            if key in keys_to_split and len(index_dte_dates[key]) > 1:
                                splited = False
                                value = index_dte_dates[key]
                                mid = (len(value) + 1) // 2
                                first_half = value[:mid]
                                second_half = value[mid:]

                                a_key = ('a',) + key
                                b_key = ('b',) + key
                                index_dte_dates[a_key] = first_half
                                index_dte_dates[b_key] = second_half
                                del index_dte_dates[key]
                                break

                        if splited:
                            no_of_terminal_allowed = -1
                            break

                        sorted_keys = sorted(index_dte_dates.keys(), key=lambda k: len(index_dte_dates[k]), reverse=True)
                    else:
                        sorted_keys = sorted_keys[:no_of_terminal_allowed]
                        index_dte_dates = {key: value for key, value in index_dte_dates.items() if key in sorted_keys}

                temp_meta_data = pd.DataFrame(columns=meta_data.columns)
                for key, value in index_dte_dates.items():
                    temp_meta_data.loc[len(temp_meta_data)] = [key[-2], key[-1], value[0].strftime("%d-%m-%Y"), value[-1].strftime("%d-%m-%Y"), datetime.time(9,15), datetime.time(15,29), True]
                temp_meta_data.to_csv(parameter_meta_data_for_run, index=False)
            else:
                meta_data.to_csv(parameter_meta_data_for_run, index=False)
                
            print('MetaData Created')

            # Restart the script
            print('\nCode is About to Restart in ...')
            fun_timer(100)
            # --- Read CSV and Start Codes ---
            df = pd.read_csv(parameter_meta_data_for_run)
            for idx, row in df.iterrows():
                if row.get('run', False):
                    print(f"Running Row {idx}: {code_base}")
                    subprocess.run(["start", python_path, script_output, "-r", str(idx)], shell=True)
                    sleep(2)
        else:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(msg, end='\r')
            sleep(3)

    except Exception as e:
        err_msg = f"Error in monitoring loop: {e}"
        print(err_msg)
        input("Press Enter to continue...")