import re
import os
import sys
import math
import ctypes
import psutil
import nbformat
import datetime
import subprocess
import numpy as np
import pandas as pd
from time import sleep
from nbconvert import PythonExporter

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
            print("Enter a number !!!")

# --- Notebook to Script Converter ---
def convert_notebook_to_script(notebook_path, script_path, temp_meta_data_path):
    
    with open(notebook_path, 'r', encoding='utf-8') as nb_file:
        nb_content = nbformat.read(nb_file, as_version=4)
        
    if temp_meta_data_path:
        for cell_idx, cell in enumerate(nb_content['cells']):
            if (cell['cell_type'] == 'code') and ("meta_data_path" in cell['source']):
                source = cell['source'].splitlines()
                for idx, value in enumerate(source):
                    if "meta_data_path =" in value:
                        source[idx] = f"meta_data_path = f'{temp_meta_data_path}'"
                    if "output_csv_path =" in value:
                        output_csv_path = source[idx]
                        output_csv_path = output_csv_path.split("=")[-1].strip()
                        output_csv_path = re.search(r'[\'"](.*?)[\'"]', output_csv_path).group(1).replace('\\\\', '\\')
                    if "code =" in value:
                        code = source[idx]
                        code = code.split("=")[-1].strip()
                        code = re.search(r'[\'"](.*?)[\'"]', code).group(1)
                        
                nb_content['cells'][cell_idx]['source'] = '\n'.join(source)
        
    script, _ = PythonExporter().from_notebook_node(nb_content)
    with open(script_path, 'w', encoding='utf-8') as py_file:
        py_file.write(script)
        
    return code, output_csv_path

def get_linux_pid(proc):
    
    cmdline = " ".join(processes[0].args[-4:])
    linux_processes = subprocess.run(['wsl.exe', 'ps', 'aux'], capture_output=True, text=True)
    linux_processes = [l for l in linux_processes.stdout.splitlines() if "/usr/bin/python3" in l and "cmd.exe" not in l]

    for process in linux_processes:
        if cmdline in process:
            pid = process.split()[1]
            return pid

# --- Platform-specific paths ---
if sys.platform == 'linux':
    cache_path = "/mnt/c/.temp"
    pickle_path = '/mnt/c/PICKLE/'
else:
    cache_path = "C:/.temp"
    pickle_path = 'C:/PICKLE/'

strategy_cache_path = f"{cache_path}/{{strategy}}"
os.makedirs(cache_path, exist_ok=True)

# --- File Detection ---
all_files = os.listdir()
jupyter_files = [f for f in all_files if f.endswith('.ipynb') and 'Untitled' not in f]
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
code, output_csv_path = convert_notebook_to_script(jupyter_code, script_output, parameter_meta_data_for_run)
code_details = f'\n#### RUN CODE ####\nCode: {code}\nJupyterCode: {jupyter_code} \nParameter: {parameter_csv} \nMetaData Parameter: {parameter_meta_data} \n##################'
print(code_details)

python_path = sys.executable

from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *

_, parameter_len = get_parameter_data(code, parameter_csv)
meta_data, _ = get_meta_data(code, parameter_meta_data)
no_of_chunk = math.ceil(parameter_len/chunk_size)
output_csv_path = output_csv_path.format(code=code) if "{code}" in output_csv_path else output_csv_path

index_dates = {}
index_dte_dates = {}
total_dates, total_pending_dates = 0, 0
for row_idx in range(len(meta_data)):
    if meta_data.loc[row_idx, 'run']:
        meta_row = meta_data.iloc[row_idx]
        index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
        total_dates += len(date_lists)
        index_dates[index] = index_dates.get(index, 0) + len(date_lists)
        files_dates = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len)]

        if files_dates:
            total_pending_dates += len(files_dates)
            index_dte_dates[(index, dte)] = sorted(index_dte_dates.get((index, dte), []) + files_dates)

file_details = f'\n####### OUTPUT FILES #######\nNo of Chunks: {no_of_chunk} \nTotals Dates: {total_dates} \nTotal Files Created: {no_of_chunk*total_dates}\nDates IndexWise: {index_dates} \n############################'
print(file_details)

print(f"\nPending Dates: {total_pending_dates}")

if total_pending_dates == 0:
    print('\nNo Pending Dates Left all Dates files Complete :)')
    sleep(5)
    sys.exit()

title = f'Code Monitor: {code_base}'
if sys.platform == 'linux':
    sys.stdout.write(f"\033]0;{title}\007")
    sys.stdout.flush()
elif sys.platform == 'win32':
    import ctypes
    ctypes.windll.kernel32.SetConsoleTitleW(title)


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
                no_of_terminal_allowed = -1
                break

            sorted_keys = sorted(index_dte_dates.keys(), key=lambda k: len(index_dte_dates[k]), reverse=True)

        else:
            sorted_keys = sorted_keys[:no_of_terminal_allowed]
            index_dte_dates = {key: value for key, value in index_dte_dates.items() if key in sorted_keys}

    temp_meta_data = pd.DataFrame(columns=meta_data.columns)
    for key, value in index_dte_dates.items():
        temp_meta_data.loc[len(temp_meta_data)] = [key[-2], key[-1], value[0].strftime("%d-%m-%Y"), value[-1].strftime("%d-%m-%Y"), meta_data.loc[(meta_data['index'] == key[-2]) & (meta_data['dte'] == key[-1]), 'start_time'].iloc[0], meta_data.loc[(meta_data['index'] == key[-2]) & (meta_data['dte'] == key[-1]), 'end_time'].iloc[0], True]
    temp_meta_data.to_csv(parameter_meta_data_for_run, index=False)
else:
    meta_data.to_csv(parameter_meta_data_for_run, index=False)
print('MetaData Created')

processes = []
print('\nRunning Code...\n')
# --- Read CSV and Start Codes ---
df = pd.read_csv(parameter_meta_data_for_run)
for idx, row in df.iterrows():
    if row.get('run', False):
        print(f"Running Row {idx}: {code_base}")
        if sys.platform == 'linux':
            proc = subprocess.Popen(['cmd.exe', '/c', 'start', 'wsl.exe', python_path, script_output, "-r", str(idx)])
        else:
            proc = subprocess.Popen([python_path, script_output, "-r", str(idx)])
        processes.append(proc)
        sleep(5)

# --- Monitoring Memory/CPU & Terminals ---
check_time = datetime.datetime.now() + datetime.timedelta(minutes=5)
while True:
    try:
        total_pending_dates = 0
        for row_idx in range(len(meta_data)):
            if meta_data.loc[row_idx, 'run']:
                meta_row = meta_data.iloc[row_idx]
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                files_dates = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len)]
                total_pending_dates += len(files_dates)
                
        if total_pending_dates == 0:
            print('\nNo Pending Dates Left all Dates files Complete :)')
            sleep(5)
            sys.exit()
            
        mem_usage = psutil.virtual_memory().percent
        cpu_usage = psutil.cpu_percent()
        
        df = pd.read_csv(parameter_meta_data_for_run)
        for idx, proc in enumerate(processes[::]):
            if proc.poll() is not None:

                meta_row = df.loc[int(proc.args[-1])]
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                pending_files = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len)]

                if pending_files:
                    print(f"Running Row {proc.args[-1]}: {code_base}")
                    proc = subprocess.Popen(proc.args)
                    sleep(5)
                    processes[idx] = proc

        no_of_terminal_running = len([proc.poll() for proc in processes if proc.poll() is None])
        msg = f"{code_details}\n{file_details}\n\n🧠 RAM Used: {mem_usage}%\n🖥 CPU Used: {cpu_usage}% \n🖥 Pending Dates: {total_pending_dates} \n🖥 No of Terminal Allowed: {no_of_terminal_allowed} \n🖥 No of Terminal Running: {no_of_terminal_running}"

        # High memory condition
        if mem_usage > 90 or (no_of_terminal_running == 0) or (no_of_terminal_allowed != -1 and check_time < datetime.datetime.now() and no_of_terminal_running < math.floor(no_of_terminal_allowed*0.70)):
            
            if mem_usage > 90:
                print(f"\nHigh RAM: {mem_usage}% at {datetime.datetime.now()}\n")
            else:
                print(f"\nNumber of Terminal Running - {no_of_terminal_running} << {no_of_terminal_allowed}")
                check_time += datetime.timedelta(minutes=5)
            
            # Killed Processes
            for proc in processes:
                try:
                    
                    if sys.platform == 'linux':
                        proc = psutil.Process(int(get_linux_pid(proc)))
                    
                    proc.terminate()
                    proc.wait(timeout=2)
                    print(f"Killed - {proc.args[-1]}")
                except psutil.TimeoutExpired:
                    proc.kill()
                    print(f"Forced kill - {proc.args[-1]}")

            print('MetaData Creating...')
            index_dte_dates = {}
            total_pending_dates = 0
            for row_idx in range(len(meta_data)):
                if meta_data.loc[row_idx, 'run']:
                    meta_row = meta_data.iloc[row_idx]
                    index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                    files_dates = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len)]

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
                    temp_meta_data.loc[len(temp_meta_data)] = [key[-2], key[-1], value[0].strftime("%d-%m-%Y"), value[-1].strftime("%d-%m-%Y"), meta_data.loc[(meta_data['index'] == key[-2]) & (meta_data['dte'] == key[-1]), 'start_time'].iloc[0], meta_data.loc[(meta_data['index'] == key[-2]) & (meta_data['dte'] == key[-1]), 'end_time'].iloc[0], True]
                temp_meta_data.to_csv(parameter_meta_data_for_run, index=False)
            else:
                meta_data.to_csv(parameter_meta_data_for_run, index=False)
                
            print('MetaData Created')

            # Restart the script
            print('\nCode is About to Restart in ...')
            if no_of_terminal_running == 0:
                fun_timer(5)
            else:
                fun_timer(60)
                
            processes = []
            print('\nRunning Code...\n')
            # --- Read CSV and Start Codes ---
            df = pd.read_csv(parameter_meta_data_for_run)
            for idx, row in df.iterrows():
                if row.get('run', False):
                    print(f"Running Row {idx}: {code_base}")
                    if sys.platform == 'linux':
                        proc = subprocess.Popen(['cmd.exe', '/c', 'start', 'wsl.exe', python_path, script_output, "-r", str(idx)])
                    else:
                        proc = subprocess.Popen([python_path, script_output, "-r", str(idx)])
                    processes.append(proc)
                    sleep(5)

        else:
            print()
            fun_timer(10)
            os.system('clear') if sys.platform == 'linux' else os.system('cls')
            print(msg, end='\r')

    except Exception as e:
        err_msg = f"Error in monitoring loop: {e}"
        input(err_msg)