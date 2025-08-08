import os
import sys
import math
import psutil
import nbformat
import tempfile
import subprocess
import pandas as pd
import polars as pl
from tqdm import tqdm
from time import sleep
from pathlib import Path
import concurrent.futures
import pyarrow.parquet as pq
from tkinter import Tk, filedialog
from nbconvert import PythonExporter

from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *

def print_heading(title="🗂 Heading"):
    print("\n" + "="*60)
    print(f"{title.center(60)}")
    print("="*60 + "\n")

def set_terminal_title(title: str):
    if sys.platform == 'win32':
        import ctypes
        ctypes.windll.kernel32.SetConsoleTitleW(title)
    else:  # Linux, macOS
        sys.stdout.write(f"\33]0;{title}\a")
        sys.stdout.flush()

def fun_timer(seconds):
    for i in range(seconds, -1, -1):
        print(f"🚀 {i} seconds 🚀", end="\r")
        sleep(1)
        
def select_folder_gui(title="Select a Folder", initialdir=None) -> Path | None:
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True) 
    folder_path = filedialog.askdirectory(title=title, initialdir=initialdir, parent=root)
    root.destroy() 
    return Path(folder_path).as_posix() + "/" if folder_path else None

def select_file_gui(title="Select a File", filetypes=None, initialdir=None) -> Path | None:
    if filetypes is None:
        filetypes = [("All files", "*.*")]

    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    file_path = filedialog.askopenfilename(title=title, filetypes=filetypes, initialdir=initialdir, parent=root)
    root.destroy()
    return Path(file_path) if file_path else None

def menu_driver(options, msg=''):
    if len(options) == 1: return Path(os.path.abspath(options[0]))
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
                return Path(os.path.abspath(options[choice - 1]))
            else:
                print("Invalid choice.")
        except ValueError:
            print("Enter a number !!!")

def get_pickle_path(meta_data_path):
    
    if sys.platform == "win32":
        base_dir = "C:"
    elif sys.platform == "linux":
        base_dir = os.environ['HOME'] # "/home/user"
    else:
        print("OS not Defined !!!")
        print("\nPlease select the Pickle folder: ")
        pickle_path = select_folder_gui("Select Pickle Folder")
        return pickle_path
    
    try:
        meta_df = pd.read_csv(meta_data_path)
        meta_indices = set(meta_df['index'].unique())
    except:
        print("Error getting Pickle Path")
        print("\nPlease select the Pickle folder: ")
        pickle_path = select_folder_gui("Select Pickle Folder")
        if os.path.exists(pickle_path):
            return pickle_path
    
    indices = ['BANKNIFTY', 'NIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'BANKEX', 'SENSEX']
    if all(index in indices for index in meta_indices):
        pickle_path = f"{base_dir}/PICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path
    
    indices = ['COPPER', 'CRUDEOIL', 'CRUDEOILM', 'GOLD', 'GOLDM', 'NATURALGAS', 'NATGASMINI', 'SILVER', 'SILVERM', 'ZINC']
    if all(index in indices for index in meta_indices):
        pickle_path = f"{base_dir}/MCXPICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path
        
    indices = ['SPXW_FRI', 'SPXW_MON', 'SPXW_THU', 'SPXW_TUE', 'SPX_WED', 'XSP_FRI', 'XSP_MON', 'XSP_THU', 'XSP_TUE', 'XSP_WED']
    indices += ['TSLA', 'AAPL', 'NVDA', 'AMZN', 'META', 'AMD', 'MSFT', 'NFLX', 'GME', 'BABA']
    if all(index in indices for index in meta_indices):
        pickle_path = f"{base_dir}/USPICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path

    print("Unable to determine Pickle folder !!!")
    print("\nPlease select the Pickle folder: ")
    pickle_path = select_folder_gui("Select Pickle Folder")
    return pickle_path

def convert_notebook_to_script(pickle_path: Path, notebook_path: Path, parameter_path: Path, meta_data_path: Path):
    
    ## reading jupyter notebook file
    with open(notebook_path, 'r', encoding='utf-8') as nb_file:
        nb_content = nbformat.read(nb_file, as_version=4)
        
    run_scrip_path = Path(tempfile.gettempdir()) / f"{notebook_path.stem}.py"
    temp_meta_data_path = Path(tempfile.gettempdir()) / meta_data_path.name

    for cell_idx, cell in enumerate(nb_content['cells']):
        if (cell['cell_type'] == 'code') and ("pickle_path" in cell['source']) and ("parameter_path" in cell['source']) and ("meta_data_path" in cell['source']):
            source = cell['source'].splitlines()
            for idx, value in enumerate(source):
                
                if "code=" in value.replace(" ", ""):
                    quote_char = '"' if '"' in value.replace(" ", "") else "'"
                    start = value.find(quote_char) + 1
                    end = value.find(quote_char, start)
                    code = value[start:end]     

                if "pickle_path=" in value.replace(" ", ""):
                    source[idx] = f"pickle_path = f'{pickle_path}'"
                    
                if "parameter_path=" in value.replace(" ", ""):
                    source[idx] = f"parameter_path = f'{parameter_path.as_posix()}'"
                    
                if "meta_data_path=" in value.replace(" ", ""):
                    source[idx] = f"meta_data_path = f'{temp_meta_data_path.as_posix()}'"
                    
                if "output_csv_path=" in value.replace(" ", ""):
                    output_csv_path = value.replace(" ", "").split("=")[-1]
                    
                    if code and "{code}" in output_csv_path:
                        output_csv_path = output_csv_path.format(code=code)
                    
                    quote_char = '"' if '"' in output_csv_path.replace(" ", "") else "'"
                    start = output_csv_path.find(quote_char) + 1
                    end = output_csv_path.find(quote_char, start)
                    output_csv_path = output_csv_path[start:end]
                    output_csv_path = f"{(notebook_path.parent / output_csv_path).as_posix()}/"
                    source[idx] = f"output_csv_path = f'{output_csv_path}'"
                    
            nb_content['cells'][cell_idx]['source'] = '\n'.join(source)

    script, _ = PythonExporter().from_notebook_node(nb_content)
    with open(run_scrip_path, 'w', encoding='utf-8') as py_file:
        py_file.write(script)
        
    return code, output_csv_path, temp_meta_data_path, run_scrip_path

def get_len_of_parameter_data(code, parameter_path):
    _, parameter_len = get_parameter_data(code, parameter_path)
    return parameter_len

def get_file_details(meta_data, pickle_path, output_csv_path, code, parameter_len, is_weekly):

    index_dates = {}
    index_dte_dates = {}
    total_dates, total_pending_dates = 0, 0
    dir_files = set(os.listdir(output_csv_path)) if os.path.exists(output_csv_path) else set()
    for row_idx in range(len(meta_data)):
        if meta_data.loc[row_idx, 'run']:
            meta_row = meta_data.iloc[row_idx]

            if not is_weekly:
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                total_dates += len(date_lists)
                index_dates[index] = index_dates.get(index, 0) + len(date_lists)
                files_dates = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len, dir_files)]

                if files_dates:
                    total_pending_dates += len(files_dates)
                    index_dte_dates[(index, dte)] = sorted(index_dte_dates.get((index, dte), []) + files_dates)
            
            else:
                index, from_dte, to_dte, _, _, _, _, week_lists = get_meta_row_data(meta_row, pickle_path, weekly=True)
                total_dates += len(week_lists)
                index_dates[index] = index_dates.get(index, 0) + len(week_lists)
                files_dates = [week_dates for week_dates in week_lists if not is_file_exists(output_csv_path, f"{index} {week_dates[0].date()} {week_dates[-1].date()} {from_dte}-{to_dte} {code}", parameter_len, dir_files)]

                if files_dates:
                    total_pending_dates += len(files_dates)
                    index_dte_dates[(index, from_dte, to_dte)] = sorted(index_dte_dates.get((index, from_dte, to_dte), []) + files_dates)

    return index_dates, index_dte_dates, total_dates, total_pending_dates

def create_temp_meta_data(index_dte_dates, meta_data, temp_meta_data_path, no_of_terminal_allowed, is_weekly):
    
    print(f'Creating MetaData...')
    if no_of_terminal_allowed > 0:
        
        if not is_weekly:
            #### For Intraday Codes
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
            temp_meta_data.to_csv(temp_meta_data_path, index=False)
            
        else:
            # For Weekly Codes
            temp_meta_data = meta_data[meta_data['run'] == True].copy()
            temp_meta_data = temp_meta_data[temp_meta_data.apply(lambda row: (row['index'], row['from_dte'], row['to_dte']) in index_dte_dates.keys(), axis=1)]
            
            if len(temp_meta_data) < no_of_terminal_allowed:
                no_of_terminal_allowed = -1
            else:
                temp_meta_data = temp_meta_data.iloc[:no_of_terminal_allowed]

            temp_meta_data.to_csv(temp_meta_data_path, index=False)
    else:
        temp_meta_data = meta_data[meta_data['run'] == True].copy()
        temp_meta_data.to_csv(temp_meta_data_path, index=False)

    print(f'MetaData Created: {temp_meta_data_path}')
    
    return no_of_terminal_allowed

def checking_all_parquet_file(output_csv_path):

    def check_parquet_file(file):
        try:
            table = pq.read_table(file)
            return None
        except Exception as e:
            return (f"Invalid file: {file} | Error: {e}")

    print("\nChecking ALL Parquet Files...")
    parquet_files = [os.path.join(output_csv_path, f) for f in os.listdir(output_csv_path) if f.endswith("parquet")]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        results = list(tqdm(executor.map(check_parquet_file, parquet_files), total=len(parquet_files), desc="Checking Parquet Files"))

    errors = [r for r in results if r]
    error_files = [parquet_files[i] for i, r in enumerate(results) if r]

    return errors, error_files

if __name__ == "__main__":
    
    # Set terminal title    
    title = "Run Code"
    set_terminal_title(title)
    print_heading(title)
    
    all_files = os.listdir()
    jupyter_files = [f for f in all_files if f.endswith('.ipynb') and 'Untitled' not in f]
    parameter_csv_files = [f for f in all_files if f.endswith('.csv') and 'metadata' not in f.lower() and "parameter" in f.lower()]
    meta_data_csv_files = [f for f in all_files if f.endswith('.csv') and 'metadata' in f.lower()]
    
    # --- Menu Selection ---
    print("\nPlease select the Jupyter Notebook file to run:  ", end="")
    if jupyter_files:
        notebook_path = menu_driver(jupyter_files, 'Choose Jupyter Code')
    else:
        notebook_path = select_file_gui("Select Jupyter Notebook File", [("Jupyter Notebook", "*.ipynb")])
        
    if notebook_path:
        print(notebook_path.stem)
    else:
        print("No Jupyter Notebook file selected. Exiting...")
        input("Press Enter to Exit...")
        sys.exit(1)
        
    # select Parameters file
    print("\nPlease select the Parameters file:  ", end="")
    if parameter_csv_files:
        parameter_path = menu_driver(parameter_csv_files, 'Choose Parameter CSV')
    else:
        parameter_path = select_file_gui("Select Parameters File", [("Parameters File", "*.csv")])
        
    if parameter_path:
        print(parameter_path.stem)
    else:
        print("No Parameters file selected. Exiting...")
        input("Press Enter to Exit...")
        sys.exit(1)  
        
    # select Metadata file
    print("\nPlease select the Metadata file:  ", end="")
    if meta_data_csv_files:
        meta_data_path = menu_driver(meta_data_csv_files, 'Choose Parameter CSV')
    else:
        meta_data_path = select_file_gui("Select Metadata File", [("Metadata File", "*.csv")])
        
    if meta_data_path:
        print(meta_data_path.stem)
    else:
        print("No Metadata file selected. Exiting...")
        input("Press Enter to Exit...")
        sys.exit(1)

    # select Pickle folder
    pickle_path = get_pickle_path(meta_data_path)
    if pickle_path:
        print(f"\nPickle Path: {pickle_path}")
    else:
        print("No Pickle folder selected. Exiting...")
        input("Press Enter to Exit...")
        sys.exit(1)

    code, output_csv_path, temp_meta_data_path, run_scrip_path = convert_notebook_to_script(pickle_path, notebook_path, parameter_path, meta_data_path)

    parameter_len = get_len_of_parameter_data(code, parameter_path)
    meta_data, _ = get_meta_data(code, meta_data_path)
    no_of_chunk = math.ceil(parameter_len/chunk_size)
    
    is_weekly = True if ("from_dte" in meta_data.columns) and ("to_dte" in meta_data.columns) else False
    weeks_or_dates = "Weeks" if ("from_dte" in meta_data.columns) and ("to_dte" in meta_data.columns) else "Dates"
    index_dates, index_dte_dates, total_dates, total_pending_dates = get_file_details(meta_data, pickle_path, output_csv_path, code, parameter_len, is_weekly)

    code_details = f'\n#### RUN CODE ####\nCode: {code}\nJupyterCode: {notebook_path.stem} \nParameter: {parameter_path.stem} \nMetaData Parameter: {meta_data_path.stem} \n##################'

    file_details = f'\n####### OUTPUT FILES #######\
                    \nNo of Chunks: {no_of_chunk} \
                    \nTotals {weeks_or_dates}: {total_dates} \
                    \nTotal Files Created: {no_of_chunk*total_dates}\
                    \n{weeks_or_dates} IndexWise: {index_dates} \
                    \n############################'
    
    print(code_details)
    print(file_details)
    
    print(f'\nTotal Pending {weeks_or_dates}: {total_pending_dates}')
    print(f'Total Pending Files: {total_pending_dates*no_of_chunk}')
    if total_pending_dates == 0:
        print(f'\nNo Pending {weeks_or_dates} Left all Dates files Complete :)')
        input("Press Enter to Exit !!!")
        sys.exit()
        
    title = f'Code Monitor: {code}'
    set_terminal_title(title)
    
    no_of_terminal_allowed = int(input("Enter the number of terminals allowed: "))
    no_of_terminal_allowed = create_temp_meta_data(index_dte_dates, meta_data, temp_meta_data_path, no_of_terminal_allowed, is_weekly)

    ### Run the converted script
    print('\nRunning Code...\n')
    processes = []
    df = pd.read_csv(temp_meta_data_path)
    for idx, row in df.iterrows():
        if row.get('run', False):

            print(f"Running Row {idx}. ")

            if sys.platform == 'linux':
                pid_file = Path(tempfile.gettempdir()) / f"proc_{code}_{idx}.pid"
                cmd = f"echo $$ > {pid_file}; exec {sys.executable} '{run_scrip_path}' -r {idx}"
                proc = subprocess.Popen(["gnome-terminal", "--", "bash", "-c", cmd], start_new_session=True)
                sleep(3)

                try:
                    script_pid = int(pid_file.read_text().strip())
                finally:
                    pid_file.unlink(missing_ok=True)         
            else:
                proc = subprocess.Popen([sys.executable, run_scrip_path, "-r", str(idx)], creationflags=subprocess.CREATE_NEW_CONSOLE)
                sleep(3)
                script_pid = proc.pid

            try:
                proc = psutil.Process(script_pid)
                processes.append((idx, proc))
            except psutil.NoSuchProcess:
                print(f'Process not Found in Row {idx}')

    while True:
        
        fun_timer(10)
        index_dates, index_dte_dates, total_dates, total_pending_dates = get_file_details(meta_data, pickle_path, output_csv_path, code, parameter_len, is_weekly)

        file_details = f'\n####### OUTPUT FILES #######\
                        \nNo of Chunks: {no_of_chunk} \
                        \nTotals {weeks_or_dates}: {total_dates} \
                        \nTotal Files Created: {no_of_chunk*total_dates}\
                        \n{weeks_or_dates} IndexWise: {index_dates} \
                        \n############################'

        os.system('clear') if sys.platform == 'linux' else os.system('cls')
        print(code_details)
        print(file_details)

        print(f'\nTotal Pending {weeks_or_dates}: {total_pending_dates}')
        print(f'Total Pending Files: {total_pending_dates*no_of_chunk}')
        if total_pending_dates == 0:
            print(f'\nNo Pending {weeks_or_dates} Left all Dates files Complete :)')

            errors, error_files = checking_all_parquet_file(output_csv_path)
            
            if errors:
                for err, error_file in zip(errors, error_files):
                    print(err)
                    try:
                        os.remove(error_file)
                        print(f"Deleted: {error_file}")
                    except Exception as e:
                        print(f"Failed to delete {error_file}: {e}")

                continue
            else:
                print("All Parquet files are valid.\n")

            input("Press Enter to Exit !!!")
            sys.exit()

        ### checking script is running
        new_processes = []
        df = pd.read_csv(temp_meta_data_path)
        dir_files = set(os.listdir(output_csv_path)) if os.path.exists(output_csv_path) else set()
        for idx, proc in processes:
            if not proc.is_running():
                
                meta_row = df.loc[int(idx)]
                
                if not is_weekly:
                    index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                    pending_files = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len, dir_files)]
                else:
                    index, from_dte, to_dte, _, _, _, _, week_lists = get_meta_row_data(meta_row, pickle_path, weekly=True)
                    pending_files = [week_dates for week_dates in week_lists if not is_file_exists(output_csv_path, f"{index} {week_dates[0].date()} {week_dates[-1].date()} {from_dte}-{to_dte} {code}", parameter_len, dir_files)]

                if pending_files:

                    print(f"\nfound closed process in Row {idx}.")
                    print(f"Pending Files in Row {idx}: {len(pending_files)}")
                    print(f"Running Row {idx}. ")

                    if sys.platform == 'linux':
                        pid_file = Path(tempfile.gettempdir()) / f"proc_{code}_{idx}.pid"
                        cmd = f"echo $$ > {pid_file}; exec {sys.executable} '{run_scrip_path}' -r {idx}"
                        proc = subprocess.Popen(["gnome-terminal", "--", "bash", "-c", cmd], start_new_session=True)
                        sleep(3)

                        try:
                            script_pid = int(pid_file.read_text().strip())
                        finally:
                            pid_file.unlink(missing_ok=True)         
                    else:
                        proc = subprocess.Popen([sys.executable, run_scrip_path, "-r", str(idx)], creationflags=subprocess.CREATE_NEW_CONSOLE)
                        sleep(3)
                        script_pid = proc.pid

                    try:
                        proc = psutil.Process(script_pid)
                        new_processes.append((idx, proc))
                    except psutil.NoSuchProcess:
                        print(f'Process not Found in Row {idx}')
                    
            else:
                new_processes.append((idx, proc))
                
        processes = new_processes
        print(f"\nRunning Processes: {len(processes)}")
        print(f"Allowed Processes: {no_of_terminal_allowed}\n")

        if len(processes) <= int(no_of_terminal_allowed*0.7) and no_of_terminal_allowed != -1:
            
            print(f"\nNumber of Terminal Running - {len(processes)} << {no_of_terminal_allowed}")
            
            for idx, proc in processes:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                    print(f"Killed - {idx}")
                except psutil.NoSuchProcess:
                    print(f"Process already terminated - {idx}")
                except psutil.TimeoutExpired:
                    proc.kill()
                    print(f"Forced kill - {idx}")

            no_of_terminal_allowed = create_temp_meta_data(index_dte_dates, meta_data, temp_meta_data_path, no_of_terminal_allowed, is_weekly)

            ### Run the converted script
            print('\nCode is About to Restart in ...')
            fun_timer(30)
            print('\nRunning Code...\n')
            processes = []
            df = pd.read_csv(temp_meta_data_path)
            for idx, row in df.iterrows():
                if row.get('run', False):

                    print(f"Running Row {idx}. ")

                    if sys.platform == 'linux':
                        pid_file = Path(tempfile.gettempdir()) / f"proc_{code}_{idx}.pid"
                        cmd = f"echo $$ > {pid_file}; exec {sys.executable} '{run_scrip_path}' -r {idx}"
                        proc = subprocess.Popen(["gnome-terminal", "--", "bash", "-c", cmd], start_new_session=True)
                        sleep(3)
                        
                        try:
                            script_pid = int(pid_file.read_text().strip())
                        finally:
                            pid_file.unlink(missing_ok=True)

                    else:
                        proc = subprocess.Popen([sys.executable, run_scrip_path, "-r", str(idx)], creationflags=subprocess.CREATE_NEW_CONSOLE)
                        sleep(3)
                        script_pid = proc.pid

                    try:
                        proc = psutil.Process(script_pid)
                        processes.append((idx, proc))
                    except psutil.NoSuchProcess:
                        print(f'Process not Found in Row {idx}') 