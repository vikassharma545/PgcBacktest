import re
import os
import sys
import math
import time
import ctypes
import psutil
import pickle
import hashlib
import nbformat
import tempfile
import fileinput
import subprocess
import pandas as pd
from tqdm import tqdm
from time import sleep
from pathlib import Path
import concurrent.futures
import pyarrow.parquet as pq
from tkinter import Tk, filedialog
from nbconvert import PythonExporter
from inputimeout import inputimeout, TimeoutOccurred

from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *

def get_run_uid(notebook_path):
    """Short unique ID from notebook's full path — prevents temp file collision."""
    return hashlib.md5(str(notebook_path.resolve()).encode()).hexdigest()[:8]

def get_sleep_config(parameter_len):
    """Dynamic sleep/timer values based on parameter size."""
    if parameter_len < 5_000:
        return {'launch': 0.5, 'monitor': 3}
    elif parameter_len < 20_000:
        return {'launch': 1, 'monitor': 5}
    elif parameter_len < 100_000:
        return {'launch': 2, 'monitor': 8}
    else:
        return {'launch': 3, 'monitor': 10}

def get_cpu_config():
    """Auto-detect initial terminals and scaling limits based on CPU."""
    cpu_count = psutil.cpu_count(logical=True)
    initial = max(2, cpu_count)
    max_terminals = cpu_count * 2
    initial = min(initial, max_terminals)
    scale_up_threshold = 70
    scale_up_ceiling = 90
    return {
        'initial': initial,
        'max': max_terminals,
        'scale_up_threshold': scale_up_threshold,
        'scale_up_ceiling': scale_up_ceiling,
        'cpu_count': cpu_count,
    }

def print_heading(title="🗂 Heading"):
    print("\n" + "="*60)
    print(f"{title.center(60)}")
    print("="*60 + "\n")

def set_terminal_title(title: str):
    if sys.platform == 'win32':
        ctypes.windll.kernel32.SetConsoleTitleW(title)
    else:  # Linux, macOS
        sys.stdout.write(f"\33]0;{title}\a")
        sys.stdout.flush()

def fun_timer(seconds):
    seconds = max(1, int(seconds))
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
        base_dir = "P:/PGC Data"
    elif sys.platform == "linux":
        base_dir = os.environ['HOME']
    else:
        print("OS not Defined !!!")
        print("\nPlease select the Pickle folder: ")
        pickle_path = select_folder_gui("Select Pickle Folder")
        return pickle_path
    
    try:
        meta_df = pd.read_csv(meta_data_path)
        meta_indices = set(meta_df['index'].unique())
        
        max_dte = -1
        for col in meta_df.columns:
            if 'dte' in col.lower():
                max_dte = max(max_dte, meta_df[col].astype(float).max())
    except Exception as e:
        print("Error getting Pickle Path")
        print("\nPlease select the Pickle folder: ")
        pickle_path = select_folder_gui("Select Pickle Folder")
        if pickle_path and os.path.exists(pickle_path):
            return pickle_path
    
    indices = NSE_INDICES + BSE_INDICES
    if all(index in indices for index in meta_indices) and max_dte < 7:
        pickle_path = f"{base_dir}/PICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path
        
    indices = NSE_INDICES + BSE_INDICES
    if all(index in indices for index in meta_indices) and max_dte >= 7:
        pickle_path = f"{base_dir}/MPICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path
    
    indices = MCX_INDICES
    if all(index in indices for index in meta_indices):
        pickle_path = f"{base_dir}/MCXPICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path
        
    indices = US_INDICES
    if all(index in indices for index in meta_indices) and max_dte < 7:
        pickle_path = f"{base_dir}/USPICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path
        
    indices = US_INDICES
    if all(index in indices for index in meta_indices) and max_dte >= 7:
        pickle_path = f"{base_dir}/MUSPICKLE/"
        if os.path.exists(pickle_path):
            return pickle_path

    print("Unable to determine Pickle folder !!!")
    print("\nPlease select the Pickle folder: ")
    pickle_path = select_folder_gui("Select Pickle Folder")
    return pickle_path

def is_network_disk(path):
    path = os.path.abspath(path)
    
    if sys.platform == 'win32':
        
        DRIVE_REMOTE = 4
        drive_letter = os.path.splitdrive(path)[0].replace(':', '')
        drive_type = ctypes.windll.kernel32.GetDriveTypeW(f"{drive_letter}:\\")
        return drive_type == DRIVE_REMOTE
    
    else:
        network_fstypes = {'nfs', 'cifs', 'smbfs', 'nfs4', 'sshfs', 'afpfs', 'fuse.sshfs'}
        with open('/proc/mounts', 'r') as f:
            mounts = [line.split() for line in f if len(line.split()) >= 3]

        abspath = os.path.abspath(path)
        parents = []
        while True:
            parents.append(abspath)
            parent = os.path.dirname(abspath)
            if parent == abspath:
                break
            abspath = parent
        for mount in mounts:
            mount_point = os.path.abspath(mount[1])
            fstype = mount[2]
            for p in parents:
                if p == mount_point and fstype in network_fstypes:
                    return True
        return False

def convert_notebook_to_script(pickle_path: Path, notebook_path: Path, parameter_path: Path, meta_data_path: Path):
    
    with open(notebook_path, 'r', encoding='utf-8') as nb_file:
        nb_content = nbformat.read(nb_file, as_version=4)
        
    run_uid = get_run_uid(notebook_path)
    run_scrip_path = Path(tempfile.gettempdir()) / f"{notebook_path.stem}_{run_uid}.py"

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
                    source[idx] = f"meta_data_path = f'{meta_data_path.as_posix()}'"
                    
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
        
    return code, output_csv_path, run_scrip_path

def get_len_of_parameter_data(code, parameter_path):
    _, parameter_len = get_parameter_data(code, parameter_path)
    return parameter_len
        
def modify_script_for_run_code(run_scrip_path, is_remote=False, include_rar=False, dir_pickle_path=None):
    """
    Modify generated .py script:
    1. ALWAYS convert 'if not is_file_exists(...)' → 'with claim_date(...) as claimed:'
    2. Inject dir_cache pickle loader (remote drives)
    3. Add dir_files/include_rar args as needed
    4. Add save_in_rar to save_chunk_data (rar mode)
    """
    
    if is_remote and dir_pickle_path:
        snippet = f"""import pickle\nwith open("{dir_pickle_path.as_posix()}", 'rb') as file:\n    dir_files = pickle.load(file)\n\n"""
        with open(run_scrip_path) as f:
            original = f.read()
        with open(run_scrip_path, "w") as f:
            f.write(snippet + original)
    
    extra_args = []
    if is_remote:
        extra_args.append('dir_files=dir_files')
    if include_rar:
        extra_args.append('include_rar=True')
    args_str = (', ' + ', '.join(extra_args)) if extra_args else ''
    
    pattern_old = re.compile(r"^(?P<indent>\s*)if\s+not\s+is_file_exists\s*\(\s*output_csv_path\s*,\s*file_name\s*,\s*parameter_len\s*\)\s*:\s*$")
    pattern_new = re.compile(r"^(?P<indent>\s*)(with\s+)?claim_date\s*\(\s*output_csv_path\s*,\s*file_name\s*,\s*parameter_len\s*\)")
    pattern_save = re.compile(r"^(?P<indent>\s*)save_chunk_data\s*\(\s*chunk\s*,\s*log_cols\s*,\s*chunck_file_name\s*\)\s*$") if include_rar else None
    
    insert_continue = False
    pending_blank_lines = []
    
    for line in fileinput.input(run_scrip_path, inplace=True):
        
        if insert_continue:
            if line.strip() == '':
                pending_blank_lines.append(line)
                continue
            else:
                body_indent = re.match(r"^(\s*)", line).group(1)
                print(f"{body_indent}if not claimed: continue")
                for bl in pending_blank_lines:
                    print(bl, end="")
                pending_blank_lines = []
                insert_continue = False
        
        m_old = pattern_old.match(line)
        if m_old:
            indent = m_old.group('indent')
            print(f"{indent}with claim_date(output_csv_path, file_name, parameter_len{args_str}) as claimed:")
            insert_continue = True
            continue
        
        if extra_args:
            line = pattern_new.sub(lambda m: f"{m.group('indent')}{m.group(2) or ''}claim_date(output_csv_path, file_name, parameter_len{args_str})", line)
        
        if pattern_save:
            line = pattern_save.sub(lambda m: f"{m.group('indent')}save_chunk_data(chunk, log_cols, chunck_file_name, save_in_rar=True)", line)
        
        print(line, end="")

def get_file_details(meta_data, pickle_path, notebook_path, output_csv_path, code, parameter_len, is_weekly, is_remote, include_rar):

    index_dates = {}
    total_dates, total_pending_dates = 0, 0
    dir_files = set(os.listdir(output_csv_path)) if os.path.exists(output_csv_path) else set()
    run_uid = get_run_uid(notebook_path)
    dir_pickle_path = Path(tempfile.gettempdir()) / f"{notebook_path.stem}_{run_uid}_dir.pkl"
    if include_rar:
        rar_files = set()
        output_dir_name = os.path.dirname(output_csv_path)
        rar_path = f"{output_dir_name}.rar"
        lock_path = f"{rar_path}.lock"

        if os.path.exists(rar_path):
            with FileLock(lock_path):
                while True:
                    try:
                        rar_files = set([f.filename.split("/")[-1] for f in rarfile.RarFile(rar_path).infolist() if f.filename.endswith(".parquet")])
                        break
                    except Exception as e:
                        print(f"RAR read failed ({e}), retrying in 5s...")
                        time.sleep(5)

        dir_files = dir_files.union(rar_files)
    
    if is_remote or include_rar:
        with open(dir_pickle_path, 'wb') as file:
            pickle.dump(dir_files, file)
    
    for row_idx in range(len(meta_data)):
        if meta_data.loc[row_idx, 'run']:
            meta_row = meta_data.iloc[row_idx]

            if not is_weekly:
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                total_dates += len(date_lists)
                index_dates[index] = index_dates.get(index, 0) + len(date_lists)
                files_dates = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len, dir_files, include_rar=include_rar)]
                if files_dates:
                    total_pending_dates += len(files_dates)
            else:
                index, from_dte, to_dte, _, _, _, _, week_lists = get_meta_row_data(meta_row, pickle_path, weekly=True)
                total_dates += len(week_lists)
                index_dates[index] = index_dates.get(index, 0) + len(week_lists)
                files_dates = [week_dates for week_dates in week_lists if not is_file_exists(output_csv_path, f"{index} {week_dates[0].date()} {week_dates[-1].date()} {from_dte}-{to_dte} {code}", parameter_len, dir_files, include_rar=include_rar)]
                if files_dates:
                    total_pending_dates += len(files_dates)

    return index_dates, total_dates, total_pending_dates, dir_pickle_path

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

    code, output_csv_path, run_scrip_path = convert_notebook_to_script(pickle_path, notebook_path, parameter_path, meta_data_path)
    run_uid = get_run_uid(notebook_path)

    parameter_len = get_len_of_parameter_data(code, parameter_path)
    meta_data, _ = get_meta_data(code, meta_data_path)
    no_of_chunk = math.ceil(parameter_len/chunk_size)
    sleep_config = get_sleep_config(parameter_len)
    
    is_weekly = True if ("from_dte" in meta_data.columns) and ("to_dte" in meta_data.columns) else False
    is_remote = True if is_network_disk(output_csv_path) else False
    include_rar = False
    weeks_or_dates = "Weeks" if ("from_dte" in meta_data.columns) and ("to_dte" in meta_data.columns) else "Dates"
    index_dates, total_dates, total_pending_dates, dir_pickle_path = get_file_details(meta_data, pickle_path, notebook_path, output_csv_path, code, parameter_len, is_weekly, is_remote, include_rar)

    modify_script_for_run_code(run_scrip_path, is_remote=is_remote, include_rar=include_rar, dir_pickle_path=dir_pickle_path)

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
    
    cpu_config = get_cpu_config()
    
    print(f"\n--- CPU Auto-Scaling ---")
    print(f"CPU Cores: {cpu_config['cpu_count']}")
    print(f"Initial Terminals: {cpu_config['initial']}")
    print(f"Max Terminals: {cpu_config['max']}")
    print(f"Scale up when CPU < {cpu_config['scale_up_threshold']}%")
    print(f"Stop scaling when CPU > {cpu_config['scale_up_ceiling']}%")

    ### Ask user for initial terminal count (3 second timeout)
    auto_initial = min(cpu_config['initial'], total_pending_dates)
    try:
        user_input = inputimeout(
            prompt=f"\nEnter number of initial terminals (default={auto_initial}, 3s timeout): ",
            timeout=3
        )
        user_count = int(user_input.strip())
        if user_count < 1:
            print("Invalid number, using auto-detect.")
            initial_count = auto_initial
        else:
            initial_count = min(user_count, total_pending_dates)
            print(f"Using user-specified: {initial_count} terminals")
    except TimeoutOccurred:
        print(f"\nNo input received, using auto-detect: {auto_initial} terminals")
        initial_count = auto_initial
    except ValueError:
        print("Invalid input, using auto-detect.")
        initial_count = auto_initial

    ### Get run rows to check if anything to run
    run_rows = [idx for idx in range(len(meta_data)) if meta_data.loc[idx, 'run']]

    ### Helper to launch a terminal
    terminal_counter = [0]
    def launch_terminal():
        label = f"T{terminal_counter[0]}"
        terminal_counter[0] += 1
        print(f"  Launching {label}")
        
        if sys.platform == 'linux':
            pid_file = Path(tempfile.gettempdir()) / f"proc_{code}_{run_uid}_{label}.pid"
            cmd = f"echo $$ > {pid_file}; exec {sys.executable} '{run_scrip_path}'"
            proc = subprocess.Popen(["gnome-terminal", "--", "bash", "-c", cmd], start_new_session=True)
            
            script_pid = None
            for _ in range(15):
                if pid_file.exists():
                    try:
                        content = pid_file.read_text().strip()
                        if content:
                            script_pid = int(content)
                            break
                    except ValueError:
                        pass
                sleep(0.5)
                
            pid_file.unlink(missing_ok=True)
            
            if not script_pid:
                print(f"  Failed to retrieve PID for {label}")
                return None
        else:
            proc = subprocess.Popen([sys.executable, str(run_scrip_path)], creationflags=subprocess.CREATE_NEW_CONSOLE)
            sleep(sleep_config['launch'])
            script_pid = proc.pid
        
        try:
            return psutil.Process(script_pid)
        except psutil.NoSuchProcess:
            print(f'  Process not found for {label}')
            return None

    ### Launch initial terminals — FIX: cap at pending dates
    if not run_rows:
        print("No rows to run!")
        sys.exit()
    
    print(f'\nLaunching {initial_count} terminals ({total_pending_dates} pending)...\n')
    
    processes = []
    for t in range(initial_count):
        result = launch_terminal()
        if result:
            processes.append(result)

    ### Monitor loop with auto-scaling
    scale_check_interval = 60
    last_scale_time = time.time()
    
    while True:
        try:
            output_dir_path = os.path.dirname(output_csv_path)
            if not os.path.exists(output_dir_path):
                raise FileNotFoundError(f"Output Directory {output_dir_path} not available")            
            
            fun_timer(sleep_config['monitor'])
            index_dates, total_dates, total_pending_dates, _ = get_file_details(meta_data, pickle_path, notebook_path, output_csv_path, code, parameter_len, is_weekly, is_remote, include_rar)

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

            ### Restart dead terminals
            new_processes = []
            dead_count = 0
            running_count = 0
            max_needed = min(cpu_config['max'], total_pending_dates)
            
            for proc in processes:
                if proc.is_running():
                    new_processes.append(proc)
                    running_count += 1
                else:
                    dead_count += 1
                    if running_count < max_needed and os.path.exists(output_dir_path):
                        result = launch_terminal()
                        if result:
                            new_processes.append(result)
                            running_count += 1

            processes = new_processes
            
            ### Auto-scale
            now = time.time()
            if now - last_scale_time >= scale_check_interval and total_pending_dates > 0:
                last_scale_time = now
                cpu_percent = psutil.cpu_percent(interval=2)
                
                running_count = len([1 for p in processes if p.is_running()])
                max_needed = min(cpu_config['max'], total_pending_dates)
                
                if running_count < max_needed and cpu_percent < cpu_config['scale_up_threshold']:
                    print(f"\n>>> CPU at {cpu_percent:.0f}% < {cpu_config['scale_up_threshold']}% — scaling up ({running_count} → {running_count + 1}, {total_pending_dates} pending)")
                    result = launch_terminal()
                    if result:
                        processes.append(result)
                
                elif cpu_percent >= cpu_config['scale_up_ceiling']:
                    print(f"\n>>> CPU at {cpu_percent:.0f}% ≥ {cpu_config['scale_up_ceiling']}% — holding at {running_count} terminals")

            running_count = len([1 for p in processes if p.is_running()])
            max_needed = min(cpu_config['max'], total_pending_dates)
            print(f"\nRunning: {running_count} / {max_needed} terminals ({total_pending_dates} pending)")
            if dead_count:
                print(f"Restarted: {dead_count} terminals")

        except Exception as e:
            print(f"Error occurred: {e}, retrying in 5s...")
            time.sleep(5)