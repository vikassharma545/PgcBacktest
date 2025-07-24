import sys
import math
import psutil
import nbformat
import tempfile
import subprocess
from time import sleep
from pathlib import Path
from tkinter import Tk, filedialog
from nbconvert import PythonExporter

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

def select_folder_gui(title="Select a Folder") -> Path | None:
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True) 
    folder_path = filedialog.askdirectory(title=title, parent=root)
    root.destroy() 
    return Path(folder_path).as_posix() + "/" if folder_path else None

def select_file_gui(title="Select a File", filetypes=None) -> Path | None:
    if filetypes is None:
        filetypes = [("All files", "*.*")]

    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)  # Ensure the dialog appears on top
    file_path = filedialog.askopenfilename(title=title, filetypes=filetypes, parent=root)
    root.destroy()
    return Path(file_path) if file_path else None

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

def get_file_details(meta_data, pickle_path, output_csv_path, code, parameter_len):
    
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
                
    return index_dates, index_dte_dates, total_dates, total_pending_dates

def create_temp_meta_data(index_dte_dates, meta_data, temp_meta_data_path, no_of_terminal_allowed):
    
    print(f'Creating MetaData...')
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
        temp_meta_data.to_csv(temp_meta_data_path, index=False)
    else:
        meta_data.to_csv(temp_meta_data_path, index=False)

    print(f'MetaData Created: {temp_meta_data_path}')
    
    return no_of_terminal_allowed

if __name__ == "__main__":
    
    # Set terminal title    
    title = "Run Code"
    set_terminal_title(title)
    print_heading(title)
    
    # select Pickle folder
    print("\nPlease select the Pickle folder:  ", end="")
    pickle_path = select_folder_gui("Select Pickle Folder")
    if pickle_path:
        print(pickle_path)
    else:
        print("No Pickle folder selected. Exiting...")
        sys.exit(1)
    
    # select jupyter notebook file
    print("\nPlease select the Jupyter Notebook file to run:  ", end="")
    notebook_path = select_file_gui("Select Jupyter Notebook File", [("Jupyter Notebook", "*.ipynb")])
    if notebook_path:
        print(notebook_path.stem)
    else:
        print("No Jupyter Notebook file selected. Exiting...")
        sys.exit(1)
        
    # select Parameters file
    print("\nPlease select the Parameters file:  ", end="")
    parameter_path = select_file_gui("Select Parameters File", [("Parameters File", "*.csv")])
    if parameter_path:
        print(parameter_path.stem)
    else:
        print("No Parameters file selected. Exiting...")
        sys.exit(1)
        
    # select Metadata file
    print("\nPlease select the Metadata file:  ", end="")
    meta_data_path = select_file_gui("Select Metadata File", [("Metadata File", "*.csv")])
    if meta_data_path:
        print(meta_data_path.stem)
    else:
        print("No Metadata file selected. Exiting...")
        sys.exit(1)

    code, output_csv_path, temp_meta_data_path, run_scrip_path = convert_notebook_to_script(pickle_path, notebook_path, parameter_path, meta_data_path)
    
    from pgcbacktest.BtParameters import *
    from pgcbacktest.BacktestOptions import *

    _, parameter_len = get_parameter_data(code, parameter_path)
    meta_data, _ = get_meta_data(code, meta_data_path)
    no_of_chunk = math.ceil(parameter_len/chunk_size)

    index_dates, index_dte_dates, total_dates, total_pending_dates = get_file_details(meta_data, pickle_path, output_csv_path, code, parameter_len)

    file_details = f'\n####### OUTPUT FILES #######\
                    \nNo of Chunks: {no_of_chunk} \
                    \nTotals Dates: {total_dates} \
                    \nTotal Files Created: {no_of_chunk*total_dates}\
                    \nDates IndexWise: {index_dates} \
                    \n############################'
    print(file_details)
    
    print(f'\nTotal Pending Dates: {total_pending_dates}')
    if total_pending_dates == 0:
        print('\nNo Pending Dates Left all Dates files Complete :)')
        sleep(5)
        sys.exit()

    no_of_terminal_allowed = int(input("Enter the number of terminals allowed: "))
    no_of_terminal_allowed = create_temp_meta_data(index_dte_dates, meta_data, temp_meta_data_path, no_of_terminal_allowed)

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
        
        code_details = f'\n#### RUN CODE ####\nCode: {code}\nJupyterCode: {notebook_path.stem} \nParameter: {parameter_path.stem} \nMetaData Parameter: {meta_data_path.stem} \n##################'
        print(code_details)

        index_dates, index_dte_dates, total_dates, total_pending_dates = get_file_details(meta_data, pickle_path, output_csv_path, code, parameter_len)

        file_details = f'\n####### OUTPUT FILES #######\
                        \nNo of Chunks: {no_of_chunk} \
                        \nTotals Dates: {total_dates} \
                        \nTotal Files Created: {no_of_chunk*total_dates}\
                        \nDates IndexWise: {index_dates} \
                        \n############################'

        print(file_details)
        print(f'\nTotal Pending Dates: {total_pending_dates}')
        if total_pending_dates == 0:
            print('\nNo Pending Dates Left all Dates files Complete :)')
            sleep(5)
            sys.exit()

        ### checking script is running
        new_processes = []
        df = pd.read_csv(temp_meta_data_path)
        for idx, proc in processes:
            if not proc.is_running():

                meta_row = df.loc[int(idx)]
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, pickle_path)
                pending_files = [current_date.date() for current_date in date_lists if not is_file_exists(output_csv_path, f"{index} {current_date.date()} {code}", parameter_len)]

                if pending_files:

                    print(f"found closed process in Row {idx}.")
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
        print(f"Allowed Processes: {no_of_terminal_allowed}")

        if len(processes) < int(no_of_terminal_allowed*0.7) and no_of_terminal_allowed != -1:

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

            no_of_terminal_allowed = create_temp_meta_data(index_dte_dates, meta_data, temp_meta_data_path, no_of_terminal_allowed)

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

        else:
            sleep(5)
            os.system('clear') if sys.platform == 'linux' else os.system('cls')