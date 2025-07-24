import sys
import math
import psutil
import nbformat
import tempfile
import subprocess
import pandas as pd
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
    return Path(folder_path) if folder_path else None

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
    temp_meta_data_path = meta_data_path

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