import os
import sys
import time
import shutil
import datetime
import numpy as np
import polars as pl
import pandas as pd
from tqdm import tqdm
from time import sleep
from pathlib import Path
import concurrent.futures
import pyarrow.parquet as pq
from tkinter import Tk, filedialog

from pgcbacktest.BtParameters import *
from pgcbacktest.BacktestOptions import *

pl.enable_string_cache()

def print_heading(title="🗂 Folder Selection & Configuration"):
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

def select_folder_gui(title="Select a Folder", initialdir=".") -> Path | None:
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True) 
    folder_path = filedialog.askdirectory(title=title, initialdir=initialdir, parent=root)
    root.destroy() 
    return Path(folder_path) if folder_path else None

def select_file_gui(title="Select a File", filetypes=None, initialdir=".") -> Path | None:
    if filetypes is None:
        filetypes = [("All files", "*.*")]

    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    file_path = filedialog.askopenfilename(title=title, filetypes=filetypes, initialdir=initialdir, parent=root)
    root.destroy()
    return Path(file_path) if file_path else None

def get_dte_csv_path():
    
    if sys.platform == "win32":
        base_dir = "P:/PGC Data"
    elif sys.platform == "linux":
        base_dir = os.environ['HOME'] # "/home/user"
    else:
        print("OS not Defined !!!")
        print("\nSelect DTE CSV File: ")
        dte_csv_path = select_file_gui(title="Select DTE CSV File", filetypes=[("CSV files", "*.csv")])
        return dte_csv_path
    
    pickle_paths = {
        "1": f"{base_dir}/PICKLE",
        "2": f"{base_dir}/MPICKLE",
        "3": f"{base_dir}/MCXPICKLE",
        "4": f"{base_dir}/USPICKLE",
        "5": f"{base_dir}/MUSPICKLE",
    }

    print("\nChoose the DTE file directory:")
    for key, value in pickle_paths.items():
        print(f"  {key}: {Path(value).name}")
    print("  6: Other (Select DTE file manually)")

    choice = input("Enter your choice (1-6): ").strip()

    if choice in pickle_paths:
        dte_csv_path = Path(pickle_paths[choice]) / "DTE.csv"
        if dte_csv_path.exists():
            return dte_csv_path
        else:
            print(f"DTE.csv not found in {pickle_paths[choice]}. Falling back to manual selection.")
    elif choice == '6':
        pass  # Proceed to manual selection
    else:
        print("Invalid choice. Falling back to manual selection.")
    
    print("\nSelect DTE CSV File: ")
    dte_csv_path = select_file_gui(title="Select DTE CSV File", filetypes=[("CSV files", "*.csv")])
    return dte_csv_path
    
def get_bool_input(prompt):
    return input(f"{prompt} (y/n): ").strip().lower() == 'y'

def get_parquet_files(folder_path):
    root = Path(folder_path).expanduser().resolve()
    iterator = root.rglob("*.parquet")
    return sorted(iterator)

def get_code_index_cols(parquet_files):

    parquet_file_path = max(parquet_files, key=lambda f: os.path.getsize(f))
    df = pd.read_parquet(parquet_file_path)
    splits = parquet_file_path.stem.split()

    if len(splits) == 4: # Intraday
        code_type = "Intraday"
        code = parquet_files[0].stem.split()[2]
    elif len(splits) == 6: # Weekly
        code_type = "Weekly"
        code = parquet_files[0].stem.split()[4]
    
    indices = sorted(set([f.stem.split()[0] for f in parquet_files]))
    name_columns = [c for c in list(df.columns) if c.startswith('P_')]
    pnl_columns = [c for c in list(df.columns) if c.endswith('PNL')]
    return code_type, code, indices, name_columns, pnl_columns

def check_parquet_file(file):
    try:
        table = pq.read_table(file)
        return None
    except Exception as e:
        return (f"Invalid file: {file} | Error: {e}")

def grouping_parquet_files(parquet_files, dte_file, code_type="Intraday"):

    if code_type == "Intraday":

        year_day_dte_files = {}
        for file in parquet_files:
            index = file.stem.split()[0]
            date = datetime.datetime.strptime(file.stem.split()[1], "%Y-%m-%d")
            year = date.year
            day = date.strftime('%A')
            dte = dte_file.loc[date, index]
            year_day_dte_files[f'{index}-{year}-{day}-{dte}'] = year_day_dte_files.get(f'{index}-{year}-{day}-{dte}', []) + [file]

        return year_day_dte_files

    elif code_type == "Weekly":

        year_dte_files = {}
        for file in parquet_files:
            index = file.stem.split()[0]
            date = datetime.datetime.strptime(file.stem.split()[1], "%Y-%m-%d")
            year = date.year
            dte = file.stem.split()[3].replace('-', '_')
            year_dte_files[f'{index}-{year}-{dte}'] = year_dte_files.get(f'{index}-{year}-{dte}', []) + [file]

        return year_dte_files
    
    else:
        raise ValueError("Invalid code_type. Must be 'Intraday' or 'Weekly'.")

if __name__ == "__main__":
    
    # Set terminal title    
    title = "DashBoard FileMaker"
    set_terminal_title(title)
    print_heading(title)
        
    # select folder containing Parquet files
    print("\nSelect Output Folder: ", end="")
    parquet_files_folder_path = select_folder_gui(title="Select Folder containing Parquet files")
    
    if parquet_files_folder_path:
        
        print(parquet_files_folder_path)
        # Get all Parquet files in the selected folder
        parquet_files = get_parquet_files(parquet_files_folder_path)
        
        if parquet_files:
            code_type, code, indices, name_columns, pnl_columns = get_code_index_cols(parquet_files)
            print()
            print(f"Total File Uploaded :- {len(parquet_files)}")
            print(f"Code Type :- {code_type}")
            print(f"Code :- {code}")
            print(f"Indices :- {indices}")
            print(f"Parameter cols :- {', '.join(name_columns)}")
            print(f"PNL cols :- {', '.join(pnl_columns)}")
        else:
            print("No Parquet files found in the provided folder path.")
            input("\nPress Enter to Exit !!!")
            sys.exit(0)
        
    else:
        print("No folder selected. :(")
        input("Press Enter to Exit !!!")
        sys.exit(0)
        
    # select DTE CSV file
    dte_csv_path = get_dte_csv_path()
    if dte_csv_path:
        print(f"\nDTE CSV Path: {dte_csv_path}")
        # Read DTE CSV file
        dte_file = pd.read_csv(dte_csv_path, parse_dates=['Date'], dayfirst=True).set_index("Date")
    else:
        print("No DTE CSV file selected. :(")
        input("Press Enter to Exit...")
        sys.exit(0)

    ### checking parquet files
    print("\nChecking Parquet Files...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(check_parquet_file, parquet_files), total=len(parquet_files), desc="Checking Parquet Files"))

    errors = [r for r in results if r]
    error_files = [parquet_files[i] for i, r in enumerate(results) if r]

    if errors:
        for err in errors:
            print(err)
        if input("Delete all error files? (y/n): ").strip().lower() == 'y':
            for f in error_files:
                try:
                    os.remove(f)
                    print(f"Deleted: {f}")
                except Exception as e:
                    print(f"Failed to delete {f}: {e}")
        input("Press Enter to Exit !!!")
        sys.exit(0)
    print("All Parquet files are valid.\n")
    
    if input("Proceed with execution? (y/n): ").strip().lower() != 'y':
        print('❌ Execution cancelled.')
        sleep(2)
        sys.exit(0)
        
    max_row = 500000
    dashboard_folder_path = parquet_files_folder_path.parent / f"{code}_dashboard"
    print(f"\nDashBoard Files Folder Path: {dashboard_folder_path}\n")
    
    shutil.rmtree(dashboard_folder_path, ignore_errors=True)
    os.makedirs(dashboard_folder_path, exist_ok=True)
    grouped_parquet = grouping_parquet_files(parquet_files, dte_file, code_type=code_type)
    
    t1 = time.time()

    print('\nBuilding DashBoard Files... \n')
    for index in indices:
        try:
            os.makedirs(f"{dashboard_folder_path}/{index}", exist_ok=True)

            for key, value in grouped_parquet.items():

                if code_type == 'Intraday':
                    check_index, year, day, dte = key.split('-')
                elif code_type == 'Weekly':
                    check_index, year, dte = key.split('-')

                if check_index != index: continue

                dashboard_data_list = []
                chunks = sorted(set([f.stem.split()[-1] for f in grouped_parquet[key]]), key=lambda x: int(x.split('-')[-1]))
                for chunk in chunks:
                    
                    if code_type == 'Intraday':
                        print(index, year, day, dte, chunk)
                    elif code_type == 'Weekly':
                        print(index, year, dte, chunk)

                    chunks_file = [f for f in grouped_parquet[key] if f.stem.split()[-1] == chunk]

                    def read_and_cast(path):
                        df = pl.read_parquet(path, columns = (name_columns+pnl_columns))
                        return df.with_columns([pl.col(name_columns).cast(pl.Utf8).cast(pl.Categorical), pl.col(pnl_columns).cast(pl.Float64)])

                    with concurrent.futures.ThreadPoolExecutor() as exe:
                        dfs = list(exe.map(read_and_cast, chunks_file))

                    data = pl.concat(dfs)
                    data = data.group_by(name_columns).agg([pl.col(col).sum() for col in pnl_columns])
                    data = data.unpivot(index=name_columns, on=pnl_columns, variable_name='PL Basis', value_name='Points')
                    data.columns = [c.replace('P_','', 1) if c.startswith('P_') else c for c in data.columns]
                
                    data = data.with_columns([
                        pl.col("PL Basis").cast(pl.Categorical).alias("PL Basis")
                    ])

                    if code_type == 'Intraday':
                        data = data.with_columns([
                            pl.lit(int(year)).cast(pl.Int16).alias("Year"),
                            pl.lit(day).cast(pl.Categorical).alias("Day"),
                            pl.lit(int(float(dte))).cast(pl.Int8).alias("DTE")
                        ])
                    elif code_type == 'Weekly':
                        data = data.with_columns([
                            pl.lit(int(year)).cast(pl.Int16).alias("Year"),
                            pl.lit(dte).cast(pl.Categorical).alias("Start.DTE-End.DTE")
                        ])
                        
                    dashboard_data_list.append(data)
                    
                if not dashboard_data_list:
                    continue

                dashboard_data = pl.concat(dashboard_data_list, how="vertical")

                for pnl_col in pnl_columns:
                    pnl_data = dashboard_data.filter(pl.col("PL Basis") == pnl_col)
                    chunk_size = max_row
                    for idx, i in enumerate(range(0, len(pnl_data), chunk_size), start=1):
                        chunk_data = pnl_data.slice(i, chunk_size)
                        if code_type == 'Intraday':
                            chunk_data.write_csv(f"{dashboard_folder_path}/{index}/{code}-{year}-{day}-{dte}-{pnl_col}-No-{idx}.csv")
                        elif code_type == 'Weekly':
                            chunk_data.write_csv(f"{dashboard_folder_path}/{index}/{code}-{year}-{dte}-{pnl_col}-No-{idx}.csv")

        except Exception as e:
            input(f"ERROR !!! {e}")
        
    print("Done\n")
    t2 = time.time()
    minutes, seconds = divmod(t2 - t1, 60)
    print(f"\nTotal Time Taken: {int(minutes)} minutes and {round(seconds, 2)} seconds.\n")
    input("Press Enter to Exit !!!")