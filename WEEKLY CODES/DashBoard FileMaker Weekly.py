import os
import gc
import sys
import datetime
import numpy as np
import pandas as pd
import polars as pl
from glob import glob
from time import sleep
import concurrent.futures
import dask.dataframe as dd

pl.enable_string_cache()

os.system(f'title DashBoard FileMaker')

def print_heading(title="🗂 Folder Selection & Configuration"):
    print("\n" + "="*60)
    print(f"{title.center(60)}")
    print("="*60 + "\n")

print_heading()

def list_and_select_directory(current_path='.'):
    while True:
        # List directories, excluding .ipynb
        folders = [f for f in os.listdir(current_path) if os.path.isdir(os.path.join(current_path, f)) and '.ipynb' not in f and 'dashboard' not in f]
        
        # If no folders, return current path
        if not folders:
            return current_path

        # Display folders
        for i, f in enumerate(folders, 1):
            print(f"{i}. {f}")
        
        # Get user selection
        while True:
            try:
                idx = int(input("Pls Select Code: "))
                if 1 <= idx <= len(folders):
                    selected_folder = folders[idx - 1]
                    print(f"\nSelected Code: {selected_folder}")
                    # Update path and continue
                    current_path = os.path.join(current_path, selected_folder)
                    break
            except ValueError:
                pass
            print("Invalid input. Try again.")
        
def get_bool_input(prompt):
    return input(f"{prompt} (y/n): ").strip().lower() == 'y'

def get_parquet_files(folder_path):
    EXT = "*.parquet"
    return [file for path, subdir, files in os.walk(folder_path) for file in glob(os.path.join(path, EXT))]

def get_code_index_cols(parquet_files):
    code = parquet_files[0].split('\\')[-1].split(' ')[2]
    indices = sorted(set([f.split('\\')[-1].split(' ')[0] for f in parquet_files]))
    
    df = pd.read_parquet(max(parquet_files, key=lambda f: os.path.getsize(f)))
    name_columns = [c for c in list(df.columns) if c.startswith('P_')]
    pnl_columns = [c for c in list(df.columns) if c.endswith('PNL')]
    return code, indices, name_columns, pnl_columns

def get_year_dte_files(parquet_files):
    year_dte_files = {}
    for file in all_files:
        index = file.split('\\')[-1].split(' ')[0]
        date = datetime.datetime.strptime(file.split('\\')[-1].split(' ')[1], "%Y-%m-%d")
        year = date.year
        dte = file.split('\\')[-1].split(' ')[3].replace('-', '_')
        year_dte_files[f'{index}-{year}-{dte}'] = year_dte_files.get(f'{index}-{year}-{dte}', []) + [file]

    return year_dte_files

parquet_files_folder_path = list_and_select_directory()
use_polars = get_bool_input("Use Polars (fastest) instead of (Pandas/Dask)?")

parquet_files = get_parquet_files(parquet_files_folder_path)
if parquet_files:
    code, indices, name_columns, pnl_columns = get_code_index_cols(parquet_files)
    print()
    print(f"Total File Uploaded :- {len(parquet_files)}")
    print(f"Code :- {code}")
    print(f"Indices :- {indices}")
    print(f"Parameter cols :- {', '.join(name_columns)}")
    print(f"PNL cols :- {', '.join(pnl_columns)}")
    print('Use Polars :-', use_polars)
else:
    print("No Parquet files found in the provided folder path.")
    input("\nPress Enter to Exit !!!")

max_row = 500000
dashboard_folder_path = parquet_files_folder_path.replace('_output', '_dashboard')
dte_file = pd.read_csv(f"C:/PICKLE/DTE.csv", parse_dates=['Date'], dayfirst=True).set_index("Date")
os.makedirs(dashboard_folder_path, exist_ok=True)
year_day_dte_files = get_year_dte_files(parquet_files)

if input("Proceed with execution? (y/n): ").strip().lower() != 'y':
    print('❌ Execution cancelled.')
    sleep(2)
    sys.exit(0)

print()
print('Building DashBoard Files... \n')
for index in indices:
    try:
        os.makedirs(f"{dashboard_folder_path}/{index}", exist_ok=True)

        for key, value in year_dte_files.items():
            check_index, year, dte = key.split('-')
            if check_index != index: continue

            dashboard_data_list = []
            chunks = sorted(set([f.split(' ')[-1].split('.')[0] for f in year_dte_files[key]]), key=lambda x: int(x.split('-')[-1]))
            for chunk in chunks:

                print(index, year, dte, chunk)

                chunks_file = [f for f in year_dte_files[key] if f"{chunk}." in f]

                if use_polars:
                    def read_and_cast(path):
                        df = pl.read_parquet(path, columns = (name_columns+pnl_columns))
                        return df.with_columns([pl.col(name_columns).cast(pl.Utf8).cast(pl.Categorical), pl.col(pnl_columns) .cast(pl.Float64)])

                    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as exe:
                        dfs = list(exe.map(read_and_cast, chunks_file))

                    data = pl.concat(dfs)
                    data = data.group_by(name_columns).agg([pl.col(col).sum() for col in pnl_columns])
                    data = data.unpivot(index=name_columns, on=pnl_columns, variable_name='PL Basis', value_name='Points')
                    data.columns = [c.replace('P_','') for c in data.columns]

                    data = data.with_columns([
                        pl.lit(int(year)).cast(pl.Int16).alias("Year"),
                        pl.lit(dte).cast(pl.Categorical).alias("Start.DTE-End.DTE")
                    ])
                else:
                    data = dd.read_parquet(chunks_file, columns=(name_columns+pnl_columns))
                    data = data.compute()
                    data = data.groupby(name_columns).sum(numeric_only=True)[pnl_columns].reset_index()
                    data = data.melt(id_vars=name_columns, value_vars=pnl_columns, var_name='PL Basis', value_name='Points')
                    data.columns = [c.replace('P_','') for c in data.columns]
                    data[['Year', 'Start.DTE-End.DTE']] = np.int16(year), str(dte)
                dashboard_data_list.append(data)

            if use_polars:
                dashboard_data = pl.concat(dashboard_data_list, how="vertical")

                for pnl_col in pnl_columns:
                    pnl_data = dashboard_data.filter(pl.col("PL Basis") == pnl_col)
                    chunk_size = max_row
                    for idx, i in enumerate(range(0, len(pnl_data), chunk_size), start=1):
                        chunk = pnl_data.slice(i, chunk_size)
                        chunk.write_csv(f"{dashboard_folder_path}/{index}/{code}-{year}-{dte}-{pnl_col}-No-{idx}.csv")
            else:
                dashboard_data = pd.concat(dashboard_data_list, ignore_index=True)
                dashboard_data[dashboard_data.select_dtypes(include=['object']).columns] = dashboard_data.select_dtypes(include=['object']).astype('category')

                for pnl_col in pnl_columns:
                    pnl_data = dashboard_data[dashboard_data['PL Basis'] == pnl_col]
                    chunk_size = max_row
                    for idx, i in enumerate(range(0, len(pnl_data), chunk_size), start=1):
                        chunk = pnl_data.iloc[i:i + chunk_size]
                        chunk.to_csv(f"{dashboard_folder_path}/{index}/{code}-{year}-{dte}-{pnl_col}-No-{idx}.csv", index=False)

            del dashboard_data
            del dashboard_data_list
            sleep(1)
            gc.collect()
            sleep(3)

    except Exception as e:
        input(f"ERROR !!! {e}")
        
print("Done\n")
input("Press Enter to Exit !!!")