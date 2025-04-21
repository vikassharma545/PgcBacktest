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

os.system(f'title DashBoard FileMaker')

def print_heading(title="🗂 Folder Selection & Configuration"):
    print("\n" + "="*60)
    print(f"{title.center(60)}")
    print("="*60 + "\n")

print_heading()

def list_and_select_directory():
    folders = [f for f in os.listdir('.') if os.path.isdir(f) and '.ipynb' not in f]
    if not folders:
        return print("No folders found.")

    for i, f in enumerate(folders, 1):
        print(f"{i}. {f}")
    
    while True:
        try:
            idx = int(input("Pls Select Code: "))
            if 1 <= idx <= len(folders):
                print(f"\nSelected Code : {folders[idx - 1]}")
                return folders[idx - 1]
        except ValueError:
            pass
        print("Invalid input. Try again.")
        
def get_bool_input(prompt):
    return input(f"{prompt} (y/n): ").strip().lower() == 'y'

code = list_and_select_directory()
use_polars = get_bool_input("Use Polars (fastest) instead of (Pandas/Dask)?")

max_row = 500000
files_folder_path = f"{code}/{code}_output/"
combine_folder_path = f"{code}/{code}_Dashboard/"
dte_file = pd.read_csv(f"C:/PICKLE/DTE.csv", parse_dates=['Date'], dayfirst=True).set_index("Date")
os.makedirs(combine_folder_path, exist_ok=True)

EXT = "*.parquet"
all_files = [file for path, subdir, files in os.walk(files_folder_path) for file in glob(os.path.join(path, EXT))]
indices = sorted(set([f.split('\\')[-1].split(' ')[0] for f in all_files]))

print()
print('Code -', code)
print('Use Polars-', use_polars)
print('Number of Files -', len(all_files))
print('Indices -', indices)
print()

if input("Proceed with execution? (y/n): ").strip().lower() != 'y':
    print('❌ Execution cancelled.')
    sleep(2)
    sys.exit(0)

df = pd.read_parquet(max(all_files, key=lambda f: os.path.getsize(f)))
name_columns = [c for c in list(df.columns) if c.startswith('P_')]
pnl_columns = [c for c in list(df.columns) if c.endswith('PNL')]
print('Name cols-', list(map(lambda x: x.replace('P_', ''), name_columns)))
print()
print('PNL cols-', pnl_columns)

year_day_dte_files = {}
for file in all_files:
    index = file.split('\\')[-1].split(' ')[0]
    date = datetime.datetime.strptime(file.split('\\')[-1].split(' ')[1], "%Y-%m-%d")
    year = date.year
    day = date.strftime('%A')
    dte = dte_file.loc[date, index]
    year_day_dte_files[f'{index}-{year}-{day}-{dte}'] = year_day_dte_files.get(f'{index}-{year}-{day}-{dte}', []) + [file]

print()
print('Building DashBoard Files... \n')
for index in indices:
    try:
        os.makedirs(f"{combine_folder_path}{index}", exist_ok=True)

        for key, value in year_day_dte_files.items():
            check_index, year, day, dte = key.split('-')
            if check_index != index: continue

            dashboard_data_list = []
            chunks = sorted(set([f.split(' ')[-1].split('.')[0] for f in year_day_dte_files[key]]), key=lambda x: int(x.split('-')[-1]))
            for chunk in chunks:

                print(index, year, day, dte, chunk)

                chunks_file = [f for f in year_day_dte_files[key] if f"{chunk}." in f]

                if use_polars:
                    data = pl.read_parquet(chunks_file, columns=(name_columns+pnl_columns), use_pyarrow=True)
                    data = data.group_by(name_columns).agg([pl.col(col).sum() for col in pnl_columns])
                    data = data.unpivot(index=name_columns, on=pnl_columns, variable_name='PL Basis', value_name='Points')
                    data.columns = [c.replace('P_','') for c in data.columns]

                    data = data.with_columns([
                        pl.lit(int(year)).cast(pl.Int16).alias("Year"),
                        pl.lit(day).cast(pl.Categorical).alias("Day"),
                        pl.lit(int(float(dte))).cast(pl.Int8).alias("DTE")
                    ])
                else:
                    data = dd.read_parquet(chunks_file, columns=(name_columns+pnl_columns))
                    data = data.compute()
                    data = data.groupby(name_columns).sum(numeric_only=True)[pnl_columns].reset_index()
                    data = data.melt(id_vars=name_columns, value_vars=pnl_columns, var_name='PL Basis', value_name='Points')
                    data.columns = [c.replace('P_','') for c in data.columns]
                    data[['Year', 'Day', 'DTE']] = np.int16(year), day, np.int8(float(dte))
                dashboard_data_list.append(data)

            if use_polars:
                dashboard_data = pl.concat(dashboard_data_list, how="vertical")

                for pnl_col in pnl_columns:
                    pnl_data = dashboard_data.filter(pl.col("PL Basis") == pnl_col)
                    chunk_size = max_row
                    for idx, i in enumerate(range(0, len(pnl_data), chunk_size), start=1):
                        chunk = pnl_data.slice(i, chunk_size)
                        chunk.write_csv(f"{combine_folder_path}{index}/{code}-{year}-{day}-{dte}-{pnl_col}-No-{idx}.csv")
            else:
                dashboard_data = pd.concat(dashboard_data_list, ignore_index=True)
                dashboard_data[dashboard_data.select_dtypes(include=['object']).columns] = dashboard_data.select_dtypes(include=['object']).astype('category')

                for pnl_col in pnl_columns:
                    pnl_data = dashboard_data[dashboard_data['PL Basis'] == pnl_col]
                    chunk_size = max_row
                    for idx, i in enumerate(range(0, len(pnl_data), chunk_size), start=1):
                        chunk = pnl_data.iloc[i:i + chunk_size]
                        chunk.to_csv(f"{combine_folder_path}{index}/{code}-{year}-{day}-{dte}-{pnl_col}-No-{idx}.csv", index=False)

            del dashboard_data
            del dashboard_data_list
            sleep(1)
            gc.collect()
            sleep(3)

    except Exception as e:
        input(f"ERROR !!! {e}")
        
print("Done\n")
input("Press Enter to Exit !!!")