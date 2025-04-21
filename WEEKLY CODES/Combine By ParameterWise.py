import os
import gc
import sys
import pandas as pd
import polars as pl
from glob import glob
from time import sleep
import concurrent.futures
import dask.dataframe as dd
os.system(f'title Combine By ParameterWise')

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
only_mtm_col = get_bool_input("Grouping with MTM columns only?")
use_polars = get_bool_input("Use Polars (fastest) instead of (Pandas/Dask)?")
files_folder_path = f"{code}/{code}_output/"
combine_folder_path = f"{code}/{code}_output_ParameterWise/"
os.makedirs(combine_folder_path, exist_ok=True)

EXT = "*.parquet"
all_files = [file for path, subdir, files in os.walk(files_folder_path) for file in glob(os.path.join(path, EXT))]
indices = sorted(set([f.split('\\')[-1].split(' ')[0] for f in all_files]))
os.makedirs(combine_folder_path, exist_ok=True)

print()
print('Code -', code)
print('Only MTM Col-', only_mtm_col)
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
if only_mtm_col: print('\nPNL cols-', pnl_columns)

for index in indices:
    chunk_nos = sorted(set([k.split('-')[-1].split('.')[0] for k in glob(f'{files_folder_path}/{index}*')]))
    for chunk_no in chunk_nos:
        try:
            all_files = glob(f'{files_folder_path}/{index}*No-{chunk_no}.parquet')
            print(f'\nTotal file in {index} Chunks-{chunk_no} -', len(all_files))
            if len(all_files) == 0:continue

            print('Reading Chunks...')
            if use_polars:
                df = pl.read_parquet(all_files, columns=(name_columns+pnl_columns) if only_mtm_col else None, use_pyarrow=True)
            else:
                df = dd.read_parquet(all_files, columns=(name_columns+pnl_columns) if only_mtm_col else None)
                df = df.compute()
            print('Reading Complete...')

            os.makedirs(f"{combine_folder_path}{index}", exist_ok=True)
            print('Grouping Chunks...')
            if use_polars:
                grouped = df.group_by(name_columns)
            else:
                grouped = df.groupby(name_columns)
            print('Grouping Complete...')

            def save_file(idx, data):
                try:
                    file_name = ' '.join(map(str, idx)).replace(":00 ", " ").replace(":", "") + '.parquet'
                    if use_polars:
                        data.write_parquet(f"{combine_folder_path}{index}/{file_name}")
                    else:
                        data.to_parquet(f"{combine_folder_path}{index}/{file_name}", index=False)
                except Exception as e:
                    print(e)

            print(f"🧩 {index} - Chunk {chunk_no}: Saving grouped files...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()*7) as executor:
                for idx, data in grouped:
                    executor.submit(save_file, idx, data)
                    
            del df, grouped
            sleep(1)
            gc.collect()
            sleep(4)
            
        except Exception as e:
            input(f"ERROR !!! {e}")