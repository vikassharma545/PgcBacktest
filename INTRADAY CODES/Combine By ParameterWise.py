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

parquet_files_folder_path = list_and_select_directory()
only_mtm_col = get_bool_input("Grouping with MTM columns only?")
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

combine_folder_path = parquet_files_folder_path.replace('_output', '_output_ParameterWise')
os.makedirs(combine_folder_path, exist_ok=True)

if input("Proceed with execution? (y/n): ").strip().lower() != 'y':
    print('❌ Execution cancelled.')
    sleep(2)
    sys.exit(0)

print()
print('Building ParameterWise Files... \n')

for index in indices:
    chunk_nos = sorted(set([k.split('-')[-1].split('.')[0] for k in glob(f'{parquet_files_folder_path}/{index}*')]))
    for chunk_no in chunk_nos:
        try:
            all_files = glob(f'{parquet_files_folder_path}/{index}*No-{chunk_no}.parquet')
            print(f'\nTotal file in {index} Chunks-{chunk_no} -', len(all_files))
            if len(all_files) == 0:continue

            print('Reading Chunks...')
            if use_polars:
                df = pl.read_parquet(all_files, columns=(name_columns+pnl_columns) if only_mtm_col else None, use_pyarrow=True)
            else:
                df = dd.read_parquet(all_files, columns=(name_columns+pnl_columns) if only_mtm_col else None)
                df = df.compute()
            print('Reading Complete...')

            os.makedirs(f"{combine_folder_path}/{index}", exist_ok=True)
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
                        data.write_parquet(f"{combine_folder_path}/{index}/{file_name}")
                    else:
                        data.to_parquet(f"{combine_folder_path}/{index}/{file_name}", index=False)
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
            
print("Done\n")
input("Press Enter to Exit !!!")