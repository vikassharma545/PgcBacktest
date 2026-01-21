import os
import sys
import time
import shutil
import polars as pl
import pandas as pd
from tqdm import tqdm
from time import sleep
from pathlib import Path
import concurrent.futures
import dask.dataframe as dd
import pyarrow.parquet as pq
from tkinter import Tk, filedialog

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
        
def get_bool_input(prompt):
    return input(f"{prompt} (y/n): ").strip().lower() == 'y'

def get_parquet_files(folder_path):
    root = Path(folder_path).expanduser().resolve()
    iterator = root.rglob("*.parquet")
    return sorted(iterator)

def get_code_index_cols(parquet_files):
    code = parquet_files[0].stem.split()[2]
    indices = sorted(set([f.stem.split()[0] for f in parquet_files]))
    
    df = pd.read_parquet(max(parquet_files, key=lambda f: os.path.getsize(f)))
    name_columns = [c for c in list(df.columns) if c.startswith('P_')]
    pnl_columns = [c for c in list(df.columns) if c.endswith('PNL')]
    others_columns = [c for c in list(df.columns) if c not in name_columns + pnl_columns]
    return code, indices, name_columns, pnl_columns, others_columns

def check_parquet_file(file):
    try:
        table = pq.read_table(file)
        return None
    except Exception as e:
        return (f"Invalid file: {file} | Error: {e}")

if __name__ == "__main__":

    title = "Combine By ParameterWise"
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
            code, indices, name_columns, pnl_columns, others_columns = get_code_index_cols(parquet_files)
            print()
            print(f"Total File Uploaded :- {len(parquet_files)}")
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

    only_mtm_col = get_bool_input("Grouping with MTM columns only?")
    use_polars = get_bool_input("Use Polars (fastest) instead of (Pandas/Dask)?")
    
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
    
    combine_folder_path = parquet_files_folder_path.parent / f"{code}_output_ParameterWise"
    print(f"\nCombine Files Folder Path: {combine_folder_path}\n")

    shutil.rmtree(combine_folder_path, ignore_errors=True)
    os.makedirs(combine_folder_path, exist_ok=True)
    
    t1 = time.time()

    print('\nBuilding ParameterWise Files... \n')
    for index in indices:
        
        index_files = [f for f in parquet_files if f.stem.split()[0] == index]
        
        chunk_nos = sorted(set([f.stem.split()[-1] for f in index_files]))
        
        for chunk_no in chunk_nos:
            try:
                chunks_file = [f for f in index_files if f.stem.split()[-1] == chunk_no]
                print(f'\nTotal file in {index} Chunks-{chunk_no} -', len(chunks_file))
                if len(chunks_file) == 0: continue

                print('Reading Chunks...')
                if use_polars:
                    
                    def read_and_cast(path):
                        df = pl.read_parquet(path, columns=(name_columns + pnl_columns) if only_mtm_col else None)
                        
                        if only_mtm_col:
                            return df.with_columns([
                                pl.col(name_columns).cast(pl.Utf8).cast(pl.Categorical),
                                pl.col(pnl_columns).cast(pl.Float64),
                            ])
                        else:
                            return df.with_columns([
                                pl.col(name_columns).cast(pl.Utf8).cast(pl.Categorical),
                                pl.col(pnl_columns).cast(pl.Float64),
                                pl.col(others_columns).cast(pl.Utf8)
                            ])

                    with concurrent.futures.ThreadPoolExecutor() as exe:
                        dfs = list(exe.map(read_and_cast, chunks_file))
                
                    df = pl.concat(dfs)                    
                else:
                    df = dd.read_parquet(chunks_file, columns=(name_columns+pnl_columns) if only_mtm_col else None)
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
                            data.to_parquet(f"{combine_folder_path}/{index}/{file_name}", index=False, compression='zstd', compression_level=22)
                    except Exception as e:
                        print(e)

                print(f"🧩 {index} - Chunk {chunk_no}: Saving grouped files...")
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for idx, data in grouped:
                        executor.submit(save_file, idx, data)
                
            except Exception as e:
                input(f"ERROR !!! {e}")
                
    print("Done\n")
    t2 = time.time()
    minutes, seconds = divmod(t2 - t1, 60)
    print(f"\nTotal Time Taken: {int(minutes)} minutes and {round(seconds, 2)} seconds.\n")
    input("Press Enter to Exit !!!")
    