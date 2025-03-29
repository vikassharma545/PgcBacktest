import os
import sys
import shutil
import subprocess
import numpy as np
import pandas as pd
from time import sleep

def menu_driver(options, msg=''):
    
    if len(options) == 1: return options[0]
    
    options.append("Exit !!!")
    while True:
        print("Menu:")
        for index, option in enumerate(options, start=1):
            print(f"{index}. {option}")
        try:
            choice = int(input("{} (1-{}): ".format(msg, len(options))))
            if 1 <= choice <= len(options):
                if choice == len(options):  # Check if the exit option was selected
                    print("Exiting the program.")
                    sys.exit()  # Exit the program
                return options[choice - 1]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")
            
import nbformat
from nbconvert import PythonExporter

def convert_notebook_to_script(notebook_path, script_path):
    # Load the notebook
    with open(notebook_path, 'r', encoding='utf-8') as notebook_file:
        notebook_content = nbformat.read(notebook_file, as_version=4)

    # Create a Python exporter
    python_exporter = PythonExporter()

    # Convert the notebook to a Python script
    script, _ = python_exporter.from_notebook_node(notebook_content)

    # Write the script to a .py file
    with open(script_path, 'w', encoding='utf-8') as script_file:
        script_file.write(script)

cache_path = "C:/.temp"
strategy_cache_path = f"{cache_path}/{{startegy}}"
if not os.path.isdir(cache_path): os.mkdir(cache_path)
list_files = os.listdir()

jupyter_files = [f for f in list_files if f.strip().lower().endswith('.ipynb')]
csv_files = [f for f in list_files if f.strip().lower().endswith('.csv') and 'metadata' in f.strip().lower()]

jupyter_code = menu_driver(jupyter_files, 'Choose Jupyter Code')
parameter = menu_driver(csv_files, 'Choose Parameter csv')

print(f'\n######### Run Code #########\nCode      : {jupyter_code} \nParameter : {parameter}\n############################')

code_name = jupyter_code.strip().lower().replace('.ipynb', '').title()
convert_notebook_to_script(jupyter_code, f"{strategy_cache_path.format(startegy=code_name)}.py")

python_path = [p for p in sys.path if p.endswith("\\Lib\\site-packages")][0].replace("\\Lib\\site-packages", "") + "\\python.exe"
python_path = python_path.replace("\\", "/")

parameter_df = pd.read_csv(parameter)
for row_no in range(len(parameter_df)):
	
    if parameter_df.loc[row_no, 'run']:

        print(row_no, code_name)
        subprocess.run(["start", python_path, f"{strategy_cache_path.format(startegy=code_name)}.py", '-r', str(row_no)], shell=True)
        sleep(2)