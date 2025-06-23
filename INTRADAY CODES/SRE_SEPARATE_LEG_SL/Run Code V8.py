import os, sys, shutil, ctypes, psutil, datetime, requests, subprocess, socket
import pandas as pd
import numpy as np
from time import sleep
import nbformat
from nbconvert import PythonExporter

# --- System & Environment Info ---
pc_name = f"{socket.gethostname()} {'Linux' if sys.platform == 'linux' else 'Windows'}"
strategy_name = os.getcwd().replace('\\', '/').split('/')[-1]
start_time = datetime.datetime.now()
universal_msg = f"{pc_name}\nRunning Code: {strategy_name}"
code_name = 'Run Code V8.py'

# --- Telegram Bot Function ---
def send_msg_telegram(msg):
    try:
        requests.get(f'https://api.telegram.org/bot5156026417:AAExQbrMAPrV0qI8tSYplFDjZltLBzXTm1w/sendMessage?chat_id=-607631145&text={msg}')
    except Exception as e:
        print(f"Telegram Error: {e}")

# --- User Menu Function ---
def menu_driver(options, msg=''):
    if len(options) == 1: return options[0]
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
                return options[choice - 1]
            else:
                print("Invalid choice.")
        except ValueError:
            print("Enter a number.")

# --- Notebook to Script Converter ---
def convert_notebook_to_script(notebook_path, script_path):
    with open(notebook_path, 'r', encoding='utf-8') as nb_file:
        nb_content = nbformat.read(nb_file, as_version=4)
    script, _ = PythonExporter().from_notebook_node(nb_content)
    with open(script_path, 'w', encoding='utf-8') as py_file:
        py_file.write(script)

# --- Platform-specific paths ---
cache_path = "/home/pranjali/.temp" if sys.platform == 'linux' else "C:/.temp"
strategy_cache_path = f"{cache_path}/{{strategy}}"
os.makedirs(cache_path, exist_ok=True)

# --- File Detection ---
all_files = os.listdir()
jupyter_files = [f for f in all_files if f.endswith('.ipynb')]
csv_files = [f for f in all_files if f.endswith('.csv') and 'metadata' in f.lower()]

# --- Menu Selection ---
jupyter_code = menu_driver(jupyter_files, 'Choose Jupyter Code')
parameter_csv = menu_driver(csv_files, 'Choose Parameter CSV')
code_base = jupyter_code.replace('.ipynb', '').title()

# --- Convert and Prepare ---
script_output = strategy_cache_path.format(strategy=code_base) + ".py"
convert_notebook_to_script(jupyter_code, script_output)
print(f'\n#### RUN CODE ####\nCode: {jupyter_code}\nParameter: {parameter_csv}\n##################')

# --- Python Path ---
if sys.platform == 'linux':
    python_path = '/usr/bin/python3'
else:
    python_path = [p for p in sys.path if p.endswith("\\Lib\\site-packages")][0].replace("\\Lib\\site-packages", "") + "\\python.exe"
    python_path = python_path.replace("\\", "/")

# --- Read CSV and Start Codes ---
df = pd.read_csv(parameter_csv)
for idx, row in df.iterrows():
    if row.get('run', False):
        print(f"Running Row {idx}: {code_base}")
        if sys.platform == 'linux':
            subprocess.Popen(["xfce4-terminal", "-e", f"{python_path} '{script_output}' -r {idx}"])
        else:
            subprocess.run(["start", python_path, script_output, "-r", str(idx)], shell=True)
        sleep(2)

# --- Check Duplicate Code Execution ---
def check_duplicate_runs(target_name):
    count = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if 'python' in proc.info['name'].lower():
                for arg in proc.info.get('cmdline', []):
                    if arg.endswith('.py') and os.path.basename(arg) == target_name:
                        count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return count

if check_duplicate_runs(code_name) > 1:
    sys.exit()

# --- Monitoring Memory and CPU ---
terminal_title = 'Code Monitor: Auto Restart on High RAM'
try:
    if sys.platform == 'linux':
        sys.stdout.write(f"\033]0;{terminal_title}\007")
    else:
        ctypes.windll.kernel32.SetConsoleTitleW(terminal_title)
except Exception:
    pass

while True:
    try:
        mem_usage = psutil.virtual_memory().percent
        cpu_usage = psutil.cpu_percent()
        current_time = datetime.datetime.now()
        msg = f"{universal_msg}\n🧠 RAM Used: {mem_usage}%\n🖥 CPU Used: {cpu_usage}%"

        # Periodic Telegram update
        if current_time > start_time + datetime.timedelta(minutes=30):
            send_msg_telegram(msg)
            start_time = current_time

        # High memory condition
        if mem_usage > 90:
            print(f"High RAM: {mem_usage}% at {datetime.datetime.now()}")
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if 'python' in proc.info['name'].lower():
                        for arg in proc.info.get('cmdline', []):
                            if arg.endswith('.py') and os.path.basename(arg) not in [code_name, 'Code Close And Run.py']:
                                print(f"Terminating: {arg}")
                                psutil.Process(proc.info['pid']).terminate()
                                sleep(3)
                except Exception as e:
                    print(f"Error stopping process: {e}")

            send_msg_telegram(
                f'{universal_msg}\n🧠 RAM Used: {mem_usage}%\n🖥 CPU Used: {cpu_usage}%\n⚠️ RAM > 90%.\n🛑 All processes stopped.\n✅ Restarting.'
            )

            # Restart the script
            sleep(25)
            if sys.platform == 'linux':
                subprocess.Popen(["xfce4-terminal", "-e", f"bash -c 'cd \"{os.getcwd()}\" && python3 \"{code_name}\"; exit'"])
            else:
                path = os.getcwd().replace("\\", "/")
                subprocess.run(f'start python "{path}/{code_name}"', shell=True)
            break

        else:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(msg, end='\r')
            sleep(3)

    except Exception as e:
        err_msg = f"Error in monitoring loop: {e}"
        send_msg_telegram(err_msg)
        print(err_msg)
        input("Press Enter to continue...")
