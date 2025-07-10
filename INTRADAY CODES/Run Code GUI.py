import re
import os
import sys
import math
import datetime
import subprocess
import numpy as np
import pandas as pd
from time import sleep
import psutil
import nbformat
from nbconvert import PythonExporter
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
import threading
import time

# --- Actual imports for pgcbacktest functions ---
# IMPORTANT: Ensure 'pgcbacktest' library is installed in your environment.
from pgcbacktest.BtParameters import * # Changed to wildcard import
from pgcbacktest.BacktestOptions import * # Changed to wildcard import

# --- Notebook to Script Converter ---
def convert_notebook_to_script(notebook_path, script_path, temp_meta_data_path, update_status_callback):
    """
    Converts a Jupyter notebook (.ipynb) to a Python script (.py).
    It also updates the 'meta_data_path' and extracts 'code' and 'output_csv_path'
    variables within the notebook's code cells if they exist.

    Args:
        notebook_path (str): The path to the input Jupyter notebook file.
        script_path (str): The path where the output Python script will be saved.
        temp_meta_data_path (str): The temporary path for the metadata CSV file
                                   to be injected into the notebook script.
        update_status_callback (callable): A callback function to update the GUI status.

    Returns:
        tuple: A tuple containing:
               - code_val (str): The value of the 'code' variable found in the notebook.
               - output_csv_path_val (str): The value of the 'output_csv_path' variable found in the notebook.
               Returns empty strings if conversion fails or variables are not found.
    """
    update_status_callback(f"Converting notebook: {notebook_path} to script: {script_path}")
    try:
        with open(notebook_path, 'r', encoding='utf-8') as nb_file:
            nb_content = nbformat.read(nb_file, as_version=4)

        code_val = ""
        output_csv_path_val = ""

        if temp_meta_data_path:
            for cell_idx, cell in enumerate(nb_content['cells']):
                if (cell['cell_type'] == 'code') and ("meta_data_path" in cell['source']):
                    source = cell['source'].splitlines()
                    for idx, value in enumerate(source):
                        if "meta_data_path =" in value:
                            source[idx] = f"meta_data_path = r'{temp_meta_data_path}'"
                        if "output_csv_path =" in value:
                            output_csv_path_val = value.split("=")[-1].strip()
                            output_csv_path_val = re.search(r'[\'"](.*?)[\'"]', output_csv_path_val).group(1).replace('\\\\', '\\')
                        if "code =" in value:
                            code_val = value.split("=")[-1].strip()
                            code_val = re.search(r'[\'"](.*?)[\'"]', code_val).group(1)

                    nb_content['cells'][cell_idx]['source'] = '\n'.join(source)

        script, _ = PythonExporter().from_notebook_node(nb_content)
        with open(script_path, 'w', encoding='utf-8') as py_file:
            py_file.write(script)

        update_status_callback("Notebook conversion complete.")
        return code_val, output_csv_path_val
    except Exception as e:
        update_status_callback(f"Error during notebook conversion: {e}")
        return "", ""

class App:
    """
    A GUI application for running Python scripts converted from Jupyter notebooks.
    It allows users to select a notebook, parameter CSV, and meta-data CSV,
    specify the number of concurrent terminals, and monitor system usage and process status.
    """
    def __init__(self, master):
        """
        Initializes the App with the main Tkinter window.

        Args:
            master (tk.Tk): The root Tkinter window.
        """
        self.master = master
        master.title("Code Runner GUI")
        master.geometry("800x700")

        self.cache_path = "C:/.temp"
        os.makedirs(self.cache_path, exist_ok=True)
        self.pickle_path = 'C:/PICKLE/' # Defined as in your original script

        # Tkinter StringVars for input fields
        self.jupyter_code_path = tk.StringVar()
        self.parameter_csv_path = tk.StringVar()
        self.meta_data_csv_path = tk.StringVar()
        self.num_terminals = tk.StringVar(value="1") # Changed to StringVar to handle partial input better
        self.num_terminals.trace_add("write", self._validate_num_terminals)

        # Tkinter StringVars for monitoring display - now holds the full string
        self.ram_usage_str = tk.StringVar(value="🧠 RAM Used: N/A")
        self.cpu_usage_str = tk.StringVar(value="🖥 CPU Used: N/A")
        self.pending_dates_str = tk.StringVar(value="📅 Pending Dates: N/A")
        self.terminals_allowed_str = tk.StringVar(value="⚙️ Terminals Allowed: N/A")
        self.terminals_running_str = tk.StringVar(value="🏃 Terminals Running: N/A")

        self.processes = []
        self.monitoring_thread = None
        self.running = False
        self.code_base = ""
        self.script_output = ""
        self.output_csv_path = ""
        self.parameter_len = 0
        self.meta_data_df = pd.DataFrame() # To store the loaded meta_data
        self.parameter_meta_data_for_run = "" # Path to the temp metadata file for current run

        self.create_widgets()
        self.master.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Determine python_path once
        self.python_path = ""
        for p in sys.path:
            if p.endswith("\\Lib\\site-packages"):
                self.python_path = p.replace("\\Lib\\site-packages", "") + "\\python.exe"
                break
        self.python_path = self.python_path.replace("\\", "/")
        if not os.path.exists(self.python_path):
            messagebox.showerror("Error", f"Python executable not found at: {self.python_path}\nPlease ensure Python is installed correctly.")
            self.python_path = sys.executable # Fallback to current interpreter

    def _validate_num_terminals(self, *args):
        """
        Validates the input for the number of terminals field.
        Ensures the input is an integer and greater than or equal to -1.
        """
        current_value = self.num_terminals.get()
        if not current_value: # Allow empty string for backspace/initial state
            return
        try:
            int_value = int(current_value)
            if int_value < -1:
                self.num_terminals.set("1") # Reset to default if less than -1
                self.update_status_text("Number of terminals must be an integer >= -1. Resetting to 1.")
        except ValueError:
            if len(current_value) > 1:
                self.num_terminals.set(current_value[:-1])
            else:
                self.num_terminals.set("1") # Reset to default if single invalid char
            self.update_status_text("Invalid input. Please enter a number.")

    def create_widgets(self):
        """
        Creates and arranges all the widgets in the GUI.
        This includes input fields, buttons, and monitoring displays.
        """
        # Apply a modern theme
        style = ttk.Style()
        style.theme_use('clam') # 'clam', 'alt', 'default', 'classic' are good options

        # Configure fonts and colors
        style.configure("TLabel", font=("Segoe UI", 10))
        style.configure("TButton", font=("Segoe UI", 10, "bold"), padding=6, relief="raised", borderwidth=2)
        style.map("TButton",
                  foreground=[('active', 'black')],
                  background=[('!active', '#e0e0e0'), ('active', '#c0c0c0')]) # Default button colors

        # Custom styles for specific buttons
        style.configure("Run.TButton", background="#4CAF50", foreground="white") # Green for Run
        style.map("Run.TButton",
                  background=[('active', '#45a049')],
                  foreground=[('active', 'white')])

        style.configure("Stop.TButton", background="#f44336", foreground="white") # Red for Stop
        style.map("Stop.TButton",
                  background=[('active', '#da190b')],
                  foreground=[('active', 'white')])

        style.configure("Exit.TButton", background="#607D8B", foreground="white") # Grey for Exit
        style.map("Exit.TButton",
                  background=[('active', '#546E7A')],
                  foreground=[('active', 'white')])

        # Style for monitoring labels
        style.configure("Monitor.TLabel", font=("Segoe UI", 10, "bold"), foreground="#333333")

        self.master.configure(bg="#f0f0f0") # Light grey background for the main window

        # Input Frame
        input_frame = ttk.LabelFrame(self.master, text="Configuration", padding=(20, 10), relief="groove")
        input_frame.pack(pady=15, padx=20, fill="x", expand=False)
        input_frame.columnconfigure(1, weight=1) # Allow entry field to expand

        ttk.Label(input_frame, text="Jupyter Code (.ipynb):").grid(row=0, column=0, sticky="w", pady=5, padx=5)
        ttk.Entry(input_frame, textvariable=self.jupyter_code_path, width=60).grid(row=0, column=1, pady=5, padx=5, sticky="ew")
        self.browse_jupyter_btn = ttk.Button(input_frame, text="Browse", command=lambda: self.browse_file(self.jupyter_code_path, ".ipynb"))
        self.browse_jupyter_btn.grid(row=0, column=2, pady=5, padx=5)

        ttk.Label(input_frame, text="Parameter CSV (.csv):").grid(row=1, column=0, sticky="w", pady=5, padx=5)
        ttk.Entry(input_frame, textvariable=self.parameter_csv_path, width=60).grid(row=1, column=1, pady=5, padx=5, sticky="ew")
        self.browse_param_btn = ttk.Button(input_frame, text="Browse", command=lambda: self.browse_file(self.parameter_csv_path, ".csv"))
        self.browse_param_btn.grid(row=1, column=2, pady=5, padx=5)

        ttk.Label(input_frame, text="Meta Data CSV (.csv):").grid(row=2, column=0, sticky="w", pady=5, padx=5)
        ttk.Entry(input_frame, textvariable=self.meta_data_csv_path, width=60).grid(row=2, column=1, pady=5, padx=5, sticky="ew")
        self.browse_meta_btn = ttk.Button(input_frame, text="Browse", command=lambda: self.browse_file(self.meta_data_csv_path, ".csv"))
        self.browse_meta_btn.grid(row=2, column=2, pady=5, padx=5)

        ttk.Label(input_frame, text="Number of Terminals Allowed (-1 for all):").grid(row=3, column=0, sticky="w", pady=5, padx=5)
        self.num_terminals_entry = ttk.Entry(input_frame, textvariable=self.num_terminals, width=10)
        self.num_terminals_entry.grid(row=3, column=1, sticky="w", pady=5, padx=5)

        # Buttons Frame
        button_frame = ttk.Frame(self.master, padding=(20, 10))
        button_frame.pack(pady=5, fill="x")

        self.run_button = ttk.Button(button_frame, text="Run Code", command=self.start_monitoring_thread, style="Run.TButton")
        self.run_button.pack(side="left", padx=10, ipadx=10, ipady=5)
        self.stop_button = ttk.Button(button_frame, text="Stop All", command=self.stop_processes, style="Stop.TButton")
        self.stop_button.pack(side="left", padx=10, ipadx=10, ipady=5)
        self.exit_button = ttk.Button(button_frame, text="Exit", command=self.on_closing, style="Exit.TButton")
        self.exit_button.pack(side="right", padx=10, ipadx=10, ipady=5)

        # Initially set button states
        self._set_button_states(False) # Start with Stop All disabled

        # Monitoring Frame
        monitor_frame = ttk.LabelFrame(self.master, text="System & Process Monitoring", padding=(20, 10), relief="groove")
        monitor_frame.pack(pady=15, padx=20, fill="x", expand=False)
        monitor_frame.columnconfigure(0, weight=1) # Allow the single label column to expand
        monitor_frame.columnconfigure(1, weight=1) # Allow the single label column to expand

        # Now, each monitoring label will display the full string including the descriptive text and value
        ttk.Label(monitor_frame, textvariable=self.ram_usage_str, style="Monitor.TLabel").grid(row=0, column=0, sticky="w", pady=2, padx=5)
        ttk.Label(monitor_frame, textvariable=self.cpu_usage_str, style="Monitor.TLabel").grid(row=0, column=1, sticky="w", pady=2, padx=5)

        ttk.Label(monitor_frame, textvariable=self.pending_dates_str, style="Monitor.TLabel").grid(row=1, column=0, sticky="w", pady=2, padx=5)
        ttk.Label(monitor_frame, textvariable=self.terminals_allowed_str, style="Monitor.TLabel").grid(row=1, column=1, sticky="w", pady=2, padx=5)

        ttk.Label(monitor_frame, textvariable=self.terminals_running_str, style="Monitor.TLabel").grid(row=2, column=0, sticky="w", pady=2, padx=5)

        # Status Text Area
        status_frame = ttk.LabelFrame(self.master, text="Status Log", padding=(10, 5), relief="groove")
        status_frame.pack(pady=10, padx=20, fill="both", expand=True)
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(0, weight=1)

        self.status_text = scrolledtext.ScrolledText(status_frame, wrap=tk.WORD, font=("Consolas", 9),
                                                     bg="#ffffff", fg="#333333", relief="flat", borderwidth=0,
                                                     insertbackground="black", selectbackground="#b3d4fc")
        self.status_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        self.status_text.insert(tk.END, "Welcome! Please select your files and click 'Run Code'.\n")
        self.status_text.config(state=tk.DISABLED) # Make it read-only

    def _set_button_states(self, running):
        """
        Sets the state of Run and Stop buttons, and input fields.

        Args:
            running (bool): True if processes are running, False otherwise.
        """
        if running:
            self.run_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            self.browse_jupyter_btn.config(state=tk.DISABLED)
            self.browse_param_btn.config(state=tk.DISABLED)
            self.browse_meta_btn.config(state=tk.DISABLED)
            self.num_terminals_entry.config(state=tk.DISABLED)
        else:
            self.run_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            self.browse_jupyter_btn.config(state=tk.NORMAL)
            self.browse_param_btn.config(state=tk.NORMAL)
            self.browse_meta_btn.config(state=tk.NORMAL)
            self.num_terminals_entry.config(state=tk.NORMAL)

    def update_status_text(self, message):
        """
        Updates the status text area in a thread-safe manner.

        Args:
            message (str): The message to append to the status log.
        """
        self.master.after(0, self._update_status_text_thread_safe, message)

    def _update_status_text_thread_safe(self, message):
        """
        Helper function to update the status text area, ensuring it's done on the main thread.
        """
        self.status_text.config(state=tk.NORMAL)
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.see(tk.END)
        self.status_text.config(state=tk.DISABLED)

    def update_monitoring_labels(self, ram_usage, cpu_usage, pending_dates, terminals_allowed, terminals_running):
        """
        Updates the monitoring labels in a thread-safe manner.

        Args:
            ram_usage (float): Current RAM usage percentage.
            cpu_usage (float): Current CPU usage percentage.
            pending_dates (int): Number of pending dates.
            terminals_allowed (int): Maximum number of terminals allowed.
            terminals_running (int): Number of terminals currently running.
        """
        self.master.after(0, self._update_monitoring_labels_thread_safe, ram_usage, cpu_usage, pending_dates, terminals_allowed, terminals_running)

    def _update_monitoring_labels_thread_safe(self, ram_usage, cpu_usage, pending_dates, terminals_allowed, terminals_running):
        """
        Helper function to update monitoring labels, ensuring it's done on the main thread.
        """
        self.ram_usage_str.set(f"🧠 RAM Used: {ram_usage:.1f}%")
        self.cpu_usage_str.set(f"🖥 CPU Used: {cpu_usage:.1f}%")
        self.pending_dates_str.set(f"📅 Pending Dates: {pending_dates}")
        self.terminals_allowed_str.set(f"⚙️ Terminals Allowed: {terminals_allowed}")
        self.terminals_running_str.set(f"🏃 Terminals Running: {terminals_running}")

    def browse_file(self, var, file_extension):
        """
        Opens a file dialog for the user to select a file and updates the associated StringVar.

        Args:
            var (tk.StringVar): The StringVar to update with the selected file path.
            file_extension (str): The file extension to filter for (e.g., ".ipynb", ".csv").
        """
        file_path = filedialog.askopenfilename(filetypes=[(f"{file_extension.upper()} files", f"*{file_extension}")])
        if file_path:
            var.set(file_path)
            self.update_status_text(f"Selected: {file_path}")

    def start_monitoring_thread(self):
        """
        Starts the main logic in a separate thread to keep the GUI responsive.
        Performs initial validation of input fields.
        """
        if self.running:
            messagebox.showinfo("Info", "Code is already running.")
            return

        # Clear previous log on new run
        self.status_text.config(state=tk.NORMAL)
        self.status_text.delete('1.0', tk.END)
        self.status_text.config(state=tk.DISABLED)

        jupyter_code = self.jupyter_code_path.get()
        parameter_csv = self.parameter_csv_path.get()
        parameter_meta_data = self.meta_data_csv_path.get()
        
        # Safely get the integer value from the StringVar
        try:
            no_of_terminal_allowed = int(self.num_terminals.get())
        except ValueError:
            messagebox.showerror("Input Error", "Please enter a valid number for 'Number of Terminals Allowed'.")
            return

        if not all([jupyter_code, parameter_csv, parameter_meta_data]):
            messagebox.showerror("Error", "Please select all required files.")
            return

        self.running = True
        self._set_button_states(True) # Disable Run, Enable Stop
        self.update_status_text("\n--- Starting Code Execution ---")
        self.monitoring_thread = threading.Thread(target=self.run_main_logic, args=(jupyter_code, parameter_csv, parameter_meta_data, no_of_terminal_allowed))
        self.monitoring_thread.daemon = True # Allow the thread to exit with the main program
        self.monitoring_thread.start()

    def stop_processes(self):
        """
        Terminates all running subprocesses and resets the GUI state.
        """
        self.update_status_text("\n--- Stopping all running processes ---")
        for proc in self.processes:
            try:
                proc.terminate()
                proc.wait(timeout=2)
                self.update_status_text(f"Terminated process: {proc.pid}")
            except psutil.TimeoutExpired:
                proc.kill()
                self.update_status_text(f"Force killed process: {proc.pid}")
            except Exception as e:
                self.update_status_text(f"Error stopping process {proc.pid}: {e}")
        self.processes = []
        self.running = False
        self.update_status_text("All processes stopped.")
        # Reset monitoring labels when processes are stopped
        # Safely get the integer value for display
        try:
            current_terminals_allowed = int(self.num_terminals.get())
        except ValueError:
            current_terminals_allowed = 0 # Default if invalid
        self.update_monitoring_labels(0, 0, 0, current_terminals_allowed, 0)
        self._set_button_states(False) # Enable Run, Disable Stop

    def on_closing(self):
        """
        Handles the window closing event, prompting the user to confirm stopping processes.
        """
        if messagebox.askokcancel("Quit", "Do you want to quit and stop all running processes?"):
            self.stop_processes()
            self.master.destroy()

    def run_main_logic(self, jupyter_code, parameter_csv, parameter_meta_data, no_of_terminal_allowed):
        """
        Contains the core logic for converting the notebook, setting up metadata,
        starting and monitoring subprocesses, and restarting them as needed.

        Args:
            jupyter_code (str): Path to the Jupyter notebook.
            parameter_csv (str): Path to the parameter CSV file.
            parameter_meta_data (str): Path to the meta-data CSV file.
            no_of_terminal_allowed (int): The maximum number of terminals allowed (-1 for all).
        """
        try:
            self.code_base = os.path.basename(jupyter_code).replace('.ipynb', '').title()
            self.script_output = os.path.join(self.cache_path, f"{self.code_base}.py")

            # Determine the path for the temporary metadata file
            if no_of_terminal_allowed == -1:
                self.parameter_meta_data_for_run = parameter_meta_data
            else:
                self.parameter_meta_data_for_run = os.path.join(self.cache_path, os.path.basename(parameter_meta_data))

            code, self.output_csv_path = convert_notebook_to_script(jupyter_code, self.script_output, self.parameter_meta_data_for_run, self.update_status_text)
            if not code:
                self.update_status_text("Failed to convert notebook or extract code/output path. Stopping.")
                self.running = False
                return

            code_details = f'\n#### RUN CODE ####\nCode: {code}\nJupyterCode: {jupyter_code} \nParameter: {parameter_csv} \nMetaData Parameter: {parameter_meta_data} \n##################'
            self.update_status_text(code_details)

            # Get parameter length and initial meta data using actual functions
            _, self.parameter_len = get_parameter_data(code, parameter_csv)
            self.meta_data_df, _ = get_meta_data(code, parameter_meta_data)

            # Format output_csv_path if it contains a placeholder
            self.output_csv_path = self.output_csv_path.format(code=code) if "{code}" in self.output_csv_path else self.output_csv_path

            self.update_and_write_metadata(no_of_terminal_allowed, code)

            self.update_status_text('\nRunning Code...\n')
            self.start_subprocesses(code)

            # --- Monitoring Memory/CPU & Terminals ---
            check_time = datetime.datetime.now() + datetime.timedelta(minutes=5)
            while self.running:
                try:
                    total_pending_dates = self.calculate_total_pending_dates(code)

                    if total_pending_dates == 0:
                        self.update_status_text('\nNo Pending Dates Left, all Dates files Complete :)')
                        self.running = False
                        break

                    mem_usage = psutil.virtual_memory().percent
                    cpu_usage = psutil.cpu_percent(interval=0.1) # Short interval for responsiveness
                    no_of_terminal_running = len([proc for proc in self.processes if proc.poll() is None])

                    # Update the monitoring labels
                    self.update_monitoring_labels(mem_usage, cpu_usage, total_pending_dates, no_of_terminal_allowed, no_of_terminal_running)

                    df_current_meta = pd.read_csv(self.parameter_meta_data_for_run) # Reload to get latest state
                    
                    # Check and restart finished processes if they have pending work
                    for i, proc in enumerate(self.processes[:]): # Iterate over a copy
                        if proc.poll() is not None: # Process has finished
                            # Find the row_idx that this process was running
                            proc_row_idx = int(proc.args[-1]) if len(proc.args) > 1 and proc.args[-2] == '-r' else -1
                            
                            if proc_row_idx != -1 and proc_row_idx < len(df_current_meta):
                                meta_row = df_current_meta.loc[proc_row_idx]
                                # Use actual get_meta_row_data
                                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, self.pickle_path)
                                pending_files_for_proc = [
                                    current_date.date() for current_date in date_lists
                                    if not is_file_exists(self.output_csv_path, f"{index} {current_date.date()} {code}", self.parameter_len)
                                ]

                                if pending_files_for_proc:
                                    self.update_status_text(f"Process for row {proc_row_idx} finished but has pending dates. Restarting...")
                                    new_proc = subprocess.Popen([self.python_path, self.script_output, "-r", str(proc_row_idx)], creationflags=subprocess.CREATE_NEW_CONSOLE)
                                    self.processes[i] = new_proc # Replace the old process with the new one
                                    time.sleep(1) # Small delay
                                else:
                                    self.update_status_text(f"Process for row {proc_row_idx} completed all its tasks.")
                                    # Remove the completed process from the list
                                    self.processes.pop(i)
                            else:
                                self.update_status_text(f"Process {proc.pid} finished, but its row index could not be determined or is out of bounds. Removing.")
                                self.processes.pop(i) # Remove if row_idx is invalid/unknown
                            
                    no_of_terminal_running = len([proc for proc in self.processes if proc.poll() is None])
                    
                    # High memory condition or low running terminals
                    if mem_usage > 90 or (no_of_terminal_allowed != -1 and check_time < datetime.datetime.now() and no_of_terminal_running < math.floor(no_of_terminal_allowed * 0.70)):
                        if mem_usage > 90:
                            self.update_status_text(f"\nHigh RAM: {mem_usage}% at {datetime.datetime.now()}\n")
                        else:
                            self.update_status_text(f"\nNumber of Terminal Running - {no_of_terminal_running} << {no_of_terminal_allowed}")
                            check_time = datetime.datetime.now() + datetime.timedelta(minutes=5) # Reset check_time

                        self.stop_processes() # Stop all current processes
                        self.update_and_write_metadata(no_of_terminal_allowed, code) # Re-evaluate and write metadata
                        self.update_status_text('\nCode is About to Restart in ...')
                        
                        if no_of_terminal_running == 0:
                            self.update_status_text("Restarting in 5 seconds...")
                            time.sleep(5)
                        else:
                            self.update_status_text("Restarting in 60 seconds...")
                            time.sleep(60)
                        
                        self.update_status_text('\nRunning Code...\n')
                        self.start_subprocesses(code) # Restart subprocesses

                    time.sleep(10) # Wait before next check
                except Exception as e:
                    self.update_status_text(f"Error in monitoring loop: {e}")
                    self.running = False # Stop monitoring on error
                    break

        except Exception as e:
            self.update_status_text(f"An error occurred during initial setup or main logic: {e}")
            self.running = False
        finally:
            self.update_status_text("\n--- Code Execution Finished or Stopped ---")
            self.running = False # Ensure running flag is false when done
            # Final update of monitoring labels
            # Safely get the integer value for display
            try:
                current_terminals_allowed = int(self.num_terminals.get())
            except ValueError:
                current_terminals_allowed = 0 # Default if invalid
            self.update_monitoring_labels(0, 0, 0, current_terminals_allowed, 0)
            self._set_button_states(False) # Ensure buttons are reset to initial state

    def calculate_total_pending_dates(self, code):
        """
        Calculates the total number of pending dates across all rows in the metadata.

        Args:
            code (str): The 'code' identifier from the notebook.

        Returns:
            int: The total count of pending dates.
        """
        total_pending_dates = 0
        # Use actual get_meta_data
        current_meta_data = get_meta_data(code, self.parameter_meta_data_for_run)[0]
        for row_idx in range(len(current_meta_data)):
            if current_meta_data.loc[row_idx, 'run']:
                meta_row = current_meta_data.iloc[row_idx]
                # Use actual get_meta_row_data and is_file_exists
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, self.pickle_path)
                files_dates = [
                    current_date.date() for current_date in date_lists
                    if not is_file_exists(self.output_csv_path, f"{index} {current_date.date()} {code}", self.parameter_len)
                ]
                total_pending_dates += len(files_dates)
        return total_pending_dates

    def update_and_write_metadata(self, no_of_terminal_allowed, code):
        """
        Updates and writes a temporary metadata file based on pending dates
        and the allowed number of terminals. This file is then used by subprocesses.

        Args:
            no_of_terminal_allowed (int): The maximum number of terminals allowed (-1 for all).
            code (str): The 'code' identifier from the notebook.
        """
        self.update_status_text('MetaData Creating...')
        index_dte_dates = {}
        total_dates = 0
        total_pending_dates = 0

        # Recalculate pending dates based on the original full metadata using actual get_meta_data
        full_meta_data, _ = get_meta_data(code, self.meta_data_csv_path.get())

        for row_idx in range(len(full_meta_data)):
            if full_meta_data.loc[row_idx, 'run']:
                meta_row = full_meta_data.iloc[row_idx]
                # Use actual get_meta_row_data and is_file_exists
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, self.pickle_path)
                total_dates += len(date_lists)
                files_dates = [
                    current_date.date() for current_date in date_lists
                    if not is_file_exists(self.output_csv_path, f"{index} {current_date.date()} {code}", self.parameter_len)
                ]

                if files_dates:
                    total_pending_dates += len(files_dates)
                    index_dte_dates[(index, dte)] = sorted(index_dte_dates.get((index, dte), []) + files_dates)

        # Use actual chunk_size
        no_of_chunk = math.ceil(self.parameter_len / chunk_size)
        file_details = (f'\n####### OUTPUT FILES #######\nNo of Chunks: {no_of_chunk}\n'
                        f'Totals Dates: {total_dates}\nTotal Files Created: {no_of_chunk * total_dates}\n'
                        f'Dates IndexWise: { {k: len(v) for k, v in index_dte_dates.items()} }\n############################')
        self.update_status_text(file_details)
        self.update_status_text(f"\nPending Dates (before split): {total_pending_dates}")

        if total_pending_dates == 0:
            self.update_status_text('\nNo Pending Dates Left, all Dates files Complete :)')
            self.running = False
            return

        if no_of_terminal_allowed > 0:
            sorted_keys = sorted(index_dte_dates.keys(), key=lambda k: len(index_dte_dates[k]), reverse=True)

            while len(sorted_keys) < no_of_terminal_allowed:
                splited = True
                key_to_split_found = False
                for key in sorted_keys:
                    if len(index_dte_dates[key]) > 1:
                        key_to_split_found = True
                        value = index_dte_dates[key]
                        mid = (len(value) + 1) // 2
                        first_half = value[:mid]
                        second_half = value[mid:]

                        # Generate unique keys for the split parts
                        a_key = (f'split_a_{key[0]}', key[1]) if isinstance(key[0], str) else ('a',) + key
                        b_key = (f'split_b_{key[0]}', key[1]) if isinstance(key[0], str) else ('b',) + key

                        index_dte_dates[a_key] = first_half
                        index_dte_dates[b_key] = second_half
                        del index_dte_dates[key]
                        splited = False
                        break # Break after splitting one key

                if splited and not key_to_split_found: # No key could be split further
                    no_of_terminal_allowed = -1 # Revert to -1 if no more splits possible
                    self.update_status_text("Cannot split further to reach desired number of terminals. Running all remaining as one.")
                    break
                sorted_keys = sorted(index_dte_dates.keys(), key=lambda k: len(index_dte_dates[k]), reverse=True)

            # If we have more keys than allowed terminals, truncate
            if len(sorted_keys) > no_of_terminal_allowed and no_of_terminal_allowed != -1:
                sorted_keys = sorted_keys[:no_of_terminal_allowed]
                index_dte_dates = {key: value for key, value in index_dte_dates.items() if key in sorted_keys}

            temp_meta_data = pd.DataFrame(columns=full_meta_data.columns)
            for key, value in index_dte_dates.items():
                temp_meta_data.loc[len(temp_meta_data)] = [key[-2], key[-1], value[0].strftime("%d-%m-%Y"), value[-1].strftime("%d-%m-%Y"), datetime.time(9,15), datetime.time(15,29), True]
            temp_meta_data.to_csv(self.parameter_meta_data_for_run, index=False)
        else: # no_of_terminal_allowed is -1 or 0 (treat 0 as -1 for this logic)
            # If -1, use the original meta_data_df to create the temp file
            # Filter out rows that are not marked to run or have no pending dates
            filtered_meta_data = full_meta_data[full_meta_data['run']].copy()
            
            # Re-check pending dates for each row in filtered_meta_data to ensure only truly pending rows are written
            rows_to_write = []
            for row_idx, meta_row in filtered_meta_data.iterrows():
                # Use actual get_meta_row_data and is_file_exists
                index, dte, _, _, _, _, date_lists = get_meta_row_data(meta_row, self.pickle_path)
                files_dates = [
                    current_date.date() for current_date in date_lists
                    if not is_file_exists(self.output_csv_path, f"{index} {current_date.date()} {code}", self.parameter_len)
                ]
                if files_dates:
                    # Update start_date and end_date to reflect only the pending dates
                    meta_row['start_date'] = files_dates[0].strftime("%d-%m-%Y")
                    meta_row['end_date'] = files_dates[-1].strftime("%d-%m-%Y")
                    rows_to_write.append(meta_row)
            
            if rows_to_write:
                temp_meta_data = pd.DataFrame(rows_to_write, columns=full_meta_data.columns)
                temp_meta_data.to_csv(self.parameter_meta_data_for_run, index=False)
            else:
                # If no rows to write after filtering, create an empty file or handle appropriately
                pd.DataFrame(columns=full_meta_data.columns).to_csv(self.parameter_meta_data_for_run, index=False)
                self.update_status_text("No pending dates found for any row. Empty metadata file created.")

        self.update_status_text('MetaData Created')

    def start_subprocesses(self, code):
        """
        Starts subprocesses based on the generated temporary metadata file.

        Args:
            code (str): The 'code' identifier from the notebook.
        """
        self.processes = []
        try:
            df = pd.read_csv(self.parameter_meta_data_for_run)
            if df.empty:
                self.update_status_text("No rows to run in the generated metadata file. Stopping.")
                self.running = False
                return

            for idx, row in df.iterrows():
                # Ensure 'run' column exists and is boolean
                if 'run' not in row or not row['run']:
                    continue # Skip if 'run' is False or missing

                self.update_status_text(f"Running Row {idx}: {self.code_base}")
                # Pass the row index to the subprocess so it knows which part of the metadata to use
                proc = subprocess.Popen([self.python_path, self.script_output, "-r", str(idx)], creationflags=subprocess.CREATE_NEW_CONSOLE)
                self.processes.append(proc)
                time.sleep(1) # Small delay to avoid overwhelming the system
        except FileNotFoundError:
            self.update_status_text(f"Error: Temporary metadata file not found at {self.parameter_meta_data_for_run}. Cannot start subprocesses.")
            self.running = False
        except Exception as e:
            self.update_status_text(f"Error starting subprocesses: {e}")
            self.running = False

# Main GUI setup
if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()