import os
import re
import sys
import subprocess

# ─── Self-Launcher: double-click to run ───
# When you double-click this .py file, Python runs it directly.
# We detect that and re-launch it via "streamlit run" automatically.
if "streamlit" not in sys.modules:
    script_path = os.path.abspath(__file__)
    subprocess.Popen([sys.executable, "-m", "streamlit", "run", script_path, "--server.headless=false"], cwd=os.path.dirname(script_path))
    sys.exit(0)

import duckdb
import pickle
import tempfile
import numpy as np
import pandas as pd
import polars as pl
import streamlit as st
from pathlib import Path
from natsort import natsorted
from tkinter import Tk, filedialog

pl.enable_string_cache()

def select_folder_gui(title="Select a Folder") -> Path | None:
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True) 
    folder_path = filedialog.askdirectory(title=title, parent=root)
    root.destroy() 
    return Path(folder_path) if folder_path else None

def sort_mixed_list(values):

    MONTH_ORDER = {'January':1,'February':2,'March':3,'April':4,'May':5,'June':6,'July':7,'August':8,'September':9,'October':10,'November':11,'December':12}
    if list(values) and all(str(v) in MONTH_ORDER for v in values):
        return sorted(values, key=lambda x: MONTH_ORDER[str(x)])

    def parse_value(value):
        val_str = str(value)
        match = re.match(r"^(-?\d+\.\d+|-?\d+)([.a-zA-Z%]*)$", val_str)
        if match:
            if match.group(2) == "":
                return (0, "", float(match.group(1)))
            else:
                return (1, match.group(2), float(match.group(1)))
        else:
            return (2, val_str, float('inf'))
    return natsorted(values, key=parse_value)

@st.cache_data
def get_parquet_files(folder_path):
    root = Path(folder_path).expanduser().resolve()
    iterator = root.rglob("*.parquet")
    return sorted(iterator)

def get_code_index_cols(dashboard_metadata):    
    code_type = dashboard_metadata['CodeType']
    code = dashboard_metadata['Strategy']
    indices = sorted(dashboard_metadata['Index'])
    name_columns = sorted(dashboard_metadata.keys() - {'CodeType', 'Dates', 'Strategy', 'Points'})
    pnl_columns = sort_mixed_list(dashboard_metadata['PL Basis'])
    return code_type, code, indices, name_columns, pnl_columns

@st.cache_data
def mapping_dashboard_files(parquet_files, code_type):
    data = []
    if code_type == 'Intraday':
        columns = ['Index', 'Year', 'Month', 'DTE', 'PL Basis', 'FilePath']
        for file in parquet_files:
            parts = file.stem.replace('--', '-@').split('-')
            parts = [p.replace('@', '-') for p in parts]
            data.append([file.parts[-2], int(parts[1]), parts[2], float(parts[3]), parts[4], file.as_posix()])
    elif code_type == 'Weekly':
        columns = ['Index', 'Year', 'Month', 'Start.DTE-End.DTE', 'PL Basis', 'FilePath']
        for file in parquet_files:
            parts = file.stem.replace('--', '-@').split('-')
            parts = [p.replace('@', '-') for p in parts]
            data.append([file.parts[-2], int(parts[1]), parts[2], parts[3], parts[4], file.as_posix()])
    return pd.DataFrame(data, columns=columns)

@st.cache_data
def load_and_filter_data(filtered_parquet_files, filter_conditions_tuple, top_level_filter_col_tuple):
    import time
    start_time = time.time()
    if not filtered_parquet_files:
        return pl.DataFrame(), 0
    
    filter_conditions = dict(filter_conditions_tuple)
    top_level_filter_col = list(top_level_filter_col_tuple)
    
    conn = duckdb.connect(':memory:')
    conditions = []
    for col, vals in filter_conditions.items():
        if vals and col not in top_level_filter_col:
            val_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in vals])
            conditions.append(f'"{col}" IN ({val_str})')
            
    where_sql = " AND ".join(conditions) if conditions else "1=1"
    query = f"SELECT * FROM read_parquet({list(filtered_parquet_files)}) WHERE {where_sql}"
    filtered_data = conn.execute(query).pl()
    conn.close()
    
    elapsed_time = time.time() - start_time
    return filtered_data, elapsed_time

# ─────────────────────────────────────────────
#  Page Config & Theme-Resilient CSS
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="PGC DashBoard", layout="wide", initial_sidebar_state="collapsed",
    page_icon="https://raw.githubusercontent.com/vikassharma545/PgcStreamlitDashboard/main/img/icon.png"
)

st.markdown("""<style>
/* Hide Sidebar Toggle safely */
[data-testid="collapsedControl"] { display: none; }
.block-container { padding: 3rem 2rem 1.5rem 2rem; max-width: 100%; }

/* ── Header Grouping Block ── */
.dashboard-header-block {
    border: 2px solid #4472c4;
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 24px;
    background: color-mix(in srgb, var(--secondary-background-color) 20%, transparent);
    box-shadow: 0 4px 12px rgba(68, 114, 196, 0.15);
}

/* ── Top bar ── */
.topbar {
    background: color-mix(in srgb, #1f3864 85%, var(--background-color));
    color: #ffffff;
    padding: 12px 24px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}
.topbar-left { display: flex; align-items: center; gap: 15px; }
.topbar h2 { margin: 0; font-size: 1.5rem; color: #ffffff; line-height: 1; }
.topbar .info { font-size: 0.9rem; opacity: 0.85; line-height: 1; }

/* ── Context Bar (Path) ── */
.context-bar {
    background: var(--secondary-background-color);
    padding: 8px 12px;
    border-radius: 4px;
    margin-top: -8px; 
    margin-bottom: 16px;
    font-size: 0.85rem;
    color: var(--text-color);
    display: flex;
    align-items: center;
    gap: 8px;
    border: 1px solid color-mix(in srgb, var(--text-color) 10%, transparent);
}
.context-label {
    font-weight: 700;
    color: #4472c4;
    text-transform: uppercase;
    font-size: 0.75rem;
    letter-spacing: 0.5px;
    white-space: nowrap;
}
.context-val-path {
    font-family: monospace;
    opacity: 0.85;
    word-break: break-all; 
}

/* ── Stat cells (Flexbox wrapped & gap optimized) ── */
.stat-row {
    display: flex;
    flex-wrap: wrap; 
    gap: 1px; 
    border: 1px solid var(--secondary-background-color);
    border-radius: 6px;
    overflow: hidden;
    background: var(--secondary-background-color); 
    box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}
.stat-cell {
    flex: 1;
    min-width: 150px; 
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    padding: 12px 8px;
    background: var(--background-color);
}
.stat-cell .sv { 
    font-size: 1.4rem; 
    font-weight: 700; 
    color: var(--text-color); 
    margin-top: 4px; 
    line-height: 1; 
    text-align: center;
}
.stat-cell .sl { 
    font-size: 0.7rem; 
    text-transform: uppercase; 
    color: #4472c4; 
    font-weight: 700; 
    letter-spacing: 0.5px; 
    line-height: 1; 
    text-align: center;
}

/* ── Excel-style slicer box ── */
.slicer-box {
    border: 1px solid var(--secondary-background-color);
    border-radius: 6px;
    background: var(--background-color);
    margin-bottom: 12px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.slicer-header {
    background: #4472c4;
    color: #ffffff;
    font-size: 0.85rem;
    font-weight: 600;
    padding: 6px 12px;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}
.slicer-body {
    padding: 4px; 
}

/* ── Status bar (Theme Resilient) ── */
.sb { 
    display: flex; 
    align-items: center; 
    background: color-mix(in srgb, #548235 15%, var(--background-color)); 
    border-left: 4px solid #548235; 
    padding: 8px 16px; 
    font-size: 0.9rem; 
    color: var(--text-color); 
    margin: 8px 0; 
    border-radius: 4px; 
}
.sb-blue { 
    background: color-mix(in srgb, #4472c4 15%, var(--background-color)); 
    border-left-color: #4472c4; 
}
</style>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  Folder Selection
# ─────────────────────────────────────────────

def select_folder_callback():
    folder = select_folder_gui("Select Folder containing Parquet files")
    if folder:
        st.session_state["folder_path"] = str(folder)

if "folder_path" not in st.session_state:
    st.markdown("""
<div style="text-align:center;padding:60px 0">
<img src="https://raw.githubusercontent.com/vikassharma545/PgcStreamlitDashboard/main/img/logo.png" width="120" style="margin-bottom: 20px;">
<h2 style="color:var(--text-color); margin-bottom: 5px;">PGC Dashboard</h2>
<p style="color:#6b7280; margin-bottom: 25px;">Select a dashboard folder to begin</p>
</div>
""", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 1])
    with c2:
        st.button("📂 Select DashBoard Folder", type="primary", on_click=select_folder_callback, use_container_width=True)
    st.stop()

folder_path = st.session_state["folder_path"]
if not os.path.exists(folder_path):
    st.error("⚠️ The specified folder could not be located. Please verify the path and try again.")
    st.button("📂 Select DashBoard Folder", type="primary", on_click=select_folder_callback)
    st.stop()

parquet_files = get_parquet_files(folder_path)
with open(Path(folder_path) / "MetaData.pickle", "rb") as f:
    dashboard_metadata = pickle.load(f)
if not parquet_files:
    st.warning("No Parquet files found in the provided folder path.")
    st.stop()

code_type, code, indices, name_columns, pnl_columns = get_code_index_cols(dashboard_metadata)
mapping_dashboard_files_df = mapping_dashboard_files(parquet_files, code_type)

# ─────────────────────────────────────────────
#  Top Bar, Context Bar & Stats (Grouped into Header Panel)
# ─────────────────────────────────────────────

st.markdown(f"""
<div class="dashboard-header-block">
<div class="topbar">
<div class="topbar-left">
<img src="https://raw.githubusercontent.com/vikassharma545/PgcStreamlitDashboard/main/img/logo.png" width="45" style="border-radius: 4px;">
<h2>{code}</h2>
</div>
<div class="info">{code_type} &nbsp;|&nbsp; {len(indices)} Indices &nbsp;|&nbsp; {len(parquet_files):,} Files</div>
</div>
<div class="context-bar">
<span class="context-label">📁 Source Path:</span> 
<span class="context-val-path">{folder_path}</span>
</div>
<div class="stat-row">
<div class="stat-cell"><div class="sl">Strategy Code</div><div class="sv">{code}</div></div>
<div class="stat-cell"><div class="sl">Total Files</div><div class="sv">{len(parquet_files):,}</div></div>
<div class="stat-cell"><div class="sl">Indices</div><div class="sv">{', '.join(indices)}</div></div>
<div class="stat-cell"><div class="sl">Parameter Cols</div><div class="sv">{', '.join(name_columns)}</div></div>
<div class="stat-cell"><div class="sl">PNL Cols</div><div class="sv">{', '.join(pnl_columns)}</div></div>
</div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  HeatMap Builder Axis Selection
# ─────────────────────────────────────────────

st.write("### HeatMap Builder 🔧")

# Removed the button, changed to 2 columns
ax1, ax2 = st.columns(2, vertical_alignment="bottom")
with ax1:
    st.markdown('<div class="slicer-box"><div class="slicer-header">Select HeatMap Index</div><div class="slicer-body">', unsafe_allow_html=True)
    pivot_index = st.selectbox("Row", options=name_columns, index=name_columns.index('StartTime') if 'StartTime' in name_columns else 0, label_visibility="collapsed")
    st.markdown('</div></div>', unsafe_allow_html=True)
with ax2:
    st.markdown('<div class="slicer-box"><div class="slicer-header">Select HeatMap Column</div><div class="slicer-body">', unsafe_allow_html=True)
    pivot_column = st.selectbox("Col", options=name_columns, index=name_columns.index('EndTime') if 'EndTime' in name_columns else min(1, len(name_columns) - 1), label_visibility="collapsed")
    st.markdown('</div></div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  Filters (Excel slicer boxes)
# ─────────────────────────────────────────────

filtered_dict = {}
filter_columns = [c for c in name_columns if c not in [pivot_index, pivot_column]]
filter_data = {}
for column in filter_columns:
    unique_values = sort_mixed_list(dashboard_metadata[column])
    if len(unique_values) > 1:
        if code_type == 'Intraday':
            default_values = [unique_values[0]] if column not in ['Year', 'Month', 'DTE'] else unique_values
        elif code_type == 'Weekly':
            default_values = [unique_values[0]] if column not in ['Year', 'Month', 'Start.DTE-End.DTE'] else unique_values
        filter_data[column] = (unique_values, default_values)

filter_keys = list(filter_data.keys())
cols_per_row = 3
for row_start in range(0, len(filter_keys), cols_per_row):
    row_keys = filter_keys[row_start:row_start + cols_per_row]
    cols = st.columns(cols_per_row)
    for i, key in enumerate(row_keys):
        unique_values, default_values = filter_data[key]
        with cols[i]:
            st.markdown(f'<div class="slicer-box"><div class="slicer-header">{key}</div><div class="slicer-body">', unsafe_allow_html=True)
            val = st.segmented_control(key, options=unique_values, selection_mode="multi", default=default_values, key=f"seg_control_{key}", label_visibility="collapsed")
            filtered_dict[key] = val
            st.markdown('</div></div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  Results & Excel Output (Instantly Executes)
# ─────────────────────────────────────────────

if pivot_index == pivot_column:
    st.warning("⚠️ Row and Column cannot be the same. Please select different axes.")
    st.stop()

if code_type == 'Intraday':
    top_level_filter_col = ['Index', 'Year', 'Month', 'DTE', 'PL Basis']
elif code_type == 'Weekly':
    top_level_filter_col = ['Index', 'Year', 'Month', 'Start.DTE-End.DTE', 'PL Basis']
    
temp_df = mapping_dashboard_files_df.copy()
for column in name_columns:
    if column in top_level_filter_col and column in filtered_dict:
        temp_df = temp_df[temp_df[column].isin(filtered_dict[column])]
        
filtered_parquet_files = temp_df['FilePath'].tolist()

r1, r2 = st.columns(2)
with r1:
    st.markdown(f'<div class="sb sb-blue">📁 &nbsp;<b>Total Files matching filters:</b> {len(filtered_parquet_files)}</div>', unsafe_allow_html=True)

if not filtered_parquet_files:
    st.warning("No files match the selected filters.")
    st.stop()

# Convert to hashable types for st.cache_data
filter_tuple = tuple((k, tuple(v) if v else ()) for k, v in filtered_dict.items())
files_tuple = tuple(filtered_parquet_files)

with st.spinner("Calculating..."):
    filtered_data, load_time = load_and_filter_data(files_tuple, filter_tuple, tuple(top_level_filter_col))

with r2:
    st.markdown(f'<div class="sb">✅ &nbsp;Data loaded in <b>{load_time:.2f} seconds</b> ({len(filtered_data):,} rows)</div>', unsafe_allow_html=True)

if len(filtered_data) == 0:
    st.warning("No data found with the selected filters.")
    st.stop()

raw_filtered_data = filtered_data

filtered_data = filtered_data.group_by([pivot_index, pivot_column]).agg(pl.col("Points").sum())
pivot = filtered_data.to_pandas().set_index([pivot_index, pivot_column]).unstack(fill_value=0).round(0)

if pivot.empty:
    st.warning("Pivot table is empty — no data for this Row × Column combination.")
    st.stop()

pivot = pivot.reindex(sort_mixed_list(pivot.index))
pivot.columns = [x[1] for x in pivot.columns]
pivot = pivot[sort_mixed_list(pivot.columns)]

# ─────────────────────────────────────────────
#  Cell Filter Conditions (cross-metric filtering)
# ─────────────────────────────────────────────
METRIC_OPTIONS = ['Total PNL', 'Max Drawdown', 'Avg by Year', 'Avg by Month',
                  'Median by Year', 'Median by Month', 'Calmar Ratio']
OP_OPTIONS = ['>', '<', '>=', '<=', '==']

st.write("### Cell Filter 🔍")
st.caption("Filter cells across all metrics. Only cells satisfying **all** conditions appear in a new Excel tab.")

n_conditions = st.number_input("Number of conditions", min_value=0, max_value=6, value=0, step=1)
cell_filter_conditions = []
for ci in range(int(n_conditions)):
    fc1, fc2, fc3 = st.columns([2, 1, 2])
    with fc1:
        metric = st.selectbox("Metric", METRIC_OPTIONS, key=f"cf_metric_{ci}", label_visibility="collapsed")
    with fc2:
        op = st.selectbox("Op", OP_OPTIONS, key=f"cf_op_{ci}", label_visibility="collapsed")
    with fc3:
        val = st.number_input("Value", value=0.0, step=1.0, key=f"cf_val_{ci}", label_visibility="collapsed", format="%.2f")
    cell_filter_conditions.append((metric, op, val))

# ─────────────────────────────────────────────
#  Excel Output (Windows only — xlwings needs COM)
# ─────────────────────────────────────────────

if sys.platform == 'win32':
    import xlwings as xw

    file_name = f"{code}.xlsx"
    file_path = Path(tempfile.gettempdir()) / f"{code}.xlsx"
    wb = None

    # --- ATTEMPT 1: Connect to Existing Open File ---
    try:
        wb = xw.books[file_name]
    except Exception:
        pass 

    # --- ATTEMPT 2: Open File (Standard) ---
    if wb is None:
        try:
            if os.path.exists(file_path):
                wb = xw.Book(file_path)
            else:
                wb = xw.Book()
                wb.save(file_path)
        except Exception:
            # --- ATTEMPT 3: The Fix for "Unknown name" / COM Errors ---
            try:
                new_app = xw.App(visible=True) 
                if os.path.exists(file_path):
                    wb = new_app.books.open(file_path)
                else:
                    wb = new_app.books.add()
                    wb.save(file_path)
            except Exception as e:
                st.error(f"❌ Fatal Excel Error: Unable to start Excel. Please close all Excel instances and try again. Error: {e}")

    # --- PROCEED IF WORKBOOK EXISTS ---
    if wb:
        sheet_name = "HeatMapDashboard"
        try:
            sheet = wb.sheets[sheet_name]
        except Exception:
            sheet = wb.sheets.add(sheet_name)

        sheet.clear()

        df_styled = pivot.copy()
        df_styled['Grand Total'] = df_styled.sum(axis=1)
        sum_row = df_styled.sum(axis=0)
        df_styled.loc['Grand Total'] = sum_row
        
        b1_cell = sheet.range("B1")
        b1_cell.value = pivot_column
        b1_cell.api.Font.Bold = True
        b1_cell.color = (220, 230, 241)
        b1_cell.api.Borders.LineStyle = 1

        start_cell = sheet.range("A2")
        start_cell.value = df_styled

        full_tbl = start_cell.expand()
        last_row = full_tbl.last_cell.row
        last_col = full_tbl.last_cell.column
        
        header_rng = sheet.range((start_cell.row, start_cell.column), (start_cell.row, last_col))
        header_rng.api.Font.Bold = True
        header_rng.color = (220, 230, 241)
        header_rng.api.Borders(8).LineStyle = 1  
        header_rng.api.Borders(9).LineStyle = 1 
        
        index_rng = sheet.range((start_cell.row + 1, start_cell.column), (last_row, start_cell.column))
        index_rng.api.Font.Bold = True
        index_rng.api.Borders(10).LineStyle = 1 

        bottom_rng = sheet.range((last_row, start_cell.column), (last_row, last_col))
        bottom_rng.api.Font.Bold = True
        bottom_rng.color = (220, 230, 241)
        bottom_rng.api.Borders(8).LineStyle = 1  
        bottom_rng.api.Borders(9).LineStyle = 1 

        right_rng = sheet.range((start_cell.row, last_col), (last_row, last_col))
        right_rng.api.Font.Bold = True
        right_rng.api.Borders(7).LineStyle = 1 
        right_rng.api.Borders(10).LineStyle = 1 
        
        data_rng = sheet.range((start_cell.row + 1, start_cell.column + 1), (last_row, last_col))
        data_rng.number_format = "#,##0" 

        data_rng = sheet.range((start_cell.row + 1, start_cell.column + 1), (last_row-1, last_col-1))
        data_rng.api.FormatConditions.Delete()
        data_rng.api.FormatConditions.AddColorScale(3) 
        
        merged_filters = filtered_dict.copy()

        if merged_filters:
            param_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in merged_filters.items()]))
            param_df = param_df.fillna("")  

            param_col_idx = last_col + 2
            param_anchor = sheet.range((start_cell.row, param_col_idx))
            param_anchor.options(index=False).value = param_df
            
            n_rows = param_df.shape[0] + 1  
            n_cols = param_df.shape[1]
            
            param_tbl = sheet.range(
                (param_anchor.row, param_anchor.column),
                (param_anchor.row + n_rows - 1, param_anchor.column + n_cols - 1)
            )
                                
            p_headers = sheet.range((param_anchor.row, param_anchor.column), (param_anchor.row, param_tbl.last_cell.column))
            p_headers.api.Font.Bold = True
            p_headers.color = (255, 235, 156) 
            p_headers.api.Borders.LineStyle = 1 
            
            param_tbl.api.Borders.LineStyle = 1 
            param_tbl.api.HorizontalAlignment = -4108 
            
        sheet.used_range.columns.autofit()

        # ─────────────────────────────────────────
        #  Max Drawdown Tab
        # ─────────────────────────────────────────
        dd_sheet_name = "Drawdown"
        try:
            dd_sheet = wb.sheets[dd_sheet_name]
        except Exception:
            dd_sheet = wb.sheets.add(dd_sheet_name)
        dd_sheet.clear()

        # ── Step 1: Group by period to get time-ordered PNL per cell ──
        MONTH_ORDER = {'January':1,'February':2,'March':3,'April':4,'May':5,
                        'June':6,'July':7,'August':8,'September':9,'October':10,
                        'November':11,'December':12}

        group_cols = [pivot_index, pivot_column, 'Year', 'Month']
        period_pnl = raw_filtered_data.group_by(group_cols).agg(
            pl.col("Points").sum()
        ).to_pandas()

        # Sort by period chronologically
        period_pnl['_month_ord'] = period_pnl['Month'].map(MONTH_ORDER).fillna(0)
        period_pnl = period_pnl.sort_values(['Year', '_month_ord'])

        # ── Step 2: Compute drawdown per cell ──
        def max_drawdown(pnl_series):
            arr = pnl_series.values.astype(float)
            if len(arr) == 0:
                return 0.0
            cum = np.cumsum(arr)
            peak = np.maximum.accumulate(cum)
            return float(np.min(cum - peak))

        dd_title = "Max Drawdown"
        dd_results = period_pnl.groupby([pivot_index, pivot_column])['Points'].apply(max_drawdown).reset_index()
        dd_results.columns = [pivot_index, pivot_column, 'DD']

        # ── Step 3: Pivot to heatmap grid ──
        dd_pivot = dd_results.pivot_table(
            index=pivot_index, columns=pivot_column, values='DD',
            aggfunc='sum', fill_value=0
        ).round(0)

        dd_pivot = dd_pivot.reindex(index=sort_mixed_list(dd_pivot.index.tolist()),
                                    columns=sort_mixed_list(dd_pivot.columns.tolist()),
                                    fill_value=0)

        dd_pivot['Grand Total'] = dd_pivot.sum(axis=1)
        dd_pivot.loc['Grand Total'] = dd_pivot.sum(axis=0)

        # ── Step 4: Write to Excel ──
        b1 = dd_sheet.range("B1")
        b1.value = pivot_column
        b1.api.Font.Bold = True
        b1.color = (220, 230, 241)
        b1.api.Borders.LineStyle = 1

        sc = dd_sheet.range("A2")
        sc.value = dd_pivot

        ft = sc.expand()
        lr = ft.last_cell.row
        lc = ft.last_cell.column

        # Header row
        hr = dd_sheet.range((sc.row, sc.column), (sc.row, lc))
        hr.api.Font.Bold = True
        hr.color = (220, 230, 241)
        hr.api.Borders(8).LineStyle = 1
        hr.api.Borders(9).LineStyle = 1

        # Index column
        ir = dd_sheet.range((sc.row + 1, sc.column), (lr, sc.column))
        ir.api.Font.Bold = True
        ir.api.Borders(10).LineStyle = 1

        # Bottom row (Grand Total)
        br = dd_sheet.range((lr, sc.column), (lr, lc))
        br.api.Font.Bold = True
        br.color = (220, 230, 241)
        br.api.Borders(8).LineStyle = 1
        br.api.Borders(9).LineStyle = 1

        # Right column (Grand Total)
        rr = dd_sheet.range((sc.row, lc), (lr, lc))
        rr.api.Font.Bold = True
        rr.api.Borders(7).LineStyle = 1
        rr.api.Borders(10).LineStyle = 1

        # Number format
        dr = dd_sheet.range((sc.row + 1, sc.column + 1), (lr, lc))
        dr.number_format = "#,##0"

        if lr > sc.row + 1 and lc > 2:
            inner = dd_sheet.range((sc.row + 1, sc.column + 1), (lr - 1, lc - 1))
            inner.api.FormatConditions.Delete()
            inner.api.FormatConditions.AddColorScale(3)

        dd_sheet.used_range.columns.autofit()

        # ─────────────────────────────────────────
        #  Helper: write a heatmap-style pivot to a sheet
        # ─────────────────────────────────────────
        def write_heatmap_sheet(wb, sheet_name, col_label, pivot_df):
            if sheet_name in [s.name for s in wb.sheets]:
                ws = wb.sheets[sheet_name]
            else:
                ws = wb.sheets.add(sheet_name)
            ws.clear()

            ws.range("B1").value = col_label
            ws.range("B1").api.Font.Bold = True
            ws.range("B1").color = (220, 230, 241)
            ws.range("B1").api.Borders.LineStyle = 1

            sc = ws.range("A2")
            sc.value = pivot_df

            ft = sc.expand()
            lr = ft.last_cell.row
            lc = ft.last_cell.column

            hr = ws.range((sc.row, sc.column), (sc.row, lc))
            hr.api.Font.Bold = True
            hr.color = (220, 230, 241)
            hr.api.Borders(8).LineStyle = 1
            hr.api.Borders(9).LineStyle = 1

            ir = ws.range((sc.row + 1, sc.column), (lr, sc.column))
            ir.api.Font.Bold = True
            ir.api.Borders(10).LineStyle = 1

            br = ws.range((lr, sc.column), (lr, lc))
            br.api.Font.Bold = True
            br.color = (220, 230, 241)
            br.api.Borders(8).LineStyle = 1
            br.api.Borders(9).LineStyle = 1

            rr = ws.range((sc.row, lc), (lr, lc))
            rr.api.Font.Bold = True
            rr.api.Borders(7).LineStyle = 1
            rr.api.Borders(10).LineStyle = 1

            dr = ws.range((sc.row + 1, sc.column + 1), (lr, lc))
            dr.number_format = "#,##0"

            if lr > sc.row + 1 and lc > 2:
                inner = ws.range((sc.row + 1, sc.column + 1), (lr - 1, lc - 1))
                inner.api.FormatConditions.Delete()
                inner.api.FormatConditions.AddColorScale(3)

            ws.used_range.columns.autofit()

        # ─────────────────────────────────────────
        #  Avg by Year Tab (weighted by months present)
        #  Total PNL / total months × 12 = annualized avg
        # ─────────────────────────────────────────
        monthly_pnl = raw_filtered_data.group_by([pivot_index, pivot_column, 'Year', 'Month']).agg(
            pl.col("Points").sum()
        ).to_pandas()

        n_months = monthly_pnl.groupby(['Year', 'Month']).ngroups

        avg_year = monthly_pnl.groupby([pivot_index, pivot_column])['Points'].sum().reset_index()
        avg_year['Points'] = (avg_year['Points'] / n_months * 12).round(0)

        avg_year_pivot = avg_year.pivot_table(
            index=pivot_index, columns=pivot_column, values='Points',
            aggfunc='sum', fill_value=0
        ).round(0)

        avg_year_pivot = avg_year_pivot.reindex(
            index=sort_mixed_list(avg_year_pivot.index.tolist()),
            columns=sort_mixed_list(avg_year_pivot.columns.tolist()),
            fill_value=0)

        avg_year_pivot['Grand Total'] = avg_year_pivot.sum(axis=1)
        avg_year_pivot.loc['Grand Total'] = avg_year_pivot.sum(axis=0)

        write_heatmap_sheet(wb, "Avg by Year", pivot_column, avg_year_pivot)

        # ─────────────────────────────────────────
        #  Avg by Month Tab
        #  Total PNL / total unique (Year,Month) pairs
        # ─────────────────────────────────────────
        avg_month = monthly_pnl.groupby([pivot_index, pivot_column])['Points'].sum().reset_index()
        avg_month['Points'] = (avg_month['Points'] / n_months).round(0)

        avg_month_pivot = avg_month.pivot_table(
            index=pivot_index, columns=pivot_column, values='Points',
            aggfunc='sum', fill_value=0
        ).round(0)

        avg_month_pivot = avg_month_pivot.reindex(
            index=sort_mixed_list(avg_month_pivot.index.tolist()),
            columns=sort_mixed_list(avg_month_pivot.columns.tolist()),
            fill_value=0)

        avg_month_pivot['Grand Total'] = avg_month_pivot.sum(axis=1)
        avg_month_pivot.loc['Grand Total'] = avg_month_pivot.sum(axis=0)

        write_heatmap_sheet(wb, "Avg by Month", pivot_column, avg_month_pivot)

        # ─────────────────────────────────────────
        #  Median by Year Tab (median monthly PNL × 12 = annualized)
        # ─────────────────────────────────────────
        med_year = monthly_pnl.groupby([pivot_index, pivot_column])['Points'].median().reset_index()
        med_year['Points'] = (med_year['Points'] * 12).round(0)

        med_year_pivot = med_year.pivot_table(
            index=pivot_index, columns=pivot_column, values='Points',
            aggfunc='sum', fill_value=0
        ).round(0)

        med_year_pivot = med_year_pivot.reindex(
            index=sort_mixed_list(med_year_pivot.index.tolist()),
            columns=sort_mixed_list(med_year_pivot.columns.tolist()),
            fill_value=0)

        med_year_pivot['Grand Total'] = med_year_pivot.sum(axis=1)
        med_year_pivot.loc['Grand Total'] = med_year_pivot.sum(axis=0)

        write_heatmap_sheet(wb, "Median by Year", pivot_column, med_year_pivot)

        # ─────────────────────────────────────────
        #  Median by Month Tab
        # ─────────────────────────────────────────
        med_month = monthly_pnl.groupby([pivot_index, pivot_column])['Points'].median().reset_index()
        med_month['Points'] = med_month['Points'].round(0)

        med_month_pivot = med_month.pivot_table(
            index=pivot_index, columns=pivot_column, values='Points',
            aggfunc='sum', fill_value=0
        ).round(0)

        med_month_pivot = med_month_pivot.reindex(
            index=sort_mixed_list(med_month_pivot.index.tolist()),
            columns=sort_mixed_list(med_month_pivot.columns.tolist()),
            fill_value=0)

        med_month_pivot['Grand Total'] = med_month_pivot.sum(axis=1)
        med_month_pivot.loc['Grand Total'] = med_month_pivot.sum(axis=0)

        write_heatmap_sheet(wb, "Median by Month", pivot_column, med_month_pivot)

        # ─────────────────────────────────────────
        #  Calmar Ratio Tab (Avg PNL/Year ÷ |Max Drawdown|)
        #  Higher is better. Uses avg_year_pivot and dd_pivot already computed.
        # ─────────────────────────────────────────
        # Work on inner cells only (exclude Grand Total row/col)
        avg_inner = avg_year_pivot.iloc[:-1, :-1]
        dd_inner = dd_pivot.iloc[:-1, :-1]

        # Align indices in case of ordering mismatch
        dd_aligned = dd_inner.reindex(index=avg_inner.index, columns=avg_inner.columns, fill_value=0)

        # Calmar = Avg Year PNL / |Max Drawdown|;  dd is negative, so use abs
        # Zero drawdown + positive PNL = best case → cap at 99.99
        dd_abs = dd_aligned.abs().replace(0, np.nan)
        calmar = avg_inner / dd_abs
        # Where drawdown was 0: positive avg → 99.99 (best), zero/negative avg → 0
        no_dd_mask = dd_abs.isna()
        calmar = calmar.where(~no_dd_mask, np.where(avg_inner > 0, 99.99, 0.0))
        calmar = calmar.round(2)

        calmar['Grand Total'] = calmar.mean(axis=1).round(2)
        calmar.loc['Grand Total'] = calmar.mean(axis=0).round(2)

        write_heatmap_sheet(wb, "Calmar Ratio", pivot_column, calmar)

        # Override number format to show 2 decimals for ratio
        calmar_ws = wb.sheets["Calmar Ratio"]
        csc = calmar_ws.range("A2")
        cft = csc.expand()
        clr = cft.last_cell.row
        clc = cft.last_cell.column
        calmar_ws.range((csc.row + 1, csc.column + 1), (clr, clc)).number_format = "#,##0.00"

        # ─────────────────────────────────────────
        #  Filtered Tab (cells passing all conditions)
        # ─────────────────────────────────────────
        import operator as op_module
        if cell_filter_conditions:
            # Map metric names to their inner pivot DataFrames (without Grand Total)
            metric_map = {
                'Total PNL':      df_styled.iloc[:-1, :-1],
                'Max Drawdown':   dd_pivot.iloc[:-1, :-1],
                'Avg by Year':    avg_year_pivot.iloc[:-1, :-1],
                'Avg by Month':   avg_month_pivot.iloc[:-1, :-1],
                'Median by Year': med_year_pivot.iloc[:-1, :-1],
                'Median by Month':med_month_pivot.iloc[:-1, :-1],
                'Calmar Ratio':   calmar.iloc[:-1, :-1],
            }

            op_map = {'>': op_module.gt, '<': op_module.lt, '>=': op_module.ge,
                      '<=': op_module.le, '==': op_module.eq}

            # Reference grid from Total PNL (all cells start as True)
            ref = metric_map['Total PNL']
            mask = pd.DataFrame(True, index=ref.index, columns=ref.columns)

            filter_desc_parts = []
            for metric_name, op_str, threshold in cell_filter_conditions:
                src = metric_map[metric_name].reindex(index=ref.index, columns=ref.columns, fill_value=0)
                cmp_fn = op_map[op_str]
                mask = mask & cmp_fn(src, threshold)
                filter_desc_parts.append(f"{metric_name} {op_str} {threshold:g}")

            # Build filtered pivot: show Total PNL where mask is True, else empty string
            filtered_pivot = ref.where(mask, other="")

            # Count qualifying cells per row/col
            match_counts_row = mask.sum(axis=1)
            match_counts_col = mask.sum(axis=0)

            filtered_pivot['Count'] = match_counts_row
            filtered_pivot.loc['Count'] = list(match_counts_col) + [int(mask.sum().sum())]

            filter_title = "Filtered: " + " AND ".join(filter_desc_parts)

            # ── Write to Excel manually (not using helper — NaN/blank cells break expand()) ──
            flt_sheet_name = "Filtered"
            if flt_sheet_name in [s.name for s in wb.sheets]:
                flt_ws = wb.sheets[flt_sheet_name]
            else:
                flt_ws = wb.sheets.add(flt_sheet_name)
            flt_ws.clear()

            flt_ws.range("B1").value = pivot_column
            flt_ws.range("B1").api.Font.Bold = True
            flt_ws.range("B1").color = (220, 230, 241)
            flt_ws.range("B1").api.Borders.LineStyle = 1

            sc = flt_ws.range("A2")
            sc.value = filtered_pivot

            # Calculate grid bounds from DataFrame shape (not expand)
            n_data_rows = filtered_pivot.shape[0]
            n_data_cols = filtered_pivot.shape[1]
            lr = sc.row + n_data_rows
            lc = sc.column + n_data_cols

            # Header row
            hr = flt_ws.range((sc.row, sc.column), (sc.row, lc))
            hr.api.Font.Bold = True
            hr.color = (220, 230, 241)
            hr.api.Borders(8).LineStyle = 1
            hr.api.Borders(9).LineStyle = 1

            # Index column
            ir = flt_ws.range((sc.row + 1, sc.column), (lr, sc.column))
            ir.api.Font.Bold = True
            ir.api.Borders(10).LineStyle = 1

            # Bottom row (Count)
            br = flt_ws.range((lr, sc.column), (lr, lc))
            br.api.Font.Bold = True
            br.color = (220, 230, 241)
            br.api.Borders(8).LineStyle = 1
            br.api.Borders(9).LineStyle = 1

            # Right column (Count)
            rr = flt_ws.range((sc.row, lc), (lr, lc))
            rr.api.Font.Bold = True
            rr.api.Borders(7).LineStyle = 1
            rr.api.Borders(10).LineStyle = 1

            # Number format for data cells
            dr = flt_ws.range((sc.row + 1, sc.column + 1), (lr, lc))
            dr.number_format = "#,##0"

            # Highlight qualifying cells green using Excel's native Conditional Formatting
            if n_data_rows > 1 and n_data_cols > 1:
                inner_grid = flt_ws.range((sc.row + 1, sc.column + 1), (lr - 1, lc - 1))
                inner_grid.api.FormatConditions.Delete()
                fc = inner_grid.api.FormatConditions.Add(1, 4, '=""')  # xlCellValue, xlNotEqual, ""
                fc.Interior.Color = 13561798  # BGR for (198, 239, 206)

            flt_ws.used_range.columns.autofit()
        else:
            # Remove stale Filtered tab if no conditions
            if "Filtered" in [s.name for s in wb.sheets] and len(wb.sheets) > 1:
                wb.sheets["Filtered"].delete()

        st.toast("✅ Excel Updated!")
else:
    st.caption("💡 Excel export available on Windows only (xlwings requires COM).")