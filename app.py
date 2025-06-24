import streamlit as st
import pandas as pd
import re
from datetime import datetime, date, time
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import io

# Apparatus-to-type mapping
apparatus_class = {
    "Engine": "FIRE",
    "Truck": "FIRE",
    "Tower": "FIRE",
    "Rescue Squad": "FIRE",
    "Hazmat": "FIRE",
    "Fire Boat": "FIRE",
    "Command Unit": "FIRE",
    "Foam Unit": "FIRE",
    "Air Unit": "FIRE",
    "Rehab Unit": "FIRE",
    "Medic": "EMS",
    "Ambulance": "EMS",
    "EMS": "EMS"
}

# WDO codes (from OPERATIONS - Working section, excluding REG)
wdo_codes = {
    # FIRE
    "DOW", "+DETAIL", "DET AS PEC", "TA-ANNEDU", "WDO", "+OTC", "BN", "FFD", "DETAIL",
    # EMS
    "+EMS (FF)", "+EMS (PM)", "+CITYWIDE", "EMS/DOTW", "EMS SUPER",
    # OTHER
    "OT-COD", "MANHOLD", "MANCALLCX"
}

# Page config
st.set_page_config(page_title="Roster Ingestion", layout="centered")

# Password protection
pwd = st.sidebar.text_input("Password", type="password")
if not pwd or pwd != st.secrets["app"]["app_password"]:
    st.title("Roster Ingestion App")
    st.write("Enter the password to continue.")
    if pwd:
        st.error("Incorrect password")
    st.stop()

st.success("Access granted.")
st.header("Upload Rosters")

# BigQuery client
def get_bigquery_client():
    creds_dict = st.secrets["bigquery"]["credentials"]
    creds = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=creds, project=st.secrets["bigquery"]["project"])

# Extract date from filename
def extract_date_from_filename(fname):
    patterns = [
        {'regex': r'(\d{4}-\d{1,2}-\d{1,2})', 'formats': ['%Y-%m-%d']},
        {'regex': r'(\d{1,2}[.-]\d{1,2}[.-]\d{4})', 'formats': ['%m.%d.%Y', '%m-%d-%Y']},
        {'regex': r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})', 'formats': ['%m/%d/%Y']},
        {'regex': r'(\d{4}[.-]\d{1,2}[.-]\d{1,2})', 'formats': ['%Y.%m.%d', '%Y-%m-%d']},
        {'regex': r'(\d{1,2}[.-]\d{1,2}[.-]\d{2})', 'formats': ['%m.%d.%y', '%m-%d-%y']}
    ]
    for pat in patterns:
        m = re.search(pat['regex'], fname)
        if m:
            for fmt in pat['formats']:
                try:
                    return datetime.strptime(m.group(1), fmt).date()
                except ValueError:
                    continue
    return None

# Clean Excel data
def clean_roster_generic(df, filename):
    df2 = df.iloc[:, 1:].copy() if df.shape[1] > 1 else df.copy()
    df2.columns = [f"column_{i+1}" for i in range(df2.shape[1])]
    df2 = df2.replace(r'^\s*$', pd.NA, regex=True)

    if {'column_1', 'column_2', 'column_3'}.issubset(df2.columns):
        unit_rows = df2['column_2'].isna() & df2['column_3'].isna()
        df2['Unit'] = df2['column_1'].where(unit_rows).ffill()
    else:
        df2['Unit'] = pd.NA

    mask = pd.Series(True, index=df2.index)
    mask &= ~((df2['column_1'].astype(str).str.strip().str.upper() == 'RANK') &
              (df2['column_2'].astype(str).str.strip().str.upper() == 'ID'))
    mask &= ~(df2['column_2'].isna() & df2['column_3'].isna())
    df2 = df2.loc[mask].reset_index(drop=True)
    df2['column_1'] = df2['column_1'].ffill()

    keep = ['Unit'] + [f"column_{i}" for i in [1, 2, 3, 5, 6, 7, 8]]
    df2 = df2[[c for c in keep if c in df2.columns]].copy()

    df2['column_6'] = pd.to_datetime(df2.get('column_6'), format='%H:%M', errors='coerce').dt.time
    df2['column_7'] = pd.to_datetime(df2.get('column_7'), format='%H:%M', errors='coerce').dt.time
    df2['column_8'] = pd.to_numeric(df2.get('column_8'), errors='coerce')
    df2['Date'] = extract_date_from_filename(filename) or pd.NaT

    col_order = ['Unit'] + [f"column_{i}" for i in [1, 2, 3, 5, 6, 7, 8]] + ['Date']
    df2 = df2[[c for c in col_order if c in df2.columns]]
    return df2.rename(columns={c: f"column_{i+1}" for i, c in enumerate(df2.columns)})

# Rename and classify
def rename_and_type(df):
    rename_map = {
        "column_1": "division",
        "column_2": "rank",
        "column_3": "member_id",
        "column_4": "name",
        "column_5": "code",
        "column_6": "start",
        "column_7": "through",
        "column_8": "hours",
        "column_9": "roster_date"
    }
    df2 = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    def to_string_time(val):
        if isinstance(val, time):
            return val.strftime("%H:%M:%S")
        if isinstance(val, str):
            try:
                return datetime.strptime(val.strip(), "%H:%M").strftime("%H:%M:%S")
            except:
                return None
        return None

    for col in ["start", "through"]:
        if col in df2.columns:
            df2[col] = df2[col].apply(to_string_time)

    df2['wdo_flag'] = df2['code'].isin(wdo_codes)

    def get_ops_type(division):
        if pd.isna(division):
            return None
        division = str(division).strip()
        for keyword, value in apparatus_class.items():
            if keyword in division:
                return value
        if "Fire" in division and "EMS" not in division:
            return "FIRE"
        if "EMS" in division:
            return "EMS"
        return None

    df2['ops_type'] = df2['division'].apply(get_ops_type)

    def assign_wdo_category(row):
        if not row.get('wdo_flag'):
            return None
        if row.get('code') == '+EMS':
            if 'Medic' in row.get('division', ''):
                return '+EMS (PM)' if '(PM)' in str(row.get('name')) else '+EMS (FF)'
            elif 'Ambulance' in row.get('division', ''):
                return '+EMS (FF)'
        return f"{row.get('ops_type')} WDO"

    df2['wdo_category'] = df2.apply(assign_wdo_category, axis=1)

    return df2.where(pd.notnull(df2), None)

# Upload to BigQuery
def push_to_bigquery(df, table_id):
    client = get_bigquery_client()
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_file(
        file_obj=csv_buffer,
        destination=table_id,
        job_config=job_config
    )

    job.result()
    return job.output_rows

# Log upload
def log_upload_event(filename, row_count, status):
    client = get_bigquery_client()
    log_table_id = st.secrets["bigquery"]["log_table_id"]
    rows_to_insert = [{
        "filename": filename,
        "upload_time": datetime.utcnow().isoformat(),
        "row_count": row_count,
        "status": status
    }]
    errors = client.insert_rows_json(log_table_id, rows_to_insert)
    if errors:
        st.warning(f"⚠️ Failed to log upload for {filename}: {errors}")

# UI logic
st.title("Roster Report Ingestion")

upload_to_bigquery = st.checkbox("Upload to BigQuery", value=False)
uploaded_files = st.file_uploader("Choose Excel files", type=["xls", "xlsx"], accept_multiple_files=True)
table_id = st.secrets["bigquery"]["table_id"]

if uploaded_files:
    if st.button("Process and upload all"):
        summary = []
        for uploaded_file in uploaded_files:
            filename = Path(uploaded_file.name).name
            st.write(f"---\n**Processing: {filename}**")
            try:
                df_raw = pd.read_excel(uploaded_file, header=None)
                df_clean = clean_roster_generic(df_raw, filename)
                df_final = rename_and_type(df_clean)

                st.write("✅ Cleaned Data Preview")
                st.dataframe(df_final.head())

                if upload_to_bigquery:
                    st.info("📤 Uploading to BigQuery...")
                    row_count = push_to_bigquery(df_final, table_id)
                    st.success(f"✅ Uploaded {row_count} rows.")
                    log_upload_event(filename, row_count, "success")
                    summary.append((filename, "success", row_count))
                else:
                    st.warning("⏸️ Skipped BigQuery upload (preview only).")
                    log_upload_event(filename, len(df_final), "preview only")
                    summary.append((filename, "preview only", len(df_final)))

            except Exception as e:
                st.error(f"❌ Error processing '{filename}': {e}")
                log_upload_event(filename, 0, f"error: {str(e)}")
                summary.append((filename, "error", 0))

        st.write("## 📊 Upload Summary")
        st.dataframe(pd.DataFrame(summary, columns=["filename", "status", "row_count"]))
