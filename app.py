import streamlit as st
import pandas as pd
import re
from datetime import datetime, date, time
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import io

# -- Non-operations divisions (full override list)
non_ops_divisions = {
        "Office of the Fire Chief / Executive Chief Staff",
    "Office of the Fire Chief / Executive Officer",
    "Information Technology Division / Information Technology Division",
    "Internal Affairs Division / IAD Staff",
    "Office of Medical Director / Medical Director",
    "Office of Medical Director / Assistant Medical Director",
    "Office of Medical Director - Chief of Staff / Office of Medical Director - Chief of Staff",
    "Controlled Medications / Controlled Medications",
    "Mobile Integrated Health Team / Mobile Integrated Health Team",
    "Mobile Integrated Health Team / Continution of Duty - MIH",
    "Continuous Quality Improvement / CQI",
    "OMD Special Projects / Whole Blood Project",
    "OMD Special Projects / Protocol Committee",
    "OMD Special Projects / OMD Special Projects",
    "Fire Prevention Division / Limited Duty - FPD",
    "Homeland Security / Homeland Security Special Events Officer #1",
    "Homeland Security / Homeland Security Special Events Officer #2",
    "Homeland Security / Homeland Security Special Events Logistics",
    "Flight 5342 Response / Midair Collision - Internal Review Committee",
    "Special Events / Special Event Planning",
    "OUC Liaison / Battalion Chief - OUC",
    "OUC Liaison / Limited Duty - OUC",
    "OUC Liaison / Special Projects - OUC",
    "Overtime Projects in Operations / List by Battalion (ADD NOTES FOR ALL ENTRIES)",
    "Overtime Projects in Operations / List by Special Operations",
    "Overtime Projects in Operations / Operations Work Groups (ADD NOTES FOR ENTRIES)",
    "Fleet / AFTER-HOURS APPARATUS DIVISION FOREMAN",
    "Fleet / Foreman and PSA Team AM",
    "Fleet / Fleet Management",
    "Fleet / Limited Duty - Apparatus Division",
    "Fleet / Technicians 0700-1530",
    "Fleet / Technicians 1500-2330",
    "Fleet / Fleet Overtime",
    "Fleet / Program Support Assistants",
    "Fleet / Foreman and PSA Team PM",
    "Adams Place / Captain Logistics",
    "Adams Place / Limited Duty - Adams Place",
    "V Street / DFC Logistics",
    "V Street / Captain Logistics",
    "V Street / Lieutenant Logistics",
    "V Street / Limited Duty - V Street",
    "Facilities Management Office / Facilities Overtime Projects",
    "Facilities Management Office / Facilities Management Director",
    "Facilities Management Office / Facilities Management Deputy Director",
    "Facilities Management Office / Facilities Maintenance Team",
    "Adams Place / EMS Changeover",
    "Adams Place / {off roster}",
    "V Street / Supply Technicians",
    "V Street / Clerical Assistant",
    "V Street / Inventory/Records Management",
    "Professional Standards Division / Professional Standards Office",
    "Professional Standards Division / Trial Board",
    "Professional Standards Division / Recruiting Office Staff",
    "Professional Standards Division / Recruiting Administrative",
    "Professional Standards Division / Departmental Court Appearances",
    "Professional Standards Division / Overtime Empowering Women to Lead",
    "Professional Standards Division / Deputy Chief Conferences",
    "Professional Standards Division / Battalion Chief Conferences",
    "Professional Standards Division / Recruiting Event",
    "Professional Standards Division / CPAT Facilitators",
    "Professional Standards Division / Honor Guard Overtime",
    "Professional Standards Division / Limited Duty - PSO",
    "EMS OPERATIONS / EMS Special Special Projects Overtime",
    "Director of Training / TA Staff",
    "Administrative Services / Training Administrative Staff",
    "School of Leadership & Development / Leadership & Development Staff",
    "School of Leadership & Development / Limited Duty Training Division",
    "School of Leadership & Development / Limited Duty - O2X Program",
    "School of Leadership & Development / Limited Duty Return to Operations Program",
    "School of Firefighting / Recruit Class",
    "School of Firefighting / Cadet Class",
    "School of Emergency Medical Services / EMS Staff PR Harris",
    "School of Emergency Medical Services / Limited Duty Training Division PR Harris",
    "School of Emergency Medical Services / FTEP Instructors",
    "School of Emergency Medical Services / Paramedic Program",
    "Training Academy Adjunct Instruction / Misc Overtime",
    "Training Academy Adjunct Instruction / 1403 Live Burns",
    "Training Academy Adjunct Instruction / LnD Class Instructors",
    "Training Academy Adjunct Instruction / ISTO Instructors",
    "Training Academy Adjunct Instruction / ISTO Logistics",
    "Training Academy Adjunct Instruction / Recruit Training Burns Instructor",
    "Training Academy Adjunct Instruction / Recruit Training Adjunct Instructor",
    "Training Academy Adjunct Instruction / Exam Proctors",
    "Training Academy Adjunct Instruction / EMS ISTO Instructors",
    "Training Academy Adjunct Instruction / EMS Sim Lab Instructors",
    "Training Academy Adjunct Instruction / EMS Adjunct Instructors",
    "Training Academy Adjunct Instruction / DOH Exam Proctors",
    "Training Academy Adjunct Instruction / CPR Instructor",
    "Special Operations Training / Special Operations Training",
    "School of Emergency Medical Services / PALS/ACLS Class",
    "School of Leadership & Development / Leadership & Development Training Courses",
    "School of Emergency Medical Services / Paramedic Grand Rounds",
    "Safety & Wellness / Health and Safety Division",
    "Safety & Wellness / Limited Duty - Health & Safety",
    "Mask Room / SCBA FIT Test",
    "Medical Services Office / Medical Services Office",
    "Safety & Wellness / Health & Wellness Special Projects",
    "Mask Room / Mask Room Technicians",
    "Medical Services Office / PFC - Follow up Appointment",
    "Medical Services Office / Annual Physical",
    "Medical Services Office / Stress Test",
    "Peer Support Team / Peer Support Team Special Projects",
    "Peer Support Team / Peer Support Team",
    "Chief of Staff / Grant Specialist",
    "Chief of Staff / Chief of Staff",
    "Chief of Staff / Administrative Office",
    "Resource Allocation Office / Program Financial Accountibility Officer",
    "Budgeting and Accounting Office / Budgeting and Accounting Office",
    "Office of General Counsel / Office of Legal Affairs",
    "Office of General Counsel / Office of Information and Privacy",
    "Office of General Counsel / Office of Compliance",
    "Office of General Counsel / General Counsel",
    "Human Resources / Human Resources",
    "Office of Program Analytics / Program Analytics",
    "Office of Labor Relations / Labor Relations",
    "Hands on Heart Program / Hands on Heart Program Manager",
    "Hands on Heart Program / Hands on Heart Program Staff",
    "Media and Community Relations Division / Media and Community Relations Administrative OT",
    "Strategy and Impact Office / Strategy and Impact Office",
    "EEO / EEO",
    "EEO / EEO Administrative Overtime",
    "Media and Community Relations Division / Media and Community Relations Division",
    "Media and Community Relations Division / Joint Information Center (JIC)",
    "Media and Community Relations Division / Media and Community Relations Special Event",
    "Hands on Heart Program / AED Coordinator",
    "Hands on Heart Program / Hands on Heart Program Training",
    "Hands on Heart Program / Hands on Heart Event",
    "Fire Education Specialists / Fire Education Specialists"
}

# Apparatus-to-type mapping (for EMS WDO labeling only)
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

# WDO codes
wdo_codes = {
    "DOW", "+DETAIL", "DET AS PEC", "TA-ANNEDU", "WDO", "+OTC", "BN", "FFD", "DETAIL",
    "+EMS (FF)", "+EMS (PM)", "+CITYWIDE", "EMS/DOTW", "EMS SUPER",
    "OT-COD", "MANHOLD", "MANCALLCX"
}

# Streamlit config
st.set_page_config(page_title="Roster Ingestion", layout="centered")
pwd = st.sidebar.text_input("Password", type="password")
if not pwd or pwd != st.secrets["app"]["app_password"]:
    st.title("Roster Ingestion App")
    st.write("Enter the password to continue.")
    if pwd:
        st.error("Incorrect password")
    st.stop()
st.success("Access granted.")

# BigQuery client setup
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

# Clean Excel file
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
        if division in non_ops_divisions:
            return None
        return "FIRE" if "Fire" in division and "EMS" not in division else ("EMS" if "EMS" in division else None)

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

# Upload logic
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
        st.warning(f"\u26a0\ufe0f Failed to log upload for {filename}: {errors}")

# Streamlit UI
st.header("Upload Rosters")
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

                st.write("Cleaned Data Preview")
                st.dataframe(df_final.head(15))

                if upload_to_bigquery:
                    st.info("Uploading to BigQuery...")
                    row_count = push_to_bigquery(df_final, table_id)
                    st.success(f"\u2705 Uploaded {row_count} rows.")
                    log_upload_event(filename, row_count, "success")
                    summary.append((filename, "success", row_count))
                else:
                    st.warning("\u23f8\ufe0f Skipped BigQuery upload (preview only).")
                    log_upload_event(filename, len(df_final), "preview only")
                    summary.append((filename, "preview only", len(df_final)))

            except Exception as e:
                st.error(f"\u274c Error processing '{filename}': {e}")
                log_upload_event(filename, 0, f"error: {str(e)}")
                summary.append((filename, "error", 0))

        st.write("Upload Summary")
        st.dataframe(pd.DataFrame(summary, columns=["filename", "status", "row_count"]))
