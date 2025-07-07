import streamlit as st
import pandas as pd
import re
from datetime import datetime, date, time
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import io


st.set_page_config(page_title="Roster Ingestion", layout="centered")
pwd = st.sidebar.text_input("Password", type="password")
if not pwd or pwd != st.secrets["app"]["app_password"]:
    st.title("Roster Ingestion App")
    st.write("Enter the password to continue.")
    if pwd:
        st.error("Incorrect password")
    st.stop()
st.success("Access granted.")

page = st.sidebar.selectbox("Select a page", ["Roster Upload", "WDO Dashboard"])


limited_injury_codes = {"LD", "LDPFC", "LD02X", "LDRTO", "LD/PFC"}
limited_issues_codes = {"LDOIA", "LD/AFC-MES", "LD/AFCO", "LD/ATC-OPS", "LD/BULLETIN12", "LD/AFC-EMS", "LDAFC-OPS"}
limited_all = limited_injury_codes | limited_issues_codes


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


fire_divisions = {
    "DFC Office / Staffing Office",
    "DFC Office / DFC Operations",
    "BFC 1 / BFC 1",
    "Quarters E06 / Engine 06",
    "Quarters E06 / Truck 04",
    "Quarters E10 / Engine 10P",
    "Quarters E10 / Truck 13",
    "Quarters E12 / Engine 12",
    "Quarters E14 / Engine 14",
    "Quarters E17 / Engine 17",
    "Quarters E26 / Engine 26P",
    "Quarters E26 / Truck 15",
    "BFC 2 / BFC 2",
    "Quarters E07 / Engine 07P",
    "Quarters E08 / Engine 08P",
    "Quarters E08 / Air Unit 2",
    "Quarters E18 / Engine 18P",
    "Quarters E18 / Truck 07",
    "Quarters E27 / Engine 27P",
    "Quarters E30 / Engine 30P",
    "Quarters E30 / Truck 17",
    "BFC 3 / BFC 3",
    "Quarters E15 / Engine 15P",
    "Quarters E19 / Engine 19P",
    "Quarters E25 / Engine 25P",
    "Quarters E32 / Engine 32P",
    "Quarters E32 / Truck 16",
    "Quarters E33 / Engine 33P",
    "Quarters E33 / Truck 08",
    "BFC 4 / BFC 4",
    "BFC 4 / Safety Officer",
    "Quarters E04 / Engine 04",
    "Quarters E04 / Air Unit 1",
    "Quarters E09 / Engine 09P",
    "Quarters E09 / Truck 09",
    "Quarters E11 / Engine 11P",
    "Quarters E11 / Truck 06",
    "Quarters E22 / Engine 22P",
    "Quarters E22 / Truck 11",
    "Quarters E24 / Engine 24",
    "BFC 5 / BFC 5",
    "Quarters E05 / Engine 05",
    "Quarters E05 / Rehab Unit",
    "Quarters E20 / Engine 20P",
    "Quarters E20 / Truck 12",
    "Quarters E21 / Engine 21",
    "Quarters E28 / Engine 28",
    "Quarters E28 / Truck 14",
    "Quarters E29 / Engine 29P",
    "Quarters E29 / Truck 05",
    "Quarters E31 / Engine 31P",
    "BFC 6 / BFC 6",
    "Quarters E01 / Engine 01",
    "Quarters E01 / Truck 02",
    "Quarters E02 / Engine 02",
    "Quarters E03 / Engine 03P",
    "Quarters E13 / Engine 13P",
    "Quarters E13 / Truck 10",
    "Quarters E13 / Foam Unit",
    "Quarters E16 / Engine 16P",
    "Quarters E16 / Tower 3",
    "Quarters E16 / Command Unit 1",
    "Quarters E23 / Engine 23",
    "Special Operations / Battalion Chief Special Operations",
    "Quarters of Rescue Squad 1 / Rescue Squad 1",
    "Quarters of Rescue Squad 2 / Rescue Squad 2",
    "Quarters of Rescue Squad 3 / Rescue Squad 3",
    "Quarters of Hazmat Unit 1 / Hazmat 1",
    "Quarters of Fire Boat / Fire Boat",
    "Quarters of Fire Boat / Fire Boat Satellite Station",
    "Homeland Security / Fire Operations Center",
    "Homeland Security / Rail Operations Control Center",
    "OUC Liaison / Fire Liaison Officer",
    "OUC Liaison / EMS Liaison Officer",
    "Fire Prevention Division / Fire Investigations Unit",
    "Quarters E09 / Engine 09P/Station Security",
    "Battalion 8 Apparatus / Engine 34",
    "Battalion 8 Apparatus / Engine 35",
    "Battalion 8 Apparatus / Engine 36",
    "Battalion 8 Apparatus / Engine 37",
    "Battalion 8 Apparatus / Truck 34"
}

ems_divisions = {
    "DFC Office / Battalion Chief of EMS",
    "BFC 1 / EMS 1",
    "Quarters E06 / Ambulance 06",
    "Quarters E10 / Medic 10",
    "Quarters E12 / Ambulance 12",
    "Quarters E14 / Ambulance 14",
    "Quarters E14 / Medic 14",
    "Quarters E17 / Medic 17",
    "Quarters E26 / Ambulance 26",
    "BFC 2 / EMS 2",
    "Quarters E07 / Medic 07",
    "Quarters E08 / Medic 08",
    "Quarters E08 / Ambulance 08",
    "Quarters E18 / Ambulance 18",
    "Quarters E27 / Ambulance 27",
    "Quarters E27 / Medic 27",
    "Quarters E30 / Ambulance 30",
    "Quarters E30 / Ambulance 30B",
    "BFC 3 / EMS 3",
    "Quarters E15 / Ambulance 15",
    "Quarters E19 / Ambulance 19",
    "Quarters E19 / Medic 19",
    "Quarters E19 / Ambulance 19B",
    "Quarters E25 / Medic 25",
    "Quarters E25 / Ambulance 25",
    "Quarters E32 / Ambulance 32",
    "Quarters E33 / Ambulance 33",
    "Quarters E33 / Medic 33",
    "BFC 4 / EMS 4",
    "Quarters E04 / Ambulance 04",
    "Quarters E09 / Ambulance 09",
    "Quarters E11 / Ambulance 11",
    "Quarters E22 / Ambulance 22",
    "Quarters E24 / Medic 24",
    "BFC 5 / EMS 5",
    "Quarters E05 / Medic 05",
    "Quarters E20 / Ambulance 20",
    "Quarters E21 / Medic 21",
    "Quarters E28 / Ambulance 28",
    "Quarters E29 / Ambulance 29",
    "Quarters E31 / Medic 31",
    "BFC 6 / EMS 6",
    "Quarters E01 / Ambulance 01",
    "Quarters E01 / Medic 01",
    "Quarters E02 / Medic 02",
    "Quarters E03 / Medic 03",
    "Quarters E03 / Ambulance 03",
    "Quarters E13 / Ambulance 13",
    "Quarters E16 / Ambulance 16",
    "Quarters E23 / Ambulance 23",
    "Special Operations / EMS 7",
    "Ambulance 51",
    "Ambulance 52",
    "Ambulance 53",
    "Ambulance 54",
    "Ambulance 55",
    "Ambulance 56",
    "Quarters E14 / Ambulance 14",
    "Quarters E30 / Medic 30"
}

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

wdo_other_ops_codes = {
    "+OT-TA-ADM", "+OT-ISTO", "+OT-TA-INST", "+OT-RECERT", "+OT-SIMLAB", "+OT-TA-STUD",
    "+OT-SPOPS", "+OT-SSO", "+OT-USAR", "+OT-MARINE", "+OT-ROCC",
    "+OT-ARSON", "+OT-CANINE", "+OT-FIU",
    "+OT-OMD", "+OT-HOH",
    "+OTSE", "EPO", "+OT-FEDERAL",
    "+OT-Fleet", "+OT-AD", "+OT-UL", "+OT-APP_COMM",
    "+OT-LOGS", "+OT-PMD",
    "+OT-PEER",
    "+OT-FPD",
    "+OT-ADMIN", "+OT-COURT", "+OT-CPAT", "+OT-GUARD", "+OT-TS", "+OT-IT", "+OT-H&S"
}

def normalize_division_name(name):
    return str(name).strip().upper() if name else ""

def clean_division_string(text):
    if not text:
        return ""
    return re.sub(r'\s*\[.*?\]', '', text).strip().upper()


non_ops_divisions = {clean_division_string(d) for d in non_ops_divisions}
fire_divisions = {clean_division_string(d) for d in fire_divisions}
ems_divisions = {clean_division_string(d) for d in ems_divisions}

def get_ops_type(division, code=None):
    if code:
        code = str(code).strip().upper()
        if code in limited_all:
            return "LIMITED"
        if code == "+OT-COD":
            return "COD"

    division_clean = clean_division_string(division)

    if division_clean in non_ops_divisions:
        return "NON-OPS"
    elif division_clean in fire_divisions:
        return "FIRE"
    elif division_clean in ems_divisions:
        return "EMS"

    for key, val in apparatus_class.items():
        if key in division_clean:
            return val

    return "UNKNOWN"


wdo_codes = {
    "DOW", "DET AS PEC", "TA-ANNEDU", "WDO", "+WDO", "EMS/DOTW", "EMS SUPER",
    "OT-COD","+OT-COD", "MANHOLD", "MANCALLCX",
    "+OT-TA-ADM", "+OT-ISTO", "+OT-TA-INST", "+OT-RECERT", "+OT-SIMLAB", "+OT-TA-STUD",
    "+OT-SPOPS", "+OT-SSO", "+OT-USAR", "+OT-MARINE", "+OT-ROCC",
    "+OT-ARSON", "+OT-CANINE", "+OT-FIU",
    "+OT-OMD", "+OT-HOH",
    "+OTSE", "EPO", "+OT-FEDERAL",
    "+OT-Fleet", "+OT-AD", "+OT-UL", "+OT-APP_COMM",
    "+OT-LOGS", "+OT-PMD",
    "+OT-PEER",
    "+OT-FPD",
    "+OT-ADMIN", "+OT-COURT", "+OT-CPAT", "+OT-GUARD", "+OT-TS", "+OT-IT", "+OT-H&S"
}

# OPS (Not Working)
ops_nw_codes = {
    'ALP', 'AL', 'EAL', 'HOL', 'MIP', 'SLT', 'POD', 'DOT', 'PFL', 'FMLA', 'COMP', 'BEV',
    'MILI', 'JURY', 'RESIGN', 'LWOP', 'SWOP', 'AWOL', 'AWOP', 'ENLV', 'ADL IA', 'LD', 'LDOIA',
    '.ALP', '.AL', 'HOL/MIP', '.MIP', 'SL', '.POD', 'PFLU', 'AL/FMLA', 'COMPT', '.BEV',
    '.SL', 'PFLTT', 'FMSK', 'COMPS', '.COMPT', 'FMLW', 'FMCS', '.COMPS', 'ENLV-AL', 'ENLV-SL',
    'LDPFC', 'LD/AFC-MES', 'LD02X', 'LD/AFCO', 'LDRTO', 'LD/ATC-OPS', 'LD/BULLETIN12',
    'AL LIEU SL', 'HOL/PFL', 'HOL/SLT', 'DOTW/SL', 'ALTU', 'ALTS', 'RLT'
}
ops_nw_codes = {c.strip().upper() for c in ops_nw_codes}

# OPS (Detailed Outside Operations)
ops_doo_codes = {
    "FIRE-TA", "NFA", "ADL TRAIN", ".ADL TRAIN", "AD", "OFC", "OMD", "ODIV", "OUC", "PEERDET", "GUARD", 
    "DETAIL PFC", "USAR", "ROCC", "DETAIL-SE", "TRBD", "FP", "L36", "L3721", "ADMIN", 
    "BURN", "ADL PFC", "PFL/DCHR", "PFFA", "ADL COURT", "ADL RETIRE", "FUNERAL", ".ADL ALSTRN"
}
ops_doo_codes = {c.strip().upper() for c in ops_doo_codes}


def get_bigquery_client():
    creds_dict = st.secrets["bigquery"]["credentials"]
    creds = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=creds, project=st.secrets["bigquery"]["project"])

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

    if 'rank' in df2.columns:
        df2['rank'] = df2['rank'].astype(str).str.lstrip('.').str.strip()


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



    df2['ops_type'] = df2.apply(lambda row: get_ops_type(row.get('division'), row.get('code')), axis=1)


    def reclassify_ops_subtype(row):
        code = str(row.get('code', '')).strip().upper()
        division = normalize_division_name(row.get('division', ''))
        ops_type = row.get('ops_type', '').strip().upper()
        rank = str(row.get('rank', '')).upper()
        name = str(row.get('name', '')).upper()

        if code == "+OTC":
            return "OTC"
        
        if code in limited_injury_codes:
            return "injury"
        if code in limited_issues_codes:
            return "issues"

        
        if code == '+EMS' and ops_type == "EMS":
            if 'PM' in rank or '(PM)' in name or 'PM' in name:
                return "+EMS (PM)"
            else:
                return "+EMS (FF)"

        if ops_type in {"FIRE", "EMS"}:
            if code in ops_nw_codes:
                return "OPS-NW"
            if code in ops_doo_codes or "FIRE-TA" in division:
                return "OPS-DOO"
        return None


    df2['ops_subtype'] = df2.apply(reclassify_ops_subtype, axis=1)
    # Now fix ops_type based on subtype
    df2.loc[df2['ops_subtype'].isin(["injury", "issues"]), 'ops_type'] = "LIMITED"

    def assign_wdo_category(row):
        if not row.get('wdo_flag'):
            return None

        code = str(row.get('code', '')).strip().upper()
        division = str(row.get('division', '')).upper()
        rank = str(row.get('rank', '')).upper()
        name = str(row.get('name', '')).upper()
        ops_type = str(row.get('ops_type', '')).upper()

        ta_codes = {"+OT-TA-INST", "+OT-TA-ADM", "+OT-ISTO", "+OT-TA-STUD", "+OT-SIMLAB"}
        if code in ta_codes:
            return "WDO TA"
        if code == "+OTSE":
            return "WDO SE"

        if "FLEET" in division:
            return "WDO Fleet"

        if code in wdo_other_ops_codes:
            return "WDO Outside Ops"

        if ops_type == "EMS":
            if 'PM' in rank or 'PM' in name:
                return 'EMS WDO (PM)'
            else:
                return 'EMS WDO (FF)'
            
        if ops_type == "FIRE":
            if "PM" in rank or "PM" in name:
                return "Fire WDO (PM)"
            else:
                return "Fire WDO"

        return f"{ops_type} WDO"


    df2['wdo_category'] = df2.apply(assign_wdo_category, axis=1)
    return df2.where(pd.notnull(df2), None)

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

if page == "Roster Upload":
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
                    st.dataframe(df_final.head(30))

                    csv_download = df_final.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download CSV",
                        data=csv_download,
                        file_name=f"{filename.replace('.xlsx', '').replace('.xls', '')}_cleaned.csv",
                        mime='text/csv'
                )


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


elif page == "WDO Dashboard":
    st.header("WDO Dashboard Tester")

    import plotly.express as px
    import pandas as pd

    # Query BigQuery
    sql = """
    SELECT 
      roster_date,
      ops_type,
      COUNT(*) AS wdo_count
    FROM `disco-ivy-463814-n0.rosters.roster_data`
    WHERE wdo_flag = TRUE
    GROUP BY roster_date, ops_type
    """
    client = get_bigquery_client()
    df = client.query(sql).to_dataframe()

    # Pivot to wide format
    df_pivot = df.pivot(index="roster_date", columns="ops_type", values="wdo_count").fillna(0)
    df_pivot["TOTAL"] = df_pivot.get("FIRE", 0) + df_pivot.get("EMS", 0)

    def plot_interactive_series(data, label, color):
        df_plot = pd.DataFrame({
            "Date": pd.to_datetime(data.index).strftime('%Y-%m-%d'),
            "WDO Count": data.values
        })

        mean = data.mean()
        std = data.std()

        fig = px.bar(df_plot, x="Date", y="WDO Count", title=f"{label} WDO per Day", color_discrete_sequence=[color])
        fig.add_hline(y=mean, line_dash="dash", line_color="red", annotation_text=f"Mean = {mean:.2f}", annotation_position="top left")
        fig.add_hline(y=mean + std, line_dash="dot", line_color="green", annotation_text=f"+1 SD = {mean + std:.2f}")
        fig.add_hline(y=mean - std, line_dash="dot", line_color="green", annotation_text=f"-1 SD = {mean - std:.2f}")
        fig.update_layout(xaxis_title="Date", yaxis_title="WDO Count")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Fire WDO Per Day")
    plot_interactive_series(df_pivot["FIRE"], "Fire", color="orange")

    st.subheader("EMS WDO Per Day")
    plot_interactive_series(df_pivot["EMS"], "EMS", color="blue")

    st.subheader("Total WDO Per Day")
    plot_interactive_series(df_pivot["TOTAL"], "Total", color="purple")


