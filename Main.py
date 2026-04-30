# =============================================================================
#  OFFICIAL TAX AUDIT & COMPLIANCE PORTAL  -  v21.1  (Unified Permission Editor)
#  Changes v21.1:
#    [REF] render_user_admin() refactored: 4 fragmented permission forms replaced
#          with a single "Unified User Profile & Permission Editor" that writes
#          all changes in one gspread batch_update() call (429-safe).
#    [ADD] _ensure_recovery_email_col() helper (was called but never defined).
#    [KEEP] inject_css(), caching, backoff, Add User, Remove User all untouched.
# =============================================================================

import html as _html
import streamlit as st
import gspread
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import textwrap
import hashlib
import time
import pytz
from datetime import datetime, timedelta
import io
import extra_streamlit_components as stx
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
import logging
import gspread.exceptions

# 0. LOGGING
logging.basicConfig(level=logging.WARNING)
_log = logging.getLogger("audit_portal")

# 1. PAGE CONFIG
st.set_page_config(page_title="Tax Audit & Compliance Portal", layout="wide", initial_sidebar_state="collapsed")
TZ = pytz.timezone("Asia/Baghdad")

# 2. SESSION STATE DEFAULTS
_DEFAULTS: dict = dict(
    logged_in=False, user_email="", user_role="",
    allowed_tabs=[], allowed_projects=["ALL"], allowed_registers=["ALL"],
    date_filter="all",
    local_df=None, local_headers=None, local_col_map=None, local_cache_key=None, local_fetched_at=None,
    review_mode=False, review_row=None, review_new_vals=None, review_eval_val="",
    review_manual_notes="", review_record=None, review_df_iloc=None, review_ws_title=None, review_sid=None,
    backup_excel_data=None, backup_excel_name="", backup_excel_proj="", backup_last_sel="",
)
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# 3. CONSTANTS
PROJECTS = {}
MASTER_USERS_ID = st.secrets["master_sheet_id"]
SYSTEM_SHEETS  = {"UsersDB"}
USERS_SHEET    = "UsersDB"
VISIBLE_SHEETS = ["Registration", "Salary Tax", "Annual Filing"]
COL_STATUS="Status"; COL_LOG="Audit_Log"; COL_AUDITOR="Auditor_ID"
COL_DATE="Update_Date"; COL_EVAL="Data_Evaluation"; COL_FEEDBACK="Correction_Notes"
SYSTEM_COLS = [COL_STATUS, COL_LOG, COL_AUDITOR, COL_DATE, COL_EVAL, COL_FEEDBACK]
VAL_DONE="Processed"; VAL_PENDING="Pending"
EVAL_OPTIONS = ["Good","Incorrect","Duplicate"]
VALID_ROLES  = ["auditor","manager","admin"]

# Master list of grantable tabs
ALL_TAB_OPTIONS = ["Worklist","Archive","Analytics","Raw Logs","Error Analytics","User Admin", "Project Admin"]

READ_TTL=300; BACKOFF_MAX=15; _ROW_SEP=" \u007c "; _PAGE_SIZE=10; _COOKIE_NAME="portal_auth"
_PT="plotly_white"; _PBG="#FFFFFF"; _PGR="#E4E7F0"; _PFC="#0D1117"; _NVY="#4F46E5"; _BLU="#60A5FA"

# RangesDB constants
RANGES_SHEET = "RangesDB"
RANGES_COLS  = ["Sheet_ID", "Tab_Name", "Read_Range"]

_COMBO_TARGETS = [
    {"match":"باجدەری باج لە کام شاردایە","options":["Erbil / هەولێر","Sulaymaniyah / سلێمانی","Duhok / دهۆک"]},
    {"match":"في أي مدينة يقع هذا دافع الضرائب","options":["Erbil / هەولێر","Sulaymaniyah / سلێمانی","Duhok / دهۆک"]},
    {"match":"which city is this taxpayer located","options":["Erbil / هەولێر","Sulaymaniyah / سلێمانی","Duhok / دهۆک"]},
    {"match":"باجدەر سەر بە کام شارە","options":["Erbil / هەولێر","Sulaymaniyah / سلێمانی","Duhok / دهۆک"]},
    {"match":"هل يوجد نموذج يتضمن عناصر التسجيل","options":["Yes","No"]},
    {"match":"Does the company have an investment license","options":["Yes","No"]},
    {"match":"نشاط الشركة","options":["CEN / Construction & Engineering / بیناسازی و ئەندازیاری","HLT / Health Services /  خزمەتگوزاری تەندروستی","ITS / IT & Software / زانیاری تەکنەلۆژیا و سۆفتوێر","LOG / Transportation & Logistics / گواستنەوەولۆجیستیک","MFG / Manufacturing / بەرهەمهێنان","REF / Real Estate & Financial Services / خانووبەرە و خزمەتگوزاری دارایی","RET / Retail & Services / فرۆشتنی تاک و خزمەتگوزاریەکان","TEL / Telecom & Media / پەیوەندییەکان و میدیا","WHT / Wholesale & Trading / فرۆشتنی بە کۆ و بازرگانی"]},
    {"match":"ئەم کۆمپانیایە دوای ساڵی 2020 کار دەکات","options":["Yes","No"]},
    {"match":"Company status","options":["Active / چالاک","Shutting down / لەژێر پاکتاو کردنە/پاکتاو کراوە","Deleted / سڕاوەتەوە"]},
]

# 4. EXPONENTIAL BACKOFF
_retry_policy = retry(
    retry=retry_if_exception_type((gspread.exceptions.APIError, gspread.exceptions.GSpreadException)),
    wait=wait_exponential(multiplier=1, min=2, max=32),
    stop=stop_after_attempt(BACKOFF_MAX),
    before_sleep=before_sleep_log(_log, logging.WARNING),
    reraise=True,
)
def _gsheets_call(func, *args, **kwargs):
    @_retry_policy
    def _inner(): return func(*args, **kwargs)
    return _inner()


# 5. CSS
def inject_css() -> None:
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');
@import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,100..700,0,1');
:root {
  color-scheme: light only !important;
  --bg:#F7F8FC; --surface:#FFFFFF; --surface-2:#F0F2F9; --border:#E4E7F0; --border-2:#D0D5E8;
  --text-primary:#0D1117; --text-secondary:#4B5563; --text-muted:#9CA3AF;
  --indigo-50:#EEF2FF; --indigo-100:#E0E7FF; --indigo-400:#818CF8; --indigo-500:#6366F1;
  --indigo-600:#4F46E5; --indigo-700:#4338CA; --blue-400:#60A5FA; --blue-500:#3B82F6;
  --green-50:#F0FDF4; --green-200:#A7F3D0; --green-600:#16A34A; --green-700:#15803D;
  --amber-50:#FFFBEB; --amber-200:#FDE68A; --amber-700:#B45309;
  --red-50:#FFF1F2; --red-200:#FECDD3; --red-600:#DC2626;
  --radius-sm:6px; --radius-md:10px; --radius-lg:16px; --radius-full:9999px;
  --shadow-sm:0 1px 3px rgba(0,0,0,0.06),0 1px 2px rgba(0,0,0,0.04);
  --shadow-md:0 4px 12px rgba(0,0,0,0.08),0 2px 4px rgba(0,0,0,0.04);
  --shadow-lg:0 12px 32px rgba(0,0,0,0.10),0 4px 8px rgba(0,0,0,0.06);
  --ring:0 0 0 3px rgba(99,102,241,0.18);
  --font:'Plus Jakarta Sans',-apple-system,BlinkMacSystemFont,sans-serif;
  --mono:'JetBrains Mono','Courier New',monospace;
}
*,*::before,*::after{box-sizing:border-box!important;}
html,body,.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"],.main,.block-container{background-color:var(--bg)!important;color:var(--text-primary)!important;font-family:var(--font);}
p,span,div,li,label,h1,h2,h3,h4,h5,h6,.stMarkdown,[data-testid="stMarkdownContainer"]{color:var(--text-primary)!important;font-family:var(--font);}
#MainMenu,footer,header,.stDeployButton,[data-testid="stToolbar"],[data-testid="stSidebarCollapseButton"],[data-testid="collapsedControl"],[data-testid="stSidebar"]{display:none!important;}
.block-container{padding-top:0rem!important;margin-top:-2rem!important;}
header{visibility:hidden!important;}
.stTextInput>div>div>input,.stTextArea>div>div>textarea{background:var(--surface)!important;color:var(--text-primary)!important;-webkit-text-fill-color:var(--text-primary)!important;border:1.5px solid var(--border-2)!important;border-radius:var(--radius-md)!important;font-size:0.875rem!important;font-weight:500!important;padding:11px 14px!important;box-shadow:var(--shadow-sm)!important;}
.stSelectbox>div>div,[data-baseweb="select"]>div{background:#FFFFFF!important;background-color:#FFFFFF!important;color:#0D1117!important;border:1.5px solid var(--border-2)!important;border-radius:var(--radius-md)!important;min-height:42px!important;}
[data-baseweb="popover"],[data-baseweb="menu"],ul[role="listbox"]{background:#FFFFFF!important;background-color:#FFFFFF!important;border:1px solid var(--border)!important;box-shadow:var(--shadow-md)!important;}
[data-baseweb="menu"] li,[role="option"]{background-color:#FFFFFF!important;color:#0D1117!important;-webkit-text-fill-color:#0D1117!important;font-size:0.875rem!important;font-weight:600!important;}
[data-baseweb="menu"] li:hover,[data-baseweb="menu"] [aria-selected="true"],[role="option"]:hover{background-color:#EEF2FF!important;color:#4F46E5!important;-webkit-text-fill-color:#4F46E5!important;}
[data-testid="stMetricContainer"]{background:var(--surface)!important;border:1px solid var(--border)!important;border-top:3px solid var(--indigo-500)!important;border-radius:var(--radius-lg)!important;padding:22px 26px!important;box-shadow:var(--shadow-md)!important;}
[data-testid="stMetricValue"]{font-size:2.1rem!important;font-weight:800!important;color:var(--indigo-600)!important;}
[data-testid="stMetricLabel"]{font-size:0.68rem!important;font-weight:700!important;color:var(--text-muted)!important;}
.stButton>button{background:linear-gradient(135deg,var(--indigo-600) 0%,var(--blue-500) 100%)!important;color:#FFFFFF!important;-webkit-text-fill-color:#FFFFFF!important;border:none!important;border-radius:var(--radius-md)!important;font-weight:700!important;font-size:0.84rem!important;padding:10px 20px!important;}
div[data-testid="stForm"]{background:var(--surface)!important;border:1px solid var(--border)!important;border-radius:var(--radius-lg)!important;padding:28px 32px!important;}
.stTabs [data-baseweb="tab-list"]{gap:2px!important;background:var(--surface-2)!important;border:1px solid var(--border)!important;border-radius:var(--radius-full)!important;padding:4px!important;width:fit-content!important;box-shadow:var(--shadow-sm)!important;}
.stTabs [data-baseweb="tab"]{background:transparent!important;color:var(--text-muted)!important;border-radius:var(--radius-full)!important;padding:8px 22px!important;font-weight:600!important;}
.stTabs [aria-selected="true"]{background:var(--surface)!important;color:var(--indigo-600)!important;-webkit-text-fill-color:var(--indigo-600)!important;box-shadow:var(--shadow-sm)!important;}
.review-panel{background:linear-gradient(135deg,#EEF2FF 0%,#F0FDF4 100%);border:1.5px solid #C7D2FE;border-radius:14px;padding:22px 26px;margin-bottom:22px;box-shadow:0 4px 16px rgba(99,102,241,0.10);}
.review-panel-title{font-size:1.15rem;font-weight:800;color:#1E3A8A!important;margin-bottom:5px;}
.review-panel-meta{font-size:0.80rem;color:#4B5563!important;margin-bottom:0;}
.review-diff-table{width:100%;border-collapse:collapse;font-family:inherit;font-size:0.82rem;}
.review-diff-table thead tr{background:#F0F2F9;}
.review-diff-table th{padding:10px 14px;text-align:left;font-size:0.60rem;font-weight:800;text-transform:uppercase;letter-spacing:.08em;}
.review-diff-table td{padding:10px 14px;vertical-align:top;max-width:220px;white-space:pre-wrap;word-break:break-word;}
.rdt-field{background:#FAFAFA;color:#374151;font-weight:700;border-bottom:1px solid #E4E7F0;}
.rdt-old{background:#FFF1F2;color:#DC2626;border-bottom:1px solid #E4E7F0;border-left:1px solid #E4E7F0;text-decoration:line-through;}
.rdt-new{background:#F0FDF4;color:#15803D;border-bottom:1px solid #E4E7F0;border-left:1px solid #E4E7F0;font-weight:700;}
.rdt-same{background:#FFFFFF;color:#6B7280;}
.btn-back-wrap>button{background:#F0F2F9!important;color:#374151!important;-webkit-text-fill-color:#374151!important;border:1px solid #D0D5E8!important;}
.btn-confirm-wrap>button{background:linear-gradient(135deg,#15803D 0%,#16A34A 100%)!important;box-shadow:0 2px 10px rgba(21,128,61,0.35)!important;}
.project-banner{background:linear-gradient(135deg,var(--indigo-50) 0%,#F0F9FF 100%);border:1px solid var(--indigo-100);border-left:4px solid var(--indigo-500);border-radius:var(--radius-lg);padding:14px 20px;margin-bottom:20px;display:flex;align-items:center;gap:12px;}
.project-label{font-size:0.62rem;font-weight:800;letter-spacing:.10em;text-transform:uppercase;color:var(--indigo-600)!important;white-space:nowrap;}
.deep-search-strip{background:var(--surface);border:1px solid var(--border);border-left:4px solid var(--indigo-500);border-radius:var(--radius-md);padding:12px 20px 16px;margin-bottom:20px;}
.deep-search-title{font-size:.62rem;font-weight:800;color:var(--indigo-600)!important;margin-bottom:10px;}
.page-header{display:flex;align-items:center;justify-content:space-between;padding:4px 0 24px;border-bottom:1px solid var(--border);margin-bottom:28px;}
.page-title{font-size:1.55rem;font-weight:800;color:var(--text-primary)!important;margin:0;}
.page-subtitle{font-size:.78rem;color:var(--text-muted)!important;margin-top:4px;font-weight:500;}
.page-timestamp{font-size:.74rem;color:var(--text-muted)!important;font-weight:600;background:var(--surface);padding:7px 16px;border-radius:var(--radius-full);border:1px solid var(--border);}
.section-title{display:inline-flex;align-items:center;gap:8px;font-size:.70rem;font-weight:800;color:var(--indigo-600)!important;margin:24px 0 14px;padding:6px 14px 6px 10px;border-left:3px solid var(--indigo-500);background:var(--indigo-50);}
.worklist-header{display:flex;align-items:center;justify-content:space-between;background:var(--surface);border:1px solid var(--border);border-top:3px solid var(--indigo-500);border-radius:var(--radius-lg);padding:18px 24px;margin-bottom:18px;}
.log-summary-card{background:var(--surface);border:1px solid var(--border);border-top:3px solid var(--indigo-500);border-radius:var(--radius-lg);padding:20px 26px;margin-bottom:18px;}
.log-stat-row{display:flex;align-items:center;gap:28px;flex-wrap:wrap;}
.log-stat{display:flex;flex-direction:column;gap:2px;}
.log-stat-value{font-size:1.55rem;font-weight:800;color:var(--indigo-600)!important;}
.log-stat-label{font-size:.62rem;font-weight:700;color:var(--text-muted)!important;}
.log-stat-divider{width:1px;height:40px;background:var(--border);}
.export-strip{background:linear-gradient(135deg,#F0FDF4 0%,#EFF6FF 100%);border:1px solid var(--green-200);border-radius:var(--radius-md);padding:14px 18px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:20px;}
.prog-wrap{background:var(--border);border-radius:var(--radius-full);height:7px;overflow:hidden;margin:6px 0 12px;}
.prog-fill{height:100%;border-radius:var(--radius-full);background:linear-gradient(90deg,var(--indigo-600),var(--blue-400));}
.prog-labels{display:flex;justify-content:space-between;font-size:.72rem;color:var(--text-muted)!important;font-weight:600;margin-bottom:4px;}
.chip{display:inline-flex;align-items:center;gap:5px;padding:4px 12px;border-radius:var(--radius-full);font-size:.68rem;font-weight:700;}
.chip-done{background:var(--green-50);color:var(--green-700)!important;border:1px solid var(--green-200);}
.chip-pending{background:var(--amber-50);color:var(--amber-700)!important;border:1px solid var(--amber-200);}
.s-chip{display:inline-flex;align-items:center;padding:3px 10px;border-radius:var(--radius-full);font-size:.63rem;font-weight:700;}
.s-done{background:var(--green-50);color:var(--green-700)!important;border:1px solid var(--green-200);}
.s-pending{background:var(--amber-50);color:var(--amber-700)!important;border:1px solid var(--amber-200);}
.s-eval-good{background:var(--green-50);color:var(--green-700)!important;border:1px solid var(--green-200);}
.s-eval-Incorrect{background:var(--red-50);color:var(--red-600)!important;border:1px solid var(--red-200);}
.s-eval-dup{background:var(--amber-50);color:var(--amber-700)!important;border:1px solid var(--amber-200);}
.gov-table-wrap{overflow-x:auto;border:1px solid var(--border);border-radius:var(--radius-lg);margin-bottom:18px;}
.gov-table{width:100%;border-collapse:collapse;background:var(--surface);font-size:.84rem;}
.gov-table th{color:var(--text-muted)!important;background:var(--surface-2)!important;font-weight:700!important;padding:13px 18px!important;white-space:nowrap;text-align:left!important;}
.gov-table td{color:var(--text-primary)!important;background:var(--surface)!important;padding:11px 18px!important;border-bottom:1px solid var(--border)!important;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.gov-table tbody tr:nth-child(even) td{background:#FBFCFF!important;}
.gov-table tbody tr:hover td{background:var(--indigo-50)!important;}
.acc-table{width:100%;border-collapse:collapse;font-size:.83rem;}
.acc-table th{background:var(--indigo-50)!important;color:var(--indigo-600)!important;font-size:.62rem!important;font-weight:800!important;padding:11px 16px!important;border-bottom:2px solid var(--indigo-100)!important;text-align:left!important;}
.acc-table td{padding:10px 16px!important;border-bottom:1px solid var(--border)!important;vertical-align:middle!important;font-weight:500!important;color:var(--text-primary)!important;background:var(--surface)!important;}
.acc-table tbody tr:nth-child(even) td{background:#FBFCFF!important;}
.acc-table tbody tr:hover td{background:var(--indigo-50)!important;}
.acc-rate-high{color:var(--green-700)!important;font-weight:800!important;}
.acc-rate-mid{color:var(--amber-700)!important;font-weight:800!important;}
.acc-rate-low{color:var(--red-600)!important;font-weight:800!important;}
.acc-bar-wrap{background:var(--border);border-radius:var(--radius-full);height:6px;width:80px;display:inline-block;vertical-align:middle;margin-left:8px;}
.acc-bar-fill{height:100%;border-radius:var(--radius-full);}
.inspector-panel{background:var(--surface-2);border:1px solid var(--border);border-left:4px solid var(--indigo-500);border-radius:var(--radius-md);padding:18px 22px;margin-top:8px;}
.inspector-meta{font-size:.72rem;font-weight:700;color:var(--text-muted)!important;margin-bottom:10px;display:flex;gap:18px;flex-wrap:wrap;}
.inspector-meta span{color:var(--text-primary)!important;font-weight:600;}
.log-line{font-family:var(--mono)!important;font-size:.74rem;color:var(--text-secondary)!important;padding:6px 0;border-bottom:1px dashed var(--border);line-height:1.5;}
.divider{border:none;border-top:1px solid var(--border);margin:14px 0;}
.role-badge-admin{background:#EDE9FE;color:#6D28D9!important;border:1px solid #DDD6FE;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;display:inline-block;}
.role-badge-manager{background:#FFF7ED;color:#C2410C!important;border:1px solid #FED7AA;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;display:inline-block;}
.role-badge-auditor{background:#F0FDF4;color:#15803D!important;border:1px solid #A7F3D0;border-radius:var(--radius-full);padding:2px 10px;font-size:.60rem;font-weight:800;display:inline-block;}
.rbac-banner{background:var(--indigo-50);border:1px solid var(--indigo-100);border-left:3px solid var(--indigo-500);border-radius:var(--radius-md);padding:12px 18px;margin-bottom:18px;font-size:.80rem;color:var(--indigo-600)!important;font-weight:600;}
.link-btn button{background:transparent!important;color:#3B82F6!important;-webkit-text-fill-color:#3B82F6!important;border:none!important;box-shadow:none!important;font-size:0.85rem!important;padding:0!important;margin-top:15px!important;}
@media(max-width:768px){
  .page-header{flex-direction:column!important;align-items:flex-start!important;gap:12px!important;margin-bottom:15px!important;}
  .page-title{font-size:1.3rem!important;}
  .worklist-header{flex-direction:column!important;align-items:flex-start!important;gap:10px!important;padding:15px!important;}
  .log-stat-row{flex-direction:column!important;align-items:flex-start!important;gap:15px!important;}
  .log-stat-divider{display:none!important;}
  div[data-testid="stForm"]{padding:18px 20px!important;}
  .gov-table th,.acc-table th{padding:10px 12px!important;font-size:0.58rem!important;}
  .gov-table td,.acc-table td{padding:10px 12px!important;font-size:0.78rem!important;}
  .review-diff-table td,.review-diff-table th{padding:8px 10px!important;}
}
div[data-baseweb="select"]>div,div[data-baseweb="popover"]>div,div[data-baseweb="menu"],ul[role="listbox"]{background-color:#FFFFFF!important;}
div[data-baseweb="menu"] li,ul[role="listbox"] li,li[role="option"]{background-color:#FFFFFF!important;color:#0D1117!important;-webkit-text-fill-color:#0D1117!important;}
div[data-baseweb="menu"] li:hover,ul[role="listbox"] li:hover{background-color:#EEF2FF!important;color:#4F46E5!important;-webkit-text-fill-color:#4F46E5!important;}
</style>
""", unsafe_allow_html=True)


# 6. TRANSLATIONS
_LANG: dict[str, dict[str, str]] = {"en": {
    "ministry":"Ministry of Finance & Customs","portal_title":"Tax Audit & Compliance Portal",
    "portal_sub":"Authorised Access Only","classified":"CLASSIFIED - GOVERNMENT USE ONLY",
    "login_prompt":"Use your authorised credentials to access the system.",
    "email_field":"Official Email / User ID","password_field":"Password",
    "sign_in":"Sign in","sign_out":"Sign Out",
    "bad_creds":"Authentication failed. Verify your credentials and try again.",
    "workspace":"📁 Select Register (Tab)","overview":"Case Overview","project_select":"🏢 Select Project File",
    "total":"Total Cases","processed":"Processed","outstanding":"Outstanding",
    "worklist_title":"Audit Worklist","worklist_sub":"Active cases pending review",
    "tab_worklist":"Worklist","tab_archive":"Archive","tab_analytics":"Analytics",
    "tab_logs":"Auditor Logs","tab_users":"User Admin",
    "select_case":"Select a case to inspect","audit_trail":"Audit Trail",
    "approve_save":"Approve & Commit Record","reopen":"Re-open Record",
    "leaderboard":"Auditor Productivity Leaderboard","daily_trend":"Daily Processing Trend",
    "period":"Time Period","today":"Today","this_week":"This Week","this_month":"This Month","all_time":"All Time",
    "add_auditor":"Register New User","update_pw":"Update Password",
    "remove_user":"Revoke Access","staff_dir":"Authorised Staff",
    "no_records":"No records found for this period.","empty_sheet":"This register contains no data.",
    "saved_ok":"Record has been submitted and approved successfully.",
    "dup_email":"This email address is already registered.","fill_fields":"All fields are required.",
    "signed_as":"Authenticated as","role_admin":"System Administrator",
    "role_auditor":"Tax Auditor","role_manager":"Manager","processing":"Processing Case",
    "no_history":"No audit trail for this record.",
    "records_period":"Records (period)","active_days":"Active Days","avg_per_day":"Avg / Day",
    "adv_filters":"Advanced Filters","f_binder":"Company Binder No.","f_license":"License Number",
    "clear_filters":"Clear Filters","active_filters":"Active filters","results_shown":"results shown",
    "no_match":"No records match the applied filters.",
    "retry_warning":"Google Sheets quota reached - retrying with backoff...",
    "local_mode":"Optimistic UI Active","cache_age":"Cache TTL",
    "rbac_notice":"Info: Your role only has access to the Audit Worklist.",
    "logs_title":"Auditor Activity Logs","logs_sub":"Full processing history from project start",
    "logs_filter_all":"All Auditors","logs_auditor_sel":"Filter by Auditor",
    "logs_total":"Total Processed","logs_auditors":"Unique Auditors",
    "logs_date_range":"Date Range","logs_no_data":"No processed records found.",
    "logs_export_hdr":"Export Full Report","logs_export_sub":"Download the complete audit log as a CSV file.",
    "logs_export_btn":"Download CSV Report","logs_filename":"audit_log_report.csv","logs_cols_shown":"Columns displayed",
    "eval_label":"Data Entry Quality","feedback_label":"Auditor Feedback",
    "feedback_placeholder":"Optional notes, issues found, corrections made...",
    "acc_ranking_title":"Data Entry Accuracy Ranking","acc_agent":"Agent Email","acc_total":"Total",
    "acc_good":"Good","acc_bad":"Incorrect","acc_dup":"Dup","acc_rate":"Accuracy %",
    "acc_no_data":"No evaluation data available yet.",
    "archive_quality_note":"Tip: Columns Data_Evaluation & Correction_Notes are highlighted.",
    "role_label":"Role","change_role":"Change User Role",
    "change_role_sub":"Upgrade or downgrade any user's access level","role_updated":"Role updated successfully.",
    "deep_search":"Deep Search","ds_binder":"Binder No.","ds_agent":"Agent Email",
    "ds_company":"Company / Taxpayer","ds_clear":"Clear","ds_showing":"Showing results for",
    "eval_breakdown":"Evaluation Breakdown per Agent",
    "eval_breakdown_sub":"Stacked view: Good / Incorrect / Duplicate per data-entry agent",
    "arch_search_title":"Archive Quick Search","inspector_title":"Inspect Full Record Details",
    "inspector_select":"Select a record to inspect",
    "inspector_hint":"Choose a row from the table above to read its full audit trail and feedback notes.",
    "inspector_audit_trail":"Audit Trail (Full)","inspector_feedback":"Correction Notes / Auto-Diff (Full)",
    "inspector_empty_trail":"No audit trail recorded for this entry.",
    "inspector_empty_feedback":"No correction notes for this entry.",
    "inspector_no_log_col":"Audit_Log column not present in this view.",
}}
def t(key: str) -> str: return _LANG["en"].get(key, key)

# 7. HELPERS
@st.cache_data(ttl=3600, show_spinner=False)
def _get_column_keywords():
    return {
        "binder":["رقم ملف الشركة","رقم_ملف_الشركة","رقم ملف","ملف الشركة","ژمارەی بایندەری کۆمپانیا","ژمارەی بایندەری","بایندەری","binder","file no","file_no"],
        "company":["ناوی کۆمپانیا","اسم الشركة","اسم_الشركة","اسم الشركه","کۆمپانیای","کۆمپانیا","كومبانيا","شركة","company name","company_name","company","ناوی باجدەر","اسم دافع الضرائب","دافع الضرائب","taxpayer name","taxpayer"],
        "license":["رقم الترخيص","رقم_الترخيص","الترخيص","ژمارەی مۆڵەتی کۆمپانیا","ژمارەی مۆڵەتی","مۆڵەتی","مۆڵەت","license no","license_no","license","licence"],
        "agent_email":["data entry email","agent email","data_entry_email","agent_email","ئیمەیڵی ئەجنت","ئیمەیل ئەجنت","ئیمەیل داخڵکەر","email agent","داخلكننده","وارد کننده","email","ئیمەیل","ایمیل"],
    }

def detect_column(headers, kind):
    keywords = _get_column_keywords().get(kind, [])
    skip_cols = set(SYSTEM_COLS) if kind == "agent_email" else set()
    for h in headers:
        if h in skip_cols: continue
        hl = h.lower().strip()
        if kind == "company":
            bad_words = ["رقم","ملف","ژمارە","بایندەر","binder","file","status","حالة","نشاط","activity","مۆڵەت","license","ئەم","does","هل","مدينة","شار","کار دەکات"]
            if any(x in hl for x in bad_words): continue
        for kw in keywords:
            if kw.lower() in hl: return h
    return None

def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
def now_str(): return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
def parse_dt(s):
    try: return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except: return None
def clean_cell(value):
    if value is None: return ""
    s = str(value)
    for ch in ("\u200b","\u200c","\u200d","\ufeff"): s = s.replace(ch,"")
    return s.replace("\xa0"," ").strip()
_EVAL_EMOJI_STRIP = str.maketrans("","","\U0001f7e2\U0001f534\u26a0\ufe0f")
def _normalise_eval(raw: str) -> str: return raw.translate(_EVAL_EMOJI_STRIP).strip()

def _raw_to_dataframe(raw):
    if not raw: return pd.DataFrame(),[],{}
    seen={}; headers=[]
    for h in raw[0]:
        h = clean_cell(h) or "Unnamed"
        if h in seen: seen[h]+=1; headers.append(f"{h}_{seen[h]}")
        else: seen[h]=0; headers.append(h)
    if not headers: return pd.DataFrame(),[],{}
    n=len(headers); rows=[]
    for r in raw[1:]:
        row=[clean_cell(c) for c in r]; row=(row+[""]*n)[:n]; rows.append(row)
    if not rows: return pd.DataFrame(columns=headers),headers,{}
    df=pd.DataFrame(rows,columns=headers)
    df=df[~(df=="").all(axis=1)].reset_index(drop=True)
    for sc in SYSTEM_COLS:
        if sc not in df.columns: df[sc]=""
    df=df.fillna("").infer_objects(copy=False)
    return df,headers,{h:i+1 for i,h in enumerate(headers)}

def apply_period_filter(df,col,period):
    if period=="all" or col not in df.columns: return df
    now=datetime.now(TZ)
    if   period=="today":      cutoff=now.replace(hour=0,minute=0,second=0,microsecond=0)
    elif period=="this_week":  cutoff=(now-timedelta(days=now.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
    elif period=="this_month": cutoff=now.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    else: return df
    parsed=pd.to_datetime(df[col],format="%Y-%m-%d %H:%M:%S",errors="coerce")
    cutoff_ts=pd.Timestamp(cutoff).tz_localize(None)
    return df[parsed>=cutoff_ts]

def build_auto_diff(record,new_vals):
    lines=[]
    for field,new_v in new_vals.items():
        old_v=clean_cell(record.get(field,"")); new_v_clean=clean_cell(new_v)
        if old_v!=new_v_clean: lines.append(f"[{field}]:\n  WAS: {old_v!r}\n  NOW: {new_v_clean!r}")
    return ("Auto-Log:\n"+"\n".join(lines)) if lines else "Auto-Log: No field changes detected."

def _get_opts(df_in,col_name):
    if col_name and col_name in df_in.columns:
        s=df_in[col_name].astype(str).str.strip()
        return sorted(s[s!=""].unique().tolist())
    return []

def send_otp_email(to_email,otp_code):
    sender_email=st.secrets.get("smtp_email",""); sender_pw=st.secrets.get("smtp_password","")
    if not sender_email or not sender_pw: st.error("SMTP credentials not configured."); return False
    msg=MIMEMultipart("alternative"); msg["Subject"]="Tax Audit Portal - Password Reset Code"
    msg["From"]=f"Tax Audit Portal <{sender_email}>"; msg["To"]=to_email
    html_content=f"""<html><body style="font-family:Arial,sans-serif;background:#f4f4f5;padding:20px;">
    <div style="max-width:500px;margin:0 auto;background:#fff;padding:30px;border-radius:10px;border-top:4px solid #4F46E5;box-shadow:0 4px 6px rgba(0,0,0,0.1);">
    <h2 style="color:#1E3A8A;margin-top:0;">Password Reset Request</h2>
    <p style="color:#4B5563;font-size:16px;">Use the following OTP to continue:</p>
    <div style="background:#EEF2FF;padding:15px;text-align:center;border-radius:8px;margin:25px 0;">
    <span style="font-size:32px;font-weight:bold;letter-spacing:5px;color:#4F46E5;">{otp_code}</span></div>
    <p style="color:#6B7280;font-size:14px;">This code expires in 10 minutes.</p></div></body></html>"""
    msg.attach(MIMEText(html_content,"html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as server:
            server.login(sender_email,sender_pw); server.sendmail(sender_email,to_email,msg.as_string())
        return True
    except Exception as e: st.error(f"Failed to send email: {e}"); return False

def check_email_exists(email):
    email=email.lower().strip()
    try:
        records=_fetch_users_cached(); df_u=pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns: return False
        return not df_u[df_u["email"]==email].empty
    except: return False

def mask_email(email):
    email=email.strip()
    if not email or "@" not in email: return "***"
    local,domain=email.split("@",1)
    return f"{local[0]}***@{domain}"

def get_recovery_email(official_email):
    official_email=official_email.lower().strip()
    try:
        records=_fetch_users_cached(); df_u=pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns: return ""
        if "recovery_email" not in df_u.columns: return ""
        row=df_u[df_u["email"]==official_email]
        if row.empty: return ""
        val=str(row["recovery_email"].values[0]).strip()
        return val if val.lower() not in ("","nan","none") else ""
    except: return ""

# [v18] ALLOWED TABS HELPERS
def _parse_allowed_tabs(raw_str: str) -> list:
    if not raw_str or raw_str.lower().strip() in ("","nan","none"): return []
    return [tt.strip() for tt in raw_str.split(",") if tt.strip() in ALL_TAB_OPTIONS]

def _ensure_allowed_tabs_col() -> None:
    try:
        gc=get_gspread_client(); uws=gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
        header_row=_gsheets_call(uws.row_values,1)
        if "allowed_tabs" not in header_row:
            next_col=len(header_row)+1
            _gsheets_call(uws.update_cell,1,next_col,"allowed_tabs")
            _fetch_users_cached.clear()
    except: pass

def _ensure_can_reopen_col() -> None:
    try:
        gc=get_gspread_client(); uws=gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
        header_row=_gsheets_call(uws.row_values,1)
        if "can_reopen" not in header_row:
            next_col=len(header_row)+1
            _gsheets_call(uws.update_cell,1,next_col,"can_reopen")
            _fetch_users_cached.clear()
    except: pass

def _ensure_recovery_email_col() -> None:
    try:
        gc=get_gspread_client(); uws=gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
        header_row=_gsheets_call(uws.row_values,1)
        if "recovery_email" not in header_row:
            next_col=len(header_row)+1
            _gsheets_call(uws.update_cell,1,next_col,"recovery_email")
            _fetch_users_cached.clear()
    except: pass

# [v21] ENSURE NEW DATA ACCESS COLS
def _ensure_data_access_cols():
    try:
        gc=get_gspread_client(); uws=gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
        header_row=_gsheets_call(uws.row_values,1)
        updates = []
        if "allowed_projects" not in header_row: updates.append("allowed_projects")
        if "allowed_registers" not in header_row: updates.append("allowed_registers")
        if updates:
            for i, col_name in enumerate(updates):
                _gsheets_call(uws.update_cell, 1, len(header_row)+1+i, col_name)
            _fetch_users_cached.clear()
    except: pass

# 8. GOOGLE SHEETS CLIENT
@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    try:
        scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        raw=dict(st.secrets["json_key"]); pk=str(raw.get("private_key",""))
        pk=pk.replace("-----BEGIN PRIVATE KEY-----","").replace("-----END PRIVATE KEY-----","")
        pk=pk.replace("\\n","").replace("\n",""); pk="".join(pk.split())
        pk="\n".join(textwrap.wrap(pk,64))
        raw["private_key"]=f"-----BEGIN PRIVATE KEY-----\n{pk}\n-----END PRIVATE KEY-----\n"
        creds=ServiceAccountCredentials.from_json_keyfile_dict(raw,scope)
        return gspread.authorize(creds)
    except Exception as e: st.error(f"Google Sheets connection error: {e}"); raise

@st.cache_data(ttl=READ_TTL,show_spinner=False)
def _fetch_sheet_metadata(sid):
    gc=get_gspread_client(); spr=gc.open_by_key(sid)
    # لێرەدا _gsheets_call بەکاردەهێنین بۆ ئەوەی ئەگەر بلۆک کرا، خۆی کەمێک بوەستێت و هەوڵ بداتەوە
    worksheets = _gsheets_call(spr.worksheets)
    return [ws.title for ws in worksheets]

@st.cache_data(ttl=READ_TTL,show_spinner=False)
def _fetch_raw_sheet_cached(spreadsheet_id, ws_title, custom_range=None):
    gc=get_gspread_client(); spr=gc.open_by_key(spreadsheet_id); ws=spr.worksheet(ws_title)
    if custom_range:
        return _gsheets_call(ws.get, custom_range), now_str()
    return _gsheets_call(ws.get_all_values), now_str()

@st.cache_data(ttl=READ_TTL,show_spinner=False)
def _fetch_users_cached():
    gc=get_gspread_client(); spr=gc.open_by_key(MASTER_USERS_ID); ws=spr.worksheet(USERS_SHEET)
    return _gsheets_call(ws.get_all_records)

# PROJECTS DB
PROJECTS_SHEET="ProjectsDB"; PROJECTS_COLS=["Project_Name","Sheet_ID","Visible_To","Is_Active"]
VISIBILITY_OPTIONS=["admin","admin,manager","admin,manager,auditor"]

@st.cache_data(ttl=READ_TTL,show_spinner=False)
def _fetch_projects_cached() -> pd.DataFrame:
    try:
        gc=get_gspread_client(); spr=gc.open_by_key(MASTER_USERS_ID)
        try: ws=spr.worksheet(PROJECTS_SHEET)
        except gspread.exceptions.WorksheetNotFound:
            ws=spr.add_worksheet(title=PROJECTS_SHEET,rows="100",cols="4")
            _gsheets_call(ws.append_row,PROJECTS_COLS); return pd.DataFrame(columns=PROJECTS_COLS)
        records=_gsheets_call(ws.get_all_records)
        if not records: return pd.DataFrame(columns=PROJECTS_COLS)
        df=pd.DataFrame(records)
        for col in PROJECTS_COLS:
            if col not in df.columns: df[col]=""
        df["Is_Active"]=df["Is_Active"].astype(str).str.strip().str.upper().isin(["TRUE","1","YES"])
        df["Project_Name"]=df["Project_Name"].astype(str).str.strip()
        df["Sheet_ID"]=df["Sheet_ID"].astype(str).str.strip()
        df["Visible_To"]=df["Visible_To"].astype(str).str.strip().str.lower()
        return df
    except Exception as e: st.warning(f"Could not load ProjectsDB: {e}"); return pd.DataFrame(columns=PROJECTS_COLS)

# RANGES DB
@st.cache_data(ttl=READ_TTL, show_spinner=False)
def _fetch_ranges_cached() -> list:
    try:
        gc  = get_gspread_client()
        spr = gc.open_by_key(MASTER_USERS_ID)
        try:
            ws = spr.worksheet(RANGES_SHEET)
        except gspread.exceptions.WorksheetNotFound:
            ws = spr.add_worksheet(title=RANGES_SHEET, rows="200", cols="3")
            _gsheets_call(ws.append_row, RANGES_COLS)
            return []
        records = _gsheets_call(ws.get_all_records)
        return records if records else []
    except Exception as e:
        _log.warning(f"Could not load RangesDB: {e}")
        return []

def _get_custom_range(spreadsheet_id: str, ws_title: str):
    try:
        records = _fetch_ranges_cached()
        if not records: return None
        df_r = pd.DataFrame(records)
        if df_r.empty or "Sheet_ID" not in df_r.columns or "Tab_Name" not in df_r.columns: return None
        match = df_r[
            (df_r["Sheet_ID"].astype(str).str.strip() == str(spreadsheet_id).strip()) &
            (df_r["Tab_Name"].astype(str).str.strip() == str(ws_title).strip())
        ]
        if match.empty: return None
        rng = str(match["Read_Range"].values[0]).strip()
        return rng if rng and rng.lower() not in ("", "nan", "none") else None
    except:
        return None

def get_visible_projects(user_role):
    df=_fetch_projects_cached(); active=df[df["Is_Active"]==True] if not df.empty else pd.DataFrame()
    if active.empty: return PROJECTS
    result={}
    for _,row in active.iterrows():
        allowed_roles={r.strip() for r in str(row["Visible_To"]).split(",")}
        if user_role in allowed_roles or user_role=="admin":
            name=row["Project_Name"]; sid=row["Sheet_ID"]
            if name and sid: result[name]=sid
    return result

def save_projects_to_sheet(df_edited):
    try:
        gc=get_gspread_client(); spr=gc.open_by_key(MASTER_USERS_ID)
        try: ws=spr.worksheet(PROJECTS_SHEET)
        except gspread.exceptions.WorksheetNotFound:
            ws=spr.add_worksheet(title=PROJECTS_SHEET,rows="100",cols="4")
        df_out=df_edited.copy()
        df_out["Is_Active"]=df_out["Is_Active"].astype(bool).map({True:"TRUE",False:"FALSE"})
        df_out["Project_Name"]=df_out["Project_Name"].astype(str).str.strip()
        df_out["Sheet_ID"]=df_out["Sheet_ID"].astype(str).str.strip()
        df_out["Visible_To"]=df_out["Visible_To"].astype(str).str.strip().str.lower()
        df_out=df_out[PROJECTS_COLS]
        _gsheets_call(ws.clear); rows=[PROJECTS_COLS]+df_out.values.tolist()
        _gsheets_call(ws.update,"A1",rows)
        _fetch_projects_cached.clear(); _fetch_sheet_metadata.clear()
        return True,f"✅ Saved {len(df_out)} project(s) successfully."
    except gspread.exceptions.APIError as e: return False,f"Google Sheets API error: {e}"
    except Exception as e: return False,f"Unexpected error: {e}"

def _data_fingerprint(raw): return hashlib.md5(str(raw[:20]).encode()).hexdigest()

def get_local_data(sid, ws_title):
    custom_range = _get_custom_range(sid, ws_title)
    raw, fetched_at = _fetch_raw_sheet_cached(sid, ws_title, custom_range)
    fp=_data_fingerprint(raw); ck=f"{sid}::{ws_title}::{fp}"
    if st.session_state.get("local_cache_key")!=ck:
        df,h,cm=_raw_to_dataframe(raw)
        st.session_state.local_df=df.copy(); st.session_state.local_headers=h
        st.session_state.local_col_map=cm; st.session_state.local_cache_key=ck
        st.session_state.local_fetched_at=fetched_at
    return (st.session_state.local_df,st.session_state.local_headers,
            st.session_state.local_col_map,st.session_state.local_fetched_at or fetched_at)

# 9. OPTIMISTIC MUTATIONS
def _apply_optimistic_approve(df_iloc,new_vals,auditor,ts_now,log_prefix,eval_val="",feedback_val=""):
    ldf=st.session_state.local_df
    if df_iloc<0 or df_iloc>=len(ldf): return
    for f,v in new_vals.items():
        if f in ldf.columns: ldf.at[df_iloc,f]=v
    old=str(ldf.at[df_iloc,COL_LOG]).strip() if COL_LOG in ldf.columns else ""
    ldf.at[df_iloc,COL_STATUS]=VAL_DONE; ldf.at[df_iloc,COL_AUDITOR]=auditor; ldf.at[df_iloc,COL_DATE]=ts_now
    if COL_LOG in ldf.columns: ldf.at[df_iloc,COL_LOG]=f"{log_prefix}\n{old}".strip()
    if COL_EVAL in ldf.columns: ldf.at[df_iloc,COL_EVAL]=eval_val
    if COL_FEEDBACK in ldf.columns: ldf.at[df_iloc,COL_FEEDBACK]=feedback_val
    st.session_state.local_df=ldf

def _apply_optimistic_reopen(df_iloc):
    ldf=st.session_state.local_df
    if df_iloc<0 or df_iloc>=len(ldf): return
    ldf.at[df_iloc,COL_STATUS]=VAL_PENDING; st.session_state.local_df=ldf

# 10. WRITE HELPERS
def ensure_system_cols_in_sheet(ws,headers,col_map):
    for sc in SYSTEM_COLS:
        if sc not in col_map:
            np_=len(headers)+1
            if np_>ws.col_count: _gsheets_call(ws.add_cols,max(4,np_-ws.col_count+1))
            _gsheets_call(ws.update_cell,1,np_,sc); headers.append(sc); col_map[sc]=np_
    return headers,col_map

def write_approval_to_sheet(sid, ws_title, sheet_row, col_map, headers, new_vals, record, auditor, ts_now, log_prefix, eval_val="", feedback_val=""):
    # یەکجار پەیوەندی بە گوگڵەوە دەکەین بۆ کردنەوەی فایلەکە
    gc = get_gspread_client()
    spr = _gsheets_call(gc.open_by_key, sid)
    ws = _gsheets_call(spr.worksheet, ws_title)
    
    # 🔴 ئەمە ئەو دێڕەیە کە بیرمان چووبوو! پێویستە لێرە بێت بۆ دروستکردنی ستوونەکان 🔴
    headers, col_map = ensure_system_cols_in_sheet(ws, headers, col_map)
    
    # ئامادەکردنی مێژووی نووسین (Audit Log)
    old = str(record.get(COL_LOG, "")).strip()
    new_log = f"{log_prefix}\n{old}".strip()
    if len(new_log) > 49000: new_log = new_log[:48900] + "\n... [TRUNCATED]"
    
    # کۆکردنەوەی هەموو گۆڕانکارییەکان لە یەک لیستدا (Batch Update)
    batch = []
    
    # گۆڕانکارییەکانی ناو کێڵگەکان (Fields)
    for f, v in new_vals.items():
        if f in col_map and clean_cell(record.get(f, "")) != v:
            batch.append({"range": rowcol_to_a1(sheet_row, col_map[f]), "values": [[v]]})
            
    # گۆڕانکاری ستوونە بنەڕەتییەکان (Status, Auditor, Date, Eval, Feedback)
    sys_updates = [
        (COL_STATUS, VAL_DONE), (COL_AUDITOR, auditor), 
        (COL_DATE, ts_now), (COL_LOG, new_log), 
        (COL_EVAL, eval_val), (COL_FEEDBACK, feedback_val)
    ]
    
    for cn, v in sys_updates:
        if cn in col_map:
            batch.append({"range": rowcol_to_a1(sheet_row, col_map[cn]), "values": [[v]]})
    
    # ناردنی هەموو گۆڕانکارییەکان بەیەکەوە لە یەک چرکەدا
    if batch:
        _gsheets_call(ws.batch_update, batch)
    return True
def write_reopen_to_sheet(sid,ws_title,sheet_row,col_map):
    gc=get_gspread_client(); ws=gc.open_by_key(sid).worksheet(ws_title)
    if COL_STATUS in col_map: _gsheets_call(ws.update_cell,sheet_row,col_map[COL_STATUS],VAL_PENDING)

# AUTHENTICATE [UPDATED v21]
def authenticate(email: str, password: str) -> tuple:
    email=email.lower().strip()
    admin_pw=st.secrets.get("admin_password","")
    if email=="admin" and admin_pw and password==admin_pw:
        return "admin", False, list(ALL_TAB_OPTIONS), ["ALL"], ["ALL"]
    try:
        records=_fetch_users_cached()
        df_u=pd.DataFrame(records)
        if df_u.empty or "email" not in df_u.columns: return None,False,[],["ALL"],["ALL"]
        row=df_u[df_u["email"]==email]
        if row.empty: return None,False,[],["ALL"],["ALL"]
        stored_pw=str(row["password"].values[0])
        if hash_pw(password)!=stored_pw: return None,False,[],["ALL"],["ALL"]
        
        role=str(row["role"].values[0])
        needs_reset=False
        if "force_reset" in df_u.columns:
            raw_flag=str(row["force_reset"].values[0]).strip().upper()
            needs_reset=(raw_flag=="TRUE")
            
        allowed_tabs_list=[]
        if "allowed_tabs" in df_u.columns:
            raw_tabs=str(row["allowed_tabs"].values[0]).strip()
            allowed_tabs_list=_parse_allowed_tabs(raw_tabs)
            
        # Parse allowed_projects
        if "allowed_projects" in df_u.columns:
            ap_raw = str(row["allowed_projects"].values[0]).strip()
            allowed_projects = ["ALL"] if ap_raw.upper() == "ALL" or not ap_raw else [x.strip() for x in ap_raw.split(",") if x.strip()]
        else: allowed_projects = ["ALL"]
        
        # Parse allowed_registers
        if "allowed_registers" in df_u.columns:
            ar_raw = str(row["allowed_registers"].values[0]).strip()
            allowed_registers = ["ALL"] if ar_raw.upper() == "ALL" or not ar_raw else [x.strip() for x in ar_raw.split(",") if x.strip()]
        else: allowed_registers = ["ALL"]

        return role, needs_reset, allowed_tabs_list, allowed_projects, allowed_registers
    except: return None,False,[],["ALL"],["ALL"]


# 11. HTML TABLE & PAGINATION
def _eval_chip(raw):
    if not raw or raw=="-": return "-"
    n=_normalise_eval(raw)
    if "Good" in n: return f"<span class='s-chip s-eval-good'>{_html.escape(raw)}</span>"
    if "Incorrect" in n: return f"<span class='s-chip s-eval-Incorrect'>{_html.escape(raw)}</span>"
    if "Duplicate" in n: return f"<span class='s-chip s-eval-dup'>{_html.escape(raw)}</span>"
    return f"<span class='s-chip s-pending'>{_html.escape(raw)}</span>"

def render_html_table(df,max_rows=500):
    if df.empty: st.info("No records to display."); return
    display_df=df.head(max_rows)
    th="<th class='row-idx'>#</th>"
    for col in display_df.columns:
        if col==COL_LOG: continue
        extra_cls=""
        if col==COL_EVAL: extra_cls=" class='col-eval'"
        elif col==COL_FEEDBACK: extra_cls=" class='col-feedback'"
        th+=f"<th{extra_cls}>{_html.escape(col)}</th>"
    rows=""
    for idx,row in display_df.iterrows():
        r=f"<td class='row-idx'>{idx}</td>"
        for col in display_df.columns:
            if col==COL_LOG: continue
            raw=str(row[col]) if row[col]!="" else ""; safe=_html.escape(raw); d=safe or "-"
            if col==COL_STATUS:
                d=("<span class='s-chip s-done'>Processed</span>" if raw==VAL_DONE else "<span class='s-chip s-pending'>Pending</span>")
            elif col==COL_EVAL: d=_eval_chip(raw); r+=f"<td class='col-eval'>{d}</td>"; continue
            elif col==COL_FEEDBACK:
                trunc=(safe[:160]+"...") if len(safe)>160 else (safe or "-"); r+=f"<td class='col-feedback'>{trunc}</td>"; continue
            elif len(raw)>55: d=f"<span title='{safe}'>{safe[:52]}...</span>"
            r+=f"<td>{d}</td>"
        rows+=f"<tr>{r}</tr>"
    st.markdown(f"<div class='gov-table-wrap'><table class='gov-table'><thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table></div>",unsafe_allow_html=True)

def render_paginated_table(df,page_key,max_rows=5000):
    if df.empty: render_html_table(df); return
    if page_key not in st.session_state: st.session_state[page_key]=1
    total_rows=min(len(df),max_rows); total_pages=max(1,-(-total_rows//_PAGE_SIZE))
    st.session_state[page_key]=max(1,min(st.session_state[page_key],total_pages))
    current=st.session_state[page_key]; start=(current-1)*_PAGE_SIZE; end=min(start+_PAGE_SIZE,total_rows)
    render_html_table(df.iloc[start:end],max_rows=_PAGE_SIZE)
    if total_pages>1:
        col_prev,col_info,col_next=st.columns([1,3,1])
        with col_prev:
            if st.button("Prev",key=f"{page_key}_prev",disabled=(current<=1),use_container_width=True):
                st.session_state[page_key]-=1; st.rerun()
        with col_info:
            st.markdown(f"<div style='text-align:center;padding:8px 0;font-size:.75rem;font-weight:700;color:var(--text-muted);font-family:var(--mono);'>Page {current} of {total_pages} <span style='font-weight:400;margin-left:8px;'>({start+1}-{end} of {total_rows} rows)</span></div>",unsafe_allow_html=True)
        with col_next:
            if st.button("Next",key=f"{page_key}_next",disabled=(current>=total_pages),use_container_width=True):
                st.session_state[page_key]+=1; st.rerun()

# 12. LOGIN
def render_login(cookie_manager) -> None:
    if "show_forgot_pw" not in st.session_state: st.session_state.show_forgot_pw=False
    if "fp_mode" not in st.session_state: st.session_state.fp_mode="email"
    st.markdown("""<style>
    [data-testid="stSidebar"],[data-testid="collapsedControl"],header{display:none!important;}
    .stApp{background:linear-gradient(-45deg,#0F172A,#1E3A8A,#3B82F6,#1E40AF);background-size:400% 400%;animation:gradientBG 15s ease infinite;}
    @keyframes gradientBG{0%{background-position:0% 50%;}50%{background-position:100% 50%;}100%{background-position:0% 50%;}}
    .block-container{display:flex;flex-direction:column;justify-content:center;align-items:center;min-height:100vh;padding:1rem!important;}
    [data-testid="stForm"]{background:rgba(255,255,255,0.95)!important;backdrop-filter:blur(12px)!important;border:1px solid rgba(255,255,255,0.3)!important;border-radius:24px!important;padding:40px 30px!important;box-shadow:0 25px 50px -12px rgba(0,0,0,0.5)!important;max-width:420px!important;width:100%!important;margin:0 auto!important;}
    [data-testid="stFormSubmitButton"] button{background:linear-gradient(135deg,#1E3A8A 0%,#3B82F6 100%)!important;color:white!important;border:none!important;border-radius:12px!important;font-weight:bold!important;font-size:1rem!important;padding:0.6rem!important;width:100%!important;margin-top:10px!important;}
    </style>""",unsafe_allow_html=True)

    if st.session_state.show_forgot_pw:
        mode=st.session_state.fp_mode
        with st.form("forgot_pw_form",clear_on_submit=True):
            st.markdown("""<div style="text-align:center;font-size:2.5rem;margin-bottom:8px;">🔐</div>
            <div style="text-align:center;font-size:1.4rem;font-weight:800;color:#0F172A;margin-bottom:20px;">Reset Password</div>""",unsafe_allow_html=True)
            if mode=="email":
                st.markdown("<div style='text-align:center;font-size:0.85rem;color:#475569;margin-bottom:15px;'>Enter your official government email. The OTP will be sent to your registered recovery email.</div>",unsafe_allow_html=True)
                email_input=st.text_input("Official Email",key="fp_req_email",placeholder="user@agents.tax.gov.krd")
                submit_btn=st.form_submit_button("Send OTP to Recovery Email")
                if submit_btn:
                    official=email_input.lower().strip()
                    if check_email_exists(official):
                        recovery=get_recovery_email(official)
                        if not recovery:
                            st.error("⚠️ No recovery email registered. Please contact your administrator.")
                        else:
                            otp=str(random.randint(100000,999999))
                            if send_otp_email(recovery,otp):
                                st.session_state.fp_email=official; st.session_state.fp_recovery_email=recovery
                                st.session_state.fp_otp=otp; st.session_state.fp_otp_expiry=datetime.now()+timedelta(minutes=10)
                                st.session_state.fp_mode="otp"; st.rerun()
                    else: st.error("This email is not registered in the system.")
            elif mode=="otp":
                masked=mask_email(st.session_state.get("fp_recovery_email",""))
                st.markdown(f"<div style='text-align:center;font-size:0.85rem;color:#475569;margin-bottom:15px;'>We sent a 6-digit code to: <b style='color:#4F46E5;'>{_html.escape(masked)}</b></div>",unsafe_allow_html=True)
                otp_input=st.text_input("Enter 6-Digit OTP",key="fp_req_otp",max_chars=6)
                submit_btn=st.form_submit_button("Verify Code")
                if submit_btn:
                    if datetime.now()>st.session_state.fp_otp_expiry:
                        st.error("OTP has expired."); st.session_state.fp_mode="email"; time.sleep(2); st.rerun()
                    elif otp_input.strip()==st.session_state.fp_otp:
                        st.session_state.fp_mode="new_pw"; st.rerun()
                    else: st.error("Invalid OTP code.")
            elif mode=="new_pw":
                st.markdown("<div style='text-align:center;font-size:0.85rem;color:#475569;margin-bottom:15px;'>Code verified! Enter your new password below.</div>",unsafe_allow_html=True)
                new_pw=st.text_input("New Password",type="password",key="fp_new_pw1")
                new_pw_conf=st.text_input("Confirm New Password",type="password",key="fp_new_pw2")
                submit_btn=st.form_submit_button("Save New Password")
                if submit_btn:
                    if len(new_pw)<6: st.error("Password must be at least 6 characters.")
                    elif new_pw!=new_pw_conf: st.error("Passwords do not match.")
                    else:
                        try:
                            gc=get_gspread_client(); uws=gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
                            header_row=_gsheets_call(uws.row_values,1); cell=_gsheets_call(uws.find,st.session_state.fp_email)
                            if cell and "password" in header_row:
                                pw_idx=header_row.index("password")+1
                                _gsheets_call(uws.update_cell,cell.row,pw_idx,hash_pw(new_pw.strip()))
                                if "force_reset" in header_row:
                                    fr_idx=header_row.index("force_reset")+1
                                    _gsheets_call(uws.update_cell,cell.row,fr_idx,"FALSE")
                                _fetch_users_cached.clear()
                                st.success("Password changed! Redirecting…")
                                st.session_state.show_forgot_pw=False; st.session_state.fp_recovery_email=""
                                time.sleep(2); st.rerun()
                            else: st.error("Database error.")
                        except Exception as e: st.error(f"System error: {e}")
        st.markdown("<div class='link-btn'>",unsafe_allow_html=True)
        if st.button("⬅️ Back to Login",key="fp_back"):
            st.session_state.show_forgot_pw=False; st.session_state.fp_mode="email"; st.session_state.fp_recovery_email=""; st.rerun()
        st.markdown("</div>",unsafe_allow_html=True)

    elif st.session_state.get("must_reset_pw"):
        with st.form("force_reset_form"):
            st.markdown("""<div style="text-align:center;font-size:2.5rem;margin-bottom:8px;">🔐</div>
            <div style="text-align:center;font-size:1.4rem;font-weight:800;color:#0F172A;margin-bottom:10px;">Set Your Password</div>
            <div style="text-align:center;font-size:0.85rem;color:#475569;margin-bottom:20px;">You are using a temporary password. Please create a new one to continue.</div>""",unsafe_allow_html=True)
            new_pw=st.text_input("New Password",type="password")
            new_pw_conf=st.text_input("Confirm New Password",type="password")
            submitted=st.form_submit_button("Update Password & Login")
            if submitted:
                if len(new_pw)<6: st.error("Password must be at least 6 characters.")
                elif new_pw!=new_pw_conf: st.error("Passwords do not match.")
                else:
                    try:
                        em=st.session_state.get("reset_email",""); role=st.session_state.get("reset_role","auditor")
                        gc=get_gspread_client(); uws=gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
                        header_row=_gsheets_call(uws.row_values,1); cell=_gsheets_call(uws.find,em)
                        if cell and "password" in header_row:
                            pw_idx=header_row.index("password")+1
                            _gsheets_call(uws.update_cell,cell.row,pw_idx,hash_pw(new_pw.strip()))
                            if "force_reset" in header_row:
                                fr_idx=header_row.index("force_reset")+1; _gsheets_call(uws.update_cell,cell.row,fr_idx,"FALSE")
                            _fetch_users_cached.clear()
                            display_email=em.lower().strip()
                            st.session_state.logged_in=True; st.session_state.user_email=display_email
                            st.session_state.user_role=role; st.session_state.must_reset_pw=False
                            st.session_state.allowed_tabs=st.session_state.get("reset_allowed_tabs",[])
                            st.session_state.allowed_projects=st.session_state.get("reset_allowed_projects",["ALL"])
                            st.session_state.allowed_registers=st.session_state.get("reset_allowed_registers",["ALL"])
                            try:
                                cookie_manager.set(_COOKIE_NAME,f"{display_email}|{role}",
                                    expires_at=datetime.now()+timedelta(days=1),key="force_reset_set_cookie")
                            except: pass
                            st.success("Password updated! Logging you in…"); time.sleep(1); st.rerun()
                        else: st.error("Error updating database.")
                    except Exception as e: st.error(f"System error: {e}")

    else:
        with st.form("login_form",clear_on_submit=False):
            st.markdown(f"""<div style="text-align:center;font-size:3rem;margin-bottom:8px;line-height:1;">&#127963;</div>
            <div style="text-align:center;font-size:1.5rem;font-weight:800;color:#0F172A;margin-bottom:4px;">{_html.escape(t('portal_title'))}</div>
            <div style="text-align:center;font-size:.60rem;font-weight:700;color:#DC2626;background:#FEE2E2;padding:4px 10px;border-radius:99px;width:max-content;margin:0 auto 16px;letter-spacing:1px;">{_html.escape(t('classified'))}</div>
            <div style="text-align:center;font-size:.85rem;color:#475569;margin-bottom:20px;">{_html.escape(t('login_prompt'))}</div>""",unsafe_allow_html=True)
            st.text_input(t("email_field"),placeholder="user@agents.tax.gov.krd",key="_login_email")
            st.text_input(t("password_field"),type="password",placeholder="••••••••••",key="_login_pw")
            submitted=st.form_submit_button(t("sign_in"))
        if submitted:
            em=st.session_state.get("_login_email",""); pw=st.session_state.get("_login_pw","")
            auth_res = authenticate(em,pw)
            if auth_res and auth_res[0]:
                role,needs_reset,allowed_tabs_list, allowed_projects, allowed_registers = auth_res
                if needs_reset:
                    st.session_state.must_reset_pw=True; st.session_state.reset_email=em.lower().strip()
                    st.session_state.reset_role=role; st.session_state.reset_allowed_tabs=allowed_tabs_list
                    st.session_state.reset_allowed_projects=allowed_projects
                    st.session_state.reset_allowed_registers=allowed_registers
                    st.rerun()
                else:
                    display_email="Admin" if role=="admin" else em.lower().strip()
                    st.session_state.logged_in=True; st.session_state.user_email=display_email
                    st.session_state.user_role=role; st.session_state.allowed_tabs=allowed_tabs_list
                    st.session_state.allowed_projects=allowed_projects
                    st.session_state.allowed_registers=allowed_registers
                    try:
                        cookie_manager.set(_COOKIE_NAME,f"{display_email}|{role}",
                            expires_at=datetime.now()+timedelta(days=1),key="login_set_cookie")
                    except: pass
                    st.rerun()
            else: st.error(t("bad_creds"))
        st.markdown("<div class='link-btn'>",unsafe_allow_html=True)
        if st.button("Forgot Password?",key="forgot_pw_btn"):
            st.session_state.show_forgot_pw=True; st.session_state.fp_mode="email"; st.session_state.fp_recovery_email=""; st.rerun()
        st.markdown("</div>",unsafe_allow_html=True)


# DEEP SEARCH WIDGET
def render_deep_search_strip(key_prefix,col_binder,col_agent_email,col_company,binder_options=None,agent_options=None,company_options=None):
    def _clear():
        st.session_state[f"{key_prefix}_binder"]=None; st.session_state[f"{key_prefix}_agent"]=None; st.session_state[f"{key_prefix}_company"]=None
        for pk in ("page_worklist","page_archive","page_logs"):
            if pk in st.session_state: st.session_state[pk]=1
    st.markdown(f"<div class='deep-search-strip'><div class='deep-search-title'>{t('deep_search')}</div></div>",unsafe_allow_html=True)
    try: c1,c2,c3,c4=st.columns([1,1.5,1,0.32],gap="small",vertical_alignment="bottom"); has_valign=True
    except TypeError: c1,c2,c3,c4=st.columns([1,1.5,1,0.32],gap="small"); has_valign=False
    with c1:
        if binder_options: st.selectbox(t("ds_binder"),options=binder_options,key=f"{key_prefix}_binder",index=None,placeholder="🔍 Search Binder...",disabled=(col_binder is None),label_visibility="collapsed")
        else: st.text_input(t("ds_binder"),key=f"{key_prefix}_binder",placeholder="🔍 Binder No...",disabled=(col_binder is None),label_visibility="collapsed")
    with c2:
        if company_options: st.selectbox(t("ds_company"),options=company_options,key=f"{key_prefix}_company",index=None,placeholder="🔍 Search Company...",disabled=(col_company is None),label_visibility="collapsed")
        else: st.text_input(t("ds_company"),key=f"{key_prefix}_company",placeholder="🔍 Company Name...",disabled=(col_company is None),label_visibility="collapsed")
    with c3:
        if agent_options: st.selectbox(t("ds_agent"),options=agent_options,key=f"{key_prefix}_agent",index=None,placeholder="🔍 Search Agent...",disabled=(col_agent_email is None),label_visibility="collapsed")
        else: st.text_input(t("ds_agent"),key=f"{key_prefix}_agent",placeholder="🔍 Agent Name...",disabled=(col_agent_email is None),label_visibility="collapsed")
    with c4:
        if not has_valign: st.markdown('<div style="margin-top:0px;"></div>',unsafe_allow_html=True)
        st.button(t("ds_clear"),key=f"{key_prefix}_clr",use_container_width=True,on_click=_clear)
    return (st.session_state.get(f"{key_prefix}_binder") or "",st.session_state.get(f"{key_prefix}_agent") or "",st.session_state.get(f"{key_prefix}_company") or "")

def apply_deep_search(df,srch_binder,srch_agent,srch_company,col_binder,col_agent_email,col_company):
    if df.empty: return df
    mask=pd.Series(True,index=df.index)
    if srch_binder.strip() and col_binder and col_binder in df.columns: mask&=df[col_binder].astype(str).str.strip()==srch_binder.strip()
    if srch_agent.strip() and col_agent_email and col_agent_email in df.columns: mask&=df[col_agent_email].astype(str).str.contains(srch_agent.strip(),case=False,na=False)
    if srch_company.strip() and col_company and col_company in df.columns: mask&=df[col_company].astype(str).str.contains(srch_company.strip(),case=False,na=False)
    return df[mask]

def _deep_search_active(b,a,c): return any(x.strip() for x in (b,a,c))

# REVIEW STATE HELPERS
def _clear_review_state():
    st.session_state.review_mode=False; st.session_state.review_row=None; st.session_state.review_new_vals=None
    st.session_state.review_eval_val=""; st.session_state.review_manual_notes=""; st.session_state.review_record=None
    st.session_state.review_df_iloc=None; st.session_state.review_ws_title=None; st.session_state.review_sid=None

def _resolve_form_values(fields,sheet_row,combo_keys):
    MANUAL_SENTINEL="-- Type manually --"; new_vals={}
    for fname in fields:
        if fname in combo_keys:
            sel_val=st.session_state.get(f"sel_{sheet_row}_{fname}",MANUAL_SENTINEL)
            txt_val=st.session_state.get(f"txt_{sheet_row}_{fname}","")
            new_vals[fname]=txt_val if sel_val==MANUAL_SENTINEL else sel_val
        else: new_vals[fname]=st.session_state.get(f"field_{sheet_row}_{fname}",clean_cell(fields[fname]))
    return new_vals

# 14a. REVIEW SUMMARY
def _render_review_summary(sheet_row,df_iloc,record,col_map,headers):
    new_vals=st.session_state.review_new_vals or {}; eval_val=st.session_state.review_eval_val or ""
    manual_notes=st.session_state.review_manual_notes or ""; ws_title=st.session_state.review_ws_title or ""; sid=st.session_state.review_sid or ""
    SKIP=set(SYSTEM_COLS); changed=[]; unchanged=[]
    for field,new_v in new_vals.items():
        if field in SKIP: continue
        old_v=clean_cell(record.get(field,"")); new_v_clean=clean_cell(new_v)
        if old_v!=new_v_clean: changed.append((field,old_v,new_v_clean))
        else: unchanged.append((field,old_v))
    n_changed=len(changed); badge_color="#16A34A" if n_changed>0 else "#9CA3AF"
    badge_text=f"{n_changed} field(s) modified" if n_changed else "No fields modified"
    st.markdown(f"""<div class="review-panel"><div class="review-panel-title">📋 Change Summary — Row #{sheet_row}</div>
    <div class="review-panel-meta"><span style="font-weight:700;color:{badge_color};">{badge_text}</span>
    &nbsp;·&nbsp; {len(unchanged)} field(s) unchanged &nbsp;·&nbsp; Evaluation: <strong>{_html.escape(eval_val)}</strong>
    &nbsp;·&nbsp; Auditor: <strong>{_html.escape(st.session_state.user_email)}</strong></div></div>""",unsafe_allow_html=True)
    st.markdown("""<div style="display:flex;align-items:center;gap:0;margin-bottom:20px;">
    <div style="display:flex;align-items:center;gap:6px;"><div style="width:26px;height:26px;border-radius:50%;background:#E4E7F0;color:#9CA3AF;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:800;">1</div><span style="font-size:.72rem;color:#9CA3AF;font-weight:600;">Edit</span></div>
    <div style="flex:1;height:2px;background:#4F46E5;margin:0 8px;max-width:60px;"></div>
    <div style="display:flex;align-items:center;gap:6px;"><div style="width:26px;height:26px;border-radius:50%;background:#4F46E5;color:#FFFFFF;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:800;">2</div><span style="font-size:.72rem;color:#4F46E5;font-weight:700;">Review</span></div>
    <div style="flex:1;height:2px;background:#E4E7F0;margin:0 8px;max-width:60px;"></div>
    <div style="display:flex;align-items:center;gap:6px;"><div style="width:26px;height:26px;border-radius:50%;background:#E4E7F0;color:#9CA3AF;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:800;">3</div><span style="font-size:.72rem;color:#9CA3AF;font-weight:600;">Submit</span></div>
    </div>""",unsafe_allow_html=True)
    if changed:
        st.markdown("""<div style="font-size:0.70rem;font-weight:800;color:#DC2626;text-transform:uppercase;letter-spacing:.08em;margin-bottom:10px;">⚡ Modified Fields</div>""",unsafe_allow_html=True)
        rows_html=""
        for field,old_v,new_v in changed:
            old_safe=_html.escape(old_v) if old_v else "—"; new_safe=_html.escape(new_v) if new_v else "—"
            field_display=_html.escape(str(field).replace("\n"," ").replace("\r"," "))
            rows_html+=f"<tr><td class='rdt-field'>{field_display}</td><td class='rdt-old'>{old_safe}</td><td class='rdt-new'>{new_safe}</td></tr>"
        st.markdown(f"""<div style="overflow-x:auto;border:1px solid #E4E7F0;border-radius:10px;margin-bottom:18px;">
        <table class="review-diff-table"><thead><tr>
        <th style="color:#374151;border-bottom:2px solid #E4E7F0;min-width:140px;">Field</th>
        <th style="color:#DC2626;background:#FFF1F2;border-bottom:2px solid #FECDD3;border-left:1px solid #E4E7F0;min-width:180px;">Old Value</th>
        <th style="color:#15803D;background:#F0FDF4;border-bottom:2px solid #A7F3D0;border-left:1px solid #E4E7F0;min-width:180px;">New Value</th>
        </tr></thead><tbody>{rows_html}</tbody></table></div>""",unsafe_allow_html=True)
    else: st.info("ℹ️  No field values were changed. The record will be marked Processed with the selected evaluation.")
    if unchanged:
        with st.expander(f"📄 View {len(unchanged)} unchanged field(s)",expanded=False):
            unch_html=""
            for field,val in unchanged:
                safe_val=_html.escape(val) if val else "<em style='color:#9CA3AF;'>—</em>"
                field_display=_html.escape(str(field).replace("\n"," ").replace("\r"," "))
                unch_html+=f"<tr><td class='rdt-field'>{field_display}</td><td class='rdt-same'>{safe_val}</td></tr>"
            st.markdown(f"""<div style="overflow-x:auto;border:1px solid #E4E7F0;border-radius:8px;">
            <table class="review-diff-table"><thead><tr><th style="color:#374151;border-bottom:1px solid #E4E7F0;">Field</th>
            <th style="color:#6B7280;border-bottom:1px solid #E4E7F0;border-left:1px solid #E4E7F0;">Current Value (unchanged)</th>
            </tr></thead><tbody>{unch_html}</tbody></table></div>""",unsafe_allow_html=True)
    if manual_notes.strip():
        st.markdown(f"""<div style="background:#FFFBEB;border:1px solid #FDE68A;border-left:3px solid #F59E0B;border-radius:8px;padding:12px 16px;margin:16px 0;">
        <div style="font-size:0.60rem;font-weight:800;color:#B45309;text-transform:uppercase;letter-spacing:.08em;margin-bottom:5px;">📝 Auditor Notes</div>
        <div style="font-size:0.82rem;color:#374151;">{_html.escape(manual_notes)}</div></div>""",unsafe_allow_html=True)
    st.markdown("<hr style='border-top:1px solid #E4E7F0;margin:22px 0 18px;'/>",unsafe_allow_html=True)
    btn_l,btn_r=st.columns([1,2],gap="medium")
    with btn_l:
        st.markdown("<div class='btn-back-wrap'>",unsafe_allow_html=True)
        if st.button("Back to Edit",key=f"back_edit_{sheet_row}",use_container_width=True):
            st.session_state.review_mode=False; st.session_state.review_new_vals=None; st.rerun()
        st.markdown("</div>",unsafe_allow_html=True)
    with btn_r:
        st.markdown("<div class='btn-confirm-wrap'>",unsafe_allow_html=True)
        if st.button("Submit",key=f"confirm_{sheet_row}",use_container_width=True):
            ts_now=now_str(); auditor=st.session_state.user_email; log_prefix=f"[x] {auditor} | {ts_now}"
            auto_diff=build_auto_diff(record,new_vals)
            feedback_combined=(f"{manual_notes.strip()}\n{auto_diff}".strip() if manual_notes.strip() else auto_diff)
            with st.spinner("Synchronizing record with central database... Please do not close this window."):
                try:
                    is_success=write_approval_to_sheet(sid,ws_title,sheet_row,col_map,headers,new_vals,record,auditor,ts_now,log_prefix,eval_val=eval_val,feedback_val=feedback_combined)
                    if not is_success:
                        st.toast("⚠️ Another auditor already processed this case.")
                        st.session_state.local_df.at[df_iloc,COL_STATUS]=VAL_DONE
                        _clear_review_state(); time.sleep(2); st.rerun(); return
                except gspread.exceptions.APIError as e: st.error(f"Write failed: {e}"); return
            _apply_optimistic_approve(df_iloc,new_vals,auditor,ts_now,log_prefix,eval_val=eval_val,feedback_val=feedback_combined)
            _clear_review_state(); st.toast(t("saved_ok"),icon="✅"); time.sleep(0.6); st.rerun()
        st.markdown("</div>",unsafe_allow_html=True)


# 14b. WORKLIST
def render_worklist(sid,pending_display,df,headers,col_map,ws_title,col_binder,col_company,col_license):
    st.markdown("<div class='deep-search-strip'><div class='deep-search-title'>🔍 Search Cases</div></div>",unsafe_allow_html=True)
    def _clear_wl(): st.session_state["wl_binder"]=None; st.session_state["wl_license"]=None; st.session_state["wl_company"]=None
    binder_opts_wl=_get_opts(pending_display,col_binder); license_opts_wl=_get_opts(pending_display,col_license); company_opts_wl=_get_opts(pending_display,col_company)
    sc1,sc2,sc3,sc4=st.columns([1,1,1.5,0.32])
    with sc1:
        if binder_opts_wl: wl_binder=st.selectbox("Binder",options=binder_opts_wl,key="wl_binder",index=None,placeholder="🔍 Search Binder...",disabled=(col_binder is None),label_visibility="collapsed") or ""
        else: wl_binder=st.text_input("Binder",key="wl_binder",placeholder="🔍 Binder No.",disabled=(col_binder is None),label_visibility="collapsed") or ""
    with sc2:
        if license_opts_wl: wl_license=st.selectbox("License",options=license_opts_wl,key="wl_license",index=None,placeholder="🔍 Search License...",disabled=(col_license is None),label_visibility="collapsed") or ""
        else: wl_license=st.text_input("License",key="wl_license",placeholder="🔍 License No.",disabled=(col_license is None),label_visibility="collapsed") or ""
    with sc3:
        if company_opts_wl: wl_company=st.selectbox("Company",options=company_opts_wl,key="wl_company",index=None,placeholder="🔍 Search Company...",disabled=(col_company is None),label_visibility="collapsed") or ""
        else: wl_company=st.text_input("Company",key="wl_company",placeholder="🔍 Company Name",disabled=(col_company is None),label_visibility="collapsed") or ""
    with sc4: st.button("Clear",key="wl_clr",use_container_width=True,on_click=_clear_wl)
    if wl_binder.strip() and col_binder and col_binder in pending_display.columns: pending_display=pending_display[pending_display[col_binder].astype(str).str.strip()==wl_binder.strip()]
    if wl_license.strip() and col_license and col_license in pending_display.columns: pending_display=pending_display[pending_display[col_license].astype(str).str.strip()==wl_license.strip()]
    if wl_company.strip() and col_company and col_company in pending_display.columns: pending_display=pending_display[pending_display[col_company].astype(str).str.contains(wl_company.strip(),case=False,na=False)]
    p_count=len(pending_display)
    st.markdown(f"""<div class="worklist-header" style="margin-top:15px;">
    <div><div class="worklist-title">{t('worklist_title')}</div><div class="worklist-sub">{t('worklist_sub')}</div></div>
    <span class="chip chip-pending">{p_count} {t('outstanding')}</span></div>""",unsafe_allow_html=True)
    if pending_display.empty: st.info("No cases found." if (wl_binder or wl_license or wl_company) else "All cases processed."); return
    render_paginated_table(pending_display,page_key="page_worklist")
    st.markdown(f"<div class='section-title'>{t('select_case')}</div>",unsafe_allow_html=True)
    
    display_label_col = (col_company or col_binder or next((h for h in headers if h not in SYSTEM_COLS), "Row"))
    col_fy = "السنة المالية / ساڵی دارایی (ساڵ) / Fiscal Year"
    if col_fy not in pending_display.columns: 
        col_fy = next((c for c in pending_display.columns if 'fiscal' in str(c).lower()), col_fy)
        
    opts = ["-"]
    for idx, row in pending_display.iterrows():
        lbl = f"Row {idx}{_ROW_SEP}{str(row.get(display_label_col,''))[:40]}"
        fy_val = str(row.get(col_fy, '')).replace('nan', '').replace('None', '').strip()
        if fy_val.endswith('.0'): fy_val = fy_val[:-2]
        if fy_val: lbl += f"{_ROW_SEP}FY: {fy_val}"
        lbl += f"{_ROW_SEP}{str(row.get(COL_DATE,''))[:10]}"
        opts.append(lbl)
        
    row_sel = st.selectbox("", opts, key="row_sel", label_visibility="collapsed")
    
    if row_sel=="-":
        if st.session_state.get("review_mode"): _clear_review_state()
        return
    sheet_row=int(row_sel.split(_ROW_SEP)[0].replace("Row","").strip()); df_iloc=sheet_row-2
    if df_iloc<0 or df_iloc>=len(df): st.error("Row index out of range."); return
    record=df.iloc[df_iloc].to_dict()
    if st.session_state.get("review_row")!=sheet_row: _clear_review_state(); st.session_state.review_row=sheet_row
    SKIP=set(SYSTEM_COLS); fields={k:v for k,v in record.items() if k not in SKIP}
    if (st.session_state.get("review_mode") and st.session_state.get("review_row")==sheet_row and st.session_state.get("review_new_vals") is not None):
        _render_review_summary(sheet_row,df_iloc,record,col_map,headers); return
    with st.expander(t("audit_trail"),expanded=False):
        history=str(record.get(COL_LOG,"")).strip()
        if history:
            for line in history.split("\n"):
                if line.strip(): st.markdown(f'<div class="log-line">{_html.escape(line)}</div>',unsafe_allow_html=True)
        else: st.caption(t("no_history"))
    st.markdown(f"<div class='section-title'>{t('processing')} #{sheet_row}</div>",unsafe_allow_html=True)
    st.markdown("""<div style="display:flex;align-items:center;gap:0;margin-bottom:18px;">
    <div style="display:flex;align-items:center;gap:6px;"><div style="width:26px;height:26px;border-radius:50%;background:#4F46E5;color:#FFFFFF;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:800;">1</div><span style="font-size:.72rem;color:#4F46E5;font-weight:700;">Edit</span></div>
    <div style="flex:1;height:2px;background:#E4E7F0;margin:0 8px;max-width:60px;"></div>
    <div style="display:flex;align-items:center;gap:6px;"><div style="width:26px;height:26px;border-radius:50%;background:#E4E7F0;color:#9CA3AF;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:800;">2</div><span style="font-size:.72rem;color:#9CA3AF;font-weight:600;">Review</span></div>
    <div style="flex:1;height:2px;background:#E4E7F0;margin:0 8px;max-width:60px;"></div>
    <div style="display:flex;align-items:center;gap:6px;"><div style="width:26px;height:26px;border-radius:50%;background:#E4E7F0;color:#9CA3AF;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:800;">3</div><span style="font-size:.72rem;color:#9CA3AF;font-weight:600;">Submit</span></div>
    </div>""",unsafe_allow_html=True)
    MANUAL_SENTINEL="-- Type manually --"; combo_keys=[]
    with st.form("audit_form"):
        for fname,fval in fields.items():
            clean_fname=str(fname).replace("\n"," ").replace("\r"," ")
            matched_target=next((tgt for tgt in _COMBO_TARGETS if tgt["match"] in clean_fname),None)
            if matched_target:
                options=matched_target["options"]; current=clean_cell(fval)
                try: def_idx=options.index(current)+1
                except ValueError: def_idx=0
                st.markdown(f"<div style='font-size:0.75rem;font-weight:700;color:var(--text-secondary);margin-bottom:5px;'>{_html.escape(fname)}</div>",unsafe_allow_html=True)
                fc1,fc2=st.columns(2)
                with fc1: st.selectbox("",[MANUAL_SENTINEL]+options,index=def_idx,key=f"sel_{sheet_row}_{fname}",label_visibility="collapsed")
                with fc2: st.text_input("",value=current,key=f"txt_{sheet_row}_{fname}",label_visibility="collapsed",placeholder="Or type here...")
                combo_keys.append(fname)
            else: st.text_input(fname,value=clean_cell(fval),key=f"field_{sheet_row}_{fname}")
        st.markdown("<hr style='border-top:1px dashed var(--border);margin:18px 0 14px;'/>",unsafe_allow_html=True)
        eval_val=st.selectbox(t("eval_label"),options=EVAL_OPTIONS,index=0,key=f"form_eval_{sheet_row}")
        manual_notes=st.text_area(t("feedback_label"),placeholder=t("feedback_placeholder"),key=f"form_feedback_{sheet_row}",height=100)
        do_review=st.form_submit_button("🔍  Review Changes",use_container_width=True)
    if do_review:
        new_vals=_resolve_form_values(fields,sheet_row,combo_keys)
        st.session_state.review_mode=True; st.session_state.review_row=sheet_row; st.session_state.review_new_vals=new_vals
        st.session_state.review_eval_val=eval_val; st.session_state.review_manual_notes=manual_notes
        st.session_state.review_record=record; st.session_state.review_df_iloc=df_iloc
        st.session_state.review_ws_title=ws_title; st.session_state.review_sid=sid; st.rerun()
# 15. ARCHIVE
def render_archive(sid,done_view,df,col_map,ws_title,can_reopen,col_binder=None,col_company=None,col_license=None):
    def clear_arch_search():
        for k in ("arch_binder","arch_license","arch_company","arch_auditor"): st.session_state[k]=None
        st.session_state["page_archive"]=1
    d_count=len(done_view)
    st.markdown(f"""<div class="worklist-header"><div><div class="worklist-title">Processed Archive</div>
    <div class="worklist-sub">Completed and committed audit records</div></div>
    <span class="chip chip-done">{d_count} {t('processed')}</span></div>""",unsafe_allow_html=True)
    st.markdown(f"<div style='margin-bottom:8px;font-size:.62rem;font-weight:800;letter-spacing:.10em;text-transform:uppercase;color:var(--indigo-600);'>{t('arch_search_title')}</div>",unsafe_allow_html=True)
    auditor_opts=_get_opts(done_view,COL_AUDITOR); binder_opts_arch=_get_opts(done_view,col_binder)
    license_opts_arch=_get_opts(done_view,col_license); company_opts_arch=_get_opts(done_view,col_company)
    ac1,ac2,ac3,ac4,ac5=st.columns([1,1,1.5,1.2,0.4])
    with ac1:
        if binder_opts_arch: s_binder=st.selectbox("Binder No.",options=binder_opts_arch,key="arch_binder",index=None,placeholder="🔍 Search Binder...",disabled=(col_binder is None),label_visibility="collapsed") or ""
        else: s_binder=st.text_input("Binder No.",key="arch_binder",placeholder="🔍 Binder No...",disabled=(col_binder is None),label_visibility="collapsed") or ""
    with ac2:
        if license_opts_arch: s_license=st.selectbox("License No.",options=license_opts_arch,key="arch_license",index=None,placeholder="🔍 Search License...",disabled=(col_license is None),label_visibility="collapsed") or ""
        else: s_license=st.text_input("License No.",key="arch_license",placeholder="🔍 License No...",disabled=(col_license is None),label_visibility="collapsed") or ""
    with ac3:
        if company_opts_arch: s_company=st.selectbox("Company",options=company_opts_arch,key="arch_company",index=None,placeholder="🔍 Search Company...",disabled=(col_company is None),label_visibility="collapsed") or ""
        else: s_company=st.text_input("Company",key="arch_company",placeholder="🔍 Company Name...",disabled=(col_company is None),label_visibility="collapsed") or ""
    with ac4: s_auditor=st.selectbox("Auditor",options=auditor_opts,key="arch_auditor",index=None,placeholder="🔍 Search Auditor...",label_visibility="collapsed") or ""
    with ac5: st.button("X",key="arch_clr",on_click=clear_arch_search,use_container_width=True)
    filtered_view=done_view
    if s_binder.strip() and col_binder and col_binder in filtered_view.columns: filtered_view=filtered_view[filtered_view[col_binder].astype(str).str.strip()==s_binder.strip()]
    if s_license.strip() and col_license and col_license in filtered_view.columns: filtered_view=filtered_view[filtered_view[col_license].astype(str).str.strip()==s_license.strip()]
    if s_company.strip() and col_company and col_company in filtered_view.columns: filtered_view=filtered_view[filtered_view[col_company].astype(str).str.contains(s_company.strip(),case=False,na=False)]
    if s_auditor.strip() and COL_AUDITOR in filtered_view.columns: filtered_view=filtered_view[filtered_view[COL_AUDITOR].astype(str)==s_auditor.strip()]
    if not filtered_view.empty and COL_DATE in filtered_view.columns:
        filtered_view["_sd"]=pd.to_datetime(filtered_view[COL_DATE],format="%Y-%m-%d %H:%M:%S",errors="coerce")
        filtered_view=filtered_view.sort_values("_sd",ascending=False,na_position="last").drop(columns=["_sd"])
    st.markdown("<hr class='divider'/>",unsafe_allow_html=True)
    if filtered_view.empty: st.info("No processed records match the search.")
    else:
        if can_reopen: st.markdown(f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);border-left:3px solid var(--indigo-500);border-radius:var(--radius-md);padding:10px 16px;margin-bottom:14px;font-size:.78rem;color:var(--indigo-600)!important;font-weight:600;'>{t('archive_quality_note')}</div>",unsafe_allow_html=True)
        priority_cols=[COL_STATUS,COL_EVAL,COL_FEEDBACK,COL_AUDITOR,COL_DATE]
        other_cols=[c for c in filtered_view.columns if c not in priority_cols and c!=COL_LOG]
        ordered_cols=[c for c in priority_cols if c in filtered_view.columns]+other_cols
        render_paginated_table(filtered_view[ordered_cols],page_key="page_archive")
    
    if can_reopen and not filtered_view.empty:
        st.markdown("<hr class='divider'/>",unsafe_allow_html=True)
        st.markdown(f"<div class='section-title'>{t('reopen')}</div>",unsafe_allow_html=True)
        display_label_col=(col_binder or col_license or next((h for h in filtered_view.columns if h not in SYSTEM_COLS),"Row"))
        ropts=["-"]+[f"Row {idx} | {str(row.get(display_label_col,''))[:40]} | {str(row.get(COL_DATE,''))[:10]}" for idx,row in filtered_view.iterrows()]
        rsel=st.selectbox("Select record to re-open:",ropts,key="reopen_sel")
        if rsel!="-":
            ridx=int(rsel.split("|")[0].replace("Row","").strip()); df_iloc=ridx-2
            if st.button(t("reopen"),key="reopen_btn"):
                with st.spinner("Re-opening..."):
                    try: write_reopen_to_sheet(sid,ws_title,ridx,col_map)
                    except gspread.exceptions.APIError as e: st.error(f"Error: {e}"); return
                _apply_optimistic_reopen(df_iloc); st.rerun()

@st.cache_data(ttl=300,show_spinner=False)
def fetch_combined_analytics(sid):
    all_dfs=[]
    for ws_name in VISIBLE_SHEETS:
        try:
            custom_range = _get_custom_range(sid, ws_name)
            raw,_=_fetch_raw_sheet_cached(sid,ws_name,custom_range)
            if not raw: continue
            df_temp,h_temp,_=_raw_to_dataframe(raw)
            if df_temp.empty: continue
            df_done=df_temp[df_temp[COL_STATUS]==VAL_DONE].copy()
            if df_done.empty: continue
            c_agent=detect_column(h_temp,"agent_email")
            df_done["_Agent"]=(df_done[c_agent].astype(str) if c_agent and c_agent in df_done.columns else "")
            for c in [COL_AUDITOR,COL_EVAL,COL_DATE]:
                if c not in df_done.columns: df_done[c]=""
            df_clean=df_done[["_Agent",COL_AUDITOR,COL_EVAL,COL_DATE]].copy(); df_clean["Sheet"]=ws_name; all_dfs.append(df_clean)
        except: pass
    if not all_dfs: return pd.DataFrame()
    return pd.concat(all_dfs,ignore_index=True)


# 16. ANALYTICS
def render_analytics(df,sid,col_agent_email=None,col_binder=None,col_company=None):
    agent_opts=_get_opts(df,col_agent_email); company_opts=_get_opts(df,col_company); binder_opts=_get_opts(df,col_binder)
    srch_binder,srch_agent,srch_company=render_deep_search_strip("anal",col_binder,col_agent_email,col_company,binder_options=binder_opts,agent_options=agent_opts,company_options=company_opts)
    work_df=apply_deep_search(df,srch_binder,srch_agent,srch_company,col_binder,col_agent_email,col_company)
    st.markdown(f"<div class='section-title'>{t('period')}</div>",unsafe_allow_html=True)
    periods=[("all",t("all_time")),("today",t("today")),("this_week",t("this_week")),("this_month",t("this_month"))]
    for cw,(pk,pl) in zip(st.columns(len(periods)),periods):
        lbl=f"[{pl}]" if st.session_state.date_filter==pk else pl
        if cw.button(lbl,use_container_width=True,key=f"pf_{pk}"): st.session_state.date_filter=pk; st.rerun()
    done_base=work_df[work_df[COL_STATUS]==VAL_DONE]; done_f=apply_period_filter(done_base,COL_DATE,st.session_state.date_filter)
    if _deep_search_active(srch_binder,srch_agent,srch_company):
        terms=[_html.escape(x) for x in (srch_binder,srch_agent,srch_company) if x.strip()]
        st.markdown(f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);border-radius:var(--radius-md);padding:9px 16px;margin-bottom:14px;font-size:.78rem;color:var(--indigo-600)!important;font-weight:600;'>{t('ds_showing')} <strong>{' &middot; '.join(terms)}</strong> &mdash; <strong>{len(done_f)}</strong> processed records matched</div>",unsafe_allow_html=True)
    if done_f.empty: st.info(t("no_records")); return
    col1,col2,col3=st.columns(3); col1.metric(t("records_period"),len(done_f))
    active=(pd.to_datetime(done_f[COL_DATE],format="%Y-%m-%d %H:%M:%S",errors="coerce").dt.date.nunique()) if COL_DATE in done_f.columns else 0
    col2.metric(t("active_days"),active); col3.metric(t("avg_per_day"),f"{len(done_f)/max(active,1):.1f}")
    left,right=st.columns([1,1.6],gap="large")
    with left:
        st.markdown(f"<div class='section-title'>{t('leaderboard')}</div>",unsafe_allow_html=True)
        if COL_AUDITOR in done_f.columns:
            lb=done_f[COL_AUDITOR].replace("","-").value_counts().reset_index(); lb.columns=["Auditor","Count"]
            for i,r in lb.head(10).iterrows():
                st.markdown(f'<div style="display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);"><span style="font-weight:600;font-size:.82rem;">{i+1}. {_html.escape(str(r["Auditor"]))}</span><span style="font-weight:800;color:var(--indigo-600);">{r["Count"]}</span></div>',unsafe_allow_html=True)
            fig=px.bar(lb.head(10),x="Count",y="Auditor",orientation="h",color="Count",color_continuous_scale=[_BLU,_NVY],template=_PT)
            fig.update_traces(marker_line_width=0)
            fig.update_layout(paper_bgcolor=_PBG,plot_bgcolor=_PBG,font=dict(family="Plus Jakarta Sans",color=_PFC,size=11),showlegend=False,coloraxis_showscale=False,margin=dict(l=8,r=8,t=10,b=8),xaxis=dict(gridcolor=_PGR,zeroline=False),yaxis=dict(gridcolor="rgba(0,0,0,0)",categoryorder="total ascending"),height=min(320,max(180,36*len(lb.head(10)))))
            st.plotly_chart(fig,use_container_width=True)
    with right:
        st.markdown(f"<div class='section-title'>{t('daily_trend')}</div>",unsafe_allow_html=True)
        if COL_DATE in done_f.columns:
            parsed_dates=pd.to_datetime(done_f[COL_DATE],format="%Y-%m-%d %H:%M:%S",errors="coerce"); valid_mask=parsed_dates.notna()
            if valid_mask.any():
                dates=parsed_dates[valid_mask].dt.date; trend=dates.value_counts().sort_index().reset_index(); trend.columns=["Date","Records"]
                if len(trend)>1:
                    rng=pd.date_range(trend["Date"].min(),trend["Date"].max())
                    trend=(trend.set_index("Date").reindex(rng.date,fill_value=0).reset_index().rename(columns={"index":"Date"}))
                fig2=go.Figure()
                fig2.add_trace(go.Scatter(x=trend["Date"],y=trend["Records"],mode="none",fill="tozeroy",fillcolor="rgba(99,102,241,0.07)",showlegend=False))
                fig2.add_trace(go.Scatter(x=trend["Date"],y=trend["Records"],mode="lines+markers",line=dict(color=_NVY,width=2.5),marker=dict(color=_BLU,size=7,line=dict(color="#FFFFFF",width=2)),hovertemplate="<b>%{x}</b><br>Records: <b>%{y}</b><extra></extra>"))
                fig2.update_layout(template=_PT,paper_bgcolor=_PBG,plot_bgcolor=_PBG,font=dict(family="Plus Jakarta Sans",color=_PFC,size=11),showlegend=False,margin=dict(l=8,r=8,t=10,b=8),xaxis=dict(gridcolor=_PGR,zeroline=False),yaxis=dict(gridcolor=_PGR,zeroline=False),height=380,hovermode="x unified")
                st.plotly_chart(fig2,use_container_width=True)
            else: st.info(t("no_records"))
    st.markdown(f"<div class='section-title'>{t('acc_ranking_title')}</div>",unsafe_allow_html=True)
    if col_agent_email and col_agent_email in done_f.columns and COL_EVAL in done_f.columns:
        normalised=done_f[COL_EVAL].fillna("").map(_normalise_eval)
        good_mask=normalised.str.contains("Good",na=False); bad_mask=normalised.str.contains(r"Incorrect",na=False,regex=True); dup_mask=normalised.str.contains("Duplicate",na=False); rated_mask=good_mask|bad_mask|dup_mask
        agent_col=done_f[col_agent_email].fillna("").astype(str).str.strip().replace("","-")
        tmp=pd.DataFrame({"agent":agent_col,"good":good_mask.astype(int),"Incorrect":bad_mask.astype(int),"dup":dup_mask.astype(int),"rated":rated_mask.astype(int)})
        grp=tmp.groupby("agent",sort=False).sum().reset_index(); grp["accuracy"]=grp.apply(lambda r:(r["good"]/r["rated"]*100) if r["rated"]>0 else 0.0,axis=1)
        grp=grp.sort_values(["accuracy","rated"],ascending=[False,False]).reset_index(drop=True)
        if not grp.empty:
            th_row=f"<tr><th>#</th><th>{t('acc_agent')}</th><th>{t('acc_total')}</th><th>{t('acc_good')}</th><th>{t('acc_bad')}</th><th>{t('acc_dup')}</th><th>{t('acc_rate')}</th></tr>"
            td_rows=""
            for pos,row in grp.iterrows():
                pct=row["accuracy"]; rc,bc=("acc-rate-high","#16A34A") if pct>=80 else (("acc-rate-mid","#B45309") if pct>=50 else ("acc-rate-low","#DC2626"))
                bar=f"<span class='acc-bar-wrap'><span class='acc-bar-fill' style='width:{int(pct)}%;background:{bc};display:block;'></span></span>"
                td_rows+=(f"<tr><td style='color:var(--text-muted);font-family:var(--mono);font-size:.70rem;'>{pos+1}</td><td style='font-weight:600;'>{_html.escape(str(row['agent']))}</td>"
                          f"<td style='font-family:var(--mono);font-weight:700;'>{int(row['rated'])}</td><td><span class='s-chip s-eval-good'>{int(row['good'])}</span></td>"
                          f"<td><span class='s-chip s-eval-Incorrect'>{int(row['Incorrect'])}</span></td><td><span class='s-chip s-eval-dup'>{int(row['dup'])}</span></td>"
                          f"<td class='{rc}'>{pct:.1f}% {bar}</td></tr>")
            st.markdown(f"<div class='gov-table-wrap'><table class='acc-table'><thead>{th_row}</thead><tbody>{td_rows}</tbody></table></div>",unsafe_allow_html=True)
        else: st.info(t("acc_no_data"))
    else: st.info(t("acc_no_data"))
    st.markdown("<br><hr class='divider' style='border-top:3px solid var(--border);'/>",unsafe_allow_html=True)
    st.markdown("<div class='section-title' style='font-size:1.1rem;'>🌍 Global Analytics (All Sheets Combined)</div>",unsafe_allow_html=True)
    st.caption("Data from all sheets in the current project aggregated here.")
    with st.spinner("Aggregating data from all sheets..."): global_df_raw=fetch_combined_analytics(sid)
    if global_df_raw.empty: st.info("No data found in the sheets."); return
    global_df=apply_period_filter(global_df_raw,COL_DATE,st.session_state.date_filter)
    if global_df.empty: st.info("No records processed in this time period."); return
    n_ev=global_df[COL_EVAL].fillna("").map(_normalise_eval); g_m=n_ev.str.contains("Good",na=False); b_m=n_ev.str.contains(r"Incorrect",na=False,regex=True); d_m=n_ev.str.contains("Duplicate",na=False)
    cg1,cg2=st.columns(2)
    with cg1:
        st.markdown("<div class='section-title'>📊 Global Agent Accuracy</div>",unsafe_allow_html=True)
        ag_col=global_df["_Agent"].fillna("").astype(str).str.strip().replace("","-"); r_m=g_m|b_m|d_m
        gtmp=pd.DataFrame({"agent":ag_col,"good":g_m.astype(int),"Incorrect":b_m.astype(int),"dup":d_m.astype(int),"rated":r_m.astype(int)})
        g_grp=gtmp.groupby("agent",sort=False).sum().reset_index(); g_grp["accuracy"]=g_grp.apply(lambda r:(r["good"]/r["rated"]*100) if r["rated"]>0 else 0.0,axis=1)
        g_grp=g_grp.sort_values(["accuracy","rated"],ascending=[False,False]).reset_index(drop=True)
        if not g_grp.empty and g_grp["rated"].sum()>0:
            g_th="<tr><th>#</th><th>Agent</th><th>Total</th><th>Good</th><th>Incorrect</th><th>Dup</th><th>Accuracy %</th></tr>"; g_td=""
            for pos,row in g_grp.iterrows():
                pct=row["accuracy"]; rc="acc-rate-high" if pct>=80 else ("acc-rate-mid" if pct>=50 else "acc-rate-low"); bc="#16A34A" if pct>=80 else ("#B45309" if pct>=50 else "#DC2626")
                bar=f"<span class='acc-bar-wrap'><span class='acc-bar-fill' style='width:{int(pct)}%;background:{bc};display:block;'></span></span>"
                g_td+=(f"<tr><td style='color:var(--text-muted);font-size:.70rem;'>{pos+1}</td><td style='font-weight:600;'>{_html.escape(str(row['agent']))[:30]}</td>"
                       f"<td style='font-family:var(--mono);font-weight:700;'>{int(row['rated'])}</td><td><span class='s-chip s-eval-good'>{int(row['good'])}</span></td>"
                       f"<td><span class='s-chip s-eval-Incorrect'>{int(row['Incorrect'])}</span></td><td><span class='s-chip s-eval-dup'>{int(row['dup'])}</span></td>"
                       f"<td class='{rc}'>{pct:.1f}% {bar}</td></tr>")
            st.markdown(f"<div class='gov-table-wrap'><table class='acc-table'><thead>{g_th}</thead><tbody>{g_td}</tbody></table></div>",unsafe_allow_html=True)
        else: st.info("No evaluation data available.")
    with cg2:
        st.markdown("<div class='section-title'>📈 Global Auditor Productivity</div>",unsafe_allow_html=True)
        aud_col=global_df[COL_AUDITOR].fillna("").astype(str).str.strip().replace("","-")
        atmp=pd.DataFrame({"auditor":aud_col,"total_cases":1,"gave_good":g_m.astype(int),"gave_bad":b_m.astype(int),"gave_dup":d_m.astype(int)})
        a_grp=atmp.groupby("auditor",sort=False).sum().reset_index().sort_values("total_cases",ascending=False).reset_index(drop=True)
        if not a_grp.empty:
            a_th="<tr><th>#</th><th>Auditor</th><th>Processed</th><th>Good</th><th>Incorrect</th><th>Dup</th></tr>"; a_td=""
            for pos,row in a_grp.iterrows():
                a_td+=(f"<tr><td style='color:var(--text-muted);font-size:.70rem;'>{pos+1}</td><td style='font-weight:600;'>{_html.escape(str(row['auditor']))[:30]}</td>"
                       f"<td style='font-size:1.1rem;color:var(--indigo-600);font-weight:800;'>{int(row['total_cases'])}</td>"
                       f"<td><span style='color:var(--green-700);font-weight:600;'>{int(row['gave_good'])}</span></td>"
                       f"<td><span style='color:var(--red-600);font-weight:600;'>{int(row['gave_bad'])}</span></td>"
                       f"<td><span style='color:var(--amber-700);font-weight:600;'>{int(row['gave_dup'])}</span></td></tr>")
            st.markdown(f"<div class='gov-table-wrap'><table class='acc-table'><thead>{a_th}</thead><tbody>{a_td}</tbody></table></div>",unsafe_allow_html=True)
        else: st.info("No auditor activity recorded yet.")


# ERROR ANALYTICS DASHBOARD
def render_error_analytics(sid: str) -> None:
    _C_ERR="#DC2626"; _C_GOOD="#10B981"; _C_DUP="#F59E0B"; _C_IND="#4F46E5"; _C_MUTED="#9CA3AF"; _C_TRANS="rgba(0,0,0,0)"
    _DET_COLS=["Date","Sheet","Company Name","Binder No","Data Entry Agent","Auditor ID","Evaluation","Correction Notes"]
    st.markdown("""<div style="display:flex;align-items:center;gap:16px;margin-bottom:28px;padding-bottom:22px;border-bottom:1px solid var(--border);">
    <div style="width:46px;height:46px;border-radius:14px;flex-shrink:0;background:linear-gradient(135deg,#FEE2E2 0%,#FECDD3 100%);display:flex;align-items:center;justify-content:center;font-size:1.5rem;">🔎</div>
    <div><div style="font-size:1.50rem;font-weight:800;color:var(--text-primary);line-height:1.1;">Error Analytics Dashboard</div>
    <div style="font-size:.76rem;color:var(--text-muted);font-weight:500;margin-top:3px;">Data-quality deep-dive · All registered sheets combined</div></div></div>""",unsafe_allow_html=True)
    with st.spinner("Aggregating quality data across all sheets…"): agg_raw=fetch_combined_analytics(sid)
    if agg_raw.empty: st.info("ℹ️  No processed records found yet."); return
    agg=agg_raw.copy(); agg["_date"]=pd.to_datetime(agg[COL_DATE],format="%Y-%m-%d %H:%M:%S",errors="coerce")
    agg["_norm"]=agg[COL_EVAL].fillna("").map(_normalise_eval)
    agg["_good"]=agg["_norm"].str.contains("Good",na=False); agg["_inc"]=agg["_norm"].str.contains("Incorrect",na=False)
    agg["_dup"]=agg["_norm"].str.contains("Duplicate",na=False); agg["_err"]=agg["_inc"]|agg["_dup"]
    total=len(agg); good_n=int(agg["_good"].sum()); inc_n=int(agg["_inc"].sum()); dup_n=int(agg["_dup"].sum())
    error_n=int(agg["_err"].sum()); unrated_n=total-good_n-error_n; error_rate=round(error_n/max(total,1)*100,1)
    rate_clr=_C_ERR if error_rate>=20 else (_C_DUP if error_rate>=10 else _C_GOOD)
    def _metric_card(label,val,sub,accent,icon):
        return (f"<div style='background:#FFFFFF;border:1px solid var(--border);border-top:3px solid {accent};border-radius:14px;padding:20px 22px;box-shadow:var(--shadow-md);'>"
                f"<div style='display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;'><span style='font-size:.58rem;font-weight:800;text-transform:uppercase;letter-spacing:.12em;color:{accent};'>{label}</span><span style='font-size:1.25rem;'>{icon}</span></div>"
                f"<div style='font-size:2.05rem;font-weight:800;color:{accent};line-height:1.1;'>{val}</div><div style='font-size:.71rem;color:var(--text-muted);margin-top:7px;font-weight:500;'>{sub}</div></div>")
    mc1,mc2,mc3,mc4=st.columns(4,gap="small")
    mc1.markdown(_metric_card("Total Processed",f"{total:,}","All records · all sheets",_C_IND,"📊"),unsafe_allow_html=True)
    mc2.markdown(_metric_card("Correct (Good)",f"{good_n:,}",f"{round(good_n/max(total,1)*100,1)}% accuracy rate",_C_GOOD,"✅"),unsafe_allow_html=True)
    mc3.markdown(_metric_card("Errors Found",f"{error_n:,}",f"{inc_n} Incorrect + {dup_n} Duplicate",_C_ERR,"⚠️"),unsafe_allow_html=True)
    mc4.markdown(_metric_card("Error Rate",f"{error_rate}%","Errors ÷ Total Processed",rate_clr,"📉"),unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)
    def _detect_aggressive(hdrs,keywords,skip=None,exclude=None):
        _skip=skip or set(); _excl=[x.lower() for x in (exclude or [])]
        for h in hdrs:
            if h in _skip: continue
            hl=h.lower().strip()
            if any(ex in hl for ex in _excl): continue
            for kw in keywords:
                if kw.lower() in hl: return h
        return None
    detail_rows=[]; _sys_skip=set(SYSTEM_COLS)
    for ws_name in VISIBLE_SHEETS:
        try:
            custom_range = _get_custom_range(sid, ws_name)
            raw,_=_fetch_raw_sheet_cached(sid,ws_name,custom_range)
            if not raw: continue
            df_ws,hdrs_ws,_=_raw_to_dataframe(raw)
            if df_ws.empty: continue
            c_company=_detect_aggressive(hdrs_ws,["company name","taxpayer name","company","taxpayer","ناوی کۆمپانیا","اسم الشركة"],skip=_sys_skip,exclude=["id","no","num","number","رقم","ژمارە"])
            c_binder=_detect_aggressive(hdrs_ws,["binder","file no","بایندەری","ملف الشركة"])
            c_agent=_detect_aggressive(hdrs_ws,["data entry email","agent email","email","ئیمەیڵی ئەجنت"],skip=_sys_skip,exclude=["company","taxpayer"])
            c_notes=_detect_aggressive(hdrs_ws,["correction_notes","correction notes","feedback","notes"])
            err_mask=df_ws[COL_EVAL].fillna("").map(_normalise_eval).str.contains("Incorrect|Duplicate",na=False,regex=True)
            err_ws=df_ws[err_mask].copy()
            if err_ws.empty: continue
            notes_src=c_notes or COL_FEEDBACK
            for _,row in err_ws.iterrows():
                detail_rows.append({"Date":str(row.get(COL_DATE,"") or "").strip() or "-","Sheet":ws_name,
                    "Company Name":(str(row.get(c_company,"") or "").strip() or "-") if c_company else "-",
                    "Binder No":(str(row.get(c_binder,"") or "").strip() or "-") if c_binder else "-",
                    "Data Entry Agent":(str(row.get(c_agent,"") or "").strip() or "-") if c_agent else "-",
                    "Auditor ID":str(row.get(COL_AUDITOR,"") or "").strip() or "-",
                    "Evaluation":str(row.get(COL_EVAL,"") or "").strip() or "-",
                    "Correction Notes":str(row.get(notes_src,"") or "").strip() or "-"})
        except: continue
    detail_df=(pd.DataFrame(detail_rows)[_DET_COLS] if detail_rows else pd.DataFrame(columns=_DET_COLS))
    if not detail_df.empty:
        _ds=pd.to_datetime(detail_df["Date"],format="%Y-%m-%d %H:%M:%S",errors="coerce")
        detail_df=(detail_df.assign(_ds=_ds).sort_values("_ds",ascending=False,na_position="last").drop(columns=["_ds"]).reset_index(drop=True))
    err_count=len(detail_df)
    st.markdown("<div class='section-title'>📊 Quality Distribution by Sheet</div>",unsafe_allow_html=True)
    cha_l,cha_r=st.columns([1.65,1],gap="large")
    with cha_l:
        sheet_rows_data=[{"Sheet":ws,"Good":int(agg[agg["Sheet"]==ws]["_good"].sum()),"Incorrect":int(agg[agg["Sheet"]==ws]["_inc"].sum()),"Duplicate":int(agg[agg["Sheet"]==ws]["_dup"].sum())} for ws in VISIBLE_SHEETS]
        sh_df=pd.DataFrame(sheet_rows_data); fig_bars=go.Figure()
        for col_key,color,name in [("Good",_C_GOOD,"Good ✅"),("Incorrect",_C_ERR,"Incorrect ❌"),("Duplicate",_C_DUP,"Duplicate ⚠️")]:
            fig_bars.add_trace(go.Bar(name=name,x=sh_df["Sheet"],y=sh_df[col_key],marker_color=color,marker_line_width=0,text=sh_df[col_key].apply(lambda v:str(int(v)) if v>0 else ""),textposition="auto",textfont=dict(size=11,color="#FFFFFF")))
        fig_bars.update_layout(barmode="group",template="plotly_white",paper_bgcolor=_C_TRANS,plot_bgcolor=_C_TRANS,font=dict(family="Plus Jakarta Sans",color=_PFC,size=12),legend=dict(orientation="h",yanchor="bottom",y=1.04,xanchor="right",x=1),margin=dict(l=8,r=8,t=44,b=8),height=330)
        st.plotly_chart(fig_bars,use_container_width=True)
    with cha_r:
        d_labels=["Good ✅","Incorrect ❌","Duplicate ⚠️","Unrated"]; d_values=[good_n,inc_n,dup_n,unrated_n]; d_colors=[_C_GOOD,_C_ERR,_C_DUP,_C_MUTED]
        nz=[(l,v,c) for l,v,c in zip(d_labels,d_values,d_colors) if v>0]
        if nz:
            nz_l,nz_v,nz_c=zip(*nz)
            fig_donut=go.Figure(go.Pie(labels=list(nz_l),values=list(nz_v),hole=0.60,marker=dict(colors=list(nz_c),line=dict(color="#FFFFFF",width=2.5)),textinfo="percent+value",sort=False))
            fig_donut.add_annotation(text=f"<b>{error_rate}%</b><br>Error Rate",x=0.5,y=0.5,showarrow=False,font=dict(size=14,color=rate_clr))
            fig_donut.update_layout(template="plotly_white",paper_bgcolor=_C_TRANS,plot_bgcolor=_C_TRANS,margin=dict(l=8,r=8,t=44,b=8),height=330)
            st.plotly_chart(fig_donut,use_container_width=True)
    st.markdown(f"""<div class="worklist-header" style="margin-top:14px;margin-bottom:18px;">
    <div><div style="font-weight:800;font-size:1.0rem;">📋 Detailed Error Records</div>
    <div style="font-size:.74rem;color:var(--text-muted);margin-top:3px;">Incorrect &amp; Duplicate evaluations</div></div>
    <span class="chip" style="background:#FFF1F2;color:{_C_ERR};border:1px solid #FECDD3;">{err_count:,} Error{'s' if err_count!=1 else ''}</span></div>""",unsafe_allow_html=True)
    if detail_df.empty: st.success("🎉 No error records found across all sheets!")
    else: render_html_table(detail_df.head(500),max_rows=500)
    st.markdown("<hr class='divider'/>",unsafe_allow_html=True)
    st.markdown("<div class='section-title'>⬇️ Export Error Report</div>",unsafe_allow_html=True)
    dtag=datetime.now(TZ).strftime("%Y%m%d_%H%M"); no_err=detail_df.empty
    ec1,ec2=st.columns(2,gap="medium")
    with ec1:
        if not no_err:
            _csv_buf=io.StringIO(); detail_df[_DET_COLS].to_csv(_csv_buf,index=False,encoding="utf-8-sig")
            st.download_button("📥 Download as CSV",data=_csv_buf.getvalue().encode("utf-8-sig"),file_name=f"error_report_{dtag}.csv",mime="text/csv",key="err_csv_dl",use_container_width=True)
        else: st.button("📥 No errors to export",disabled=True,key="err_csv_dis",use_container_width=True)
    with ec2:
        if not no_err:
            try:
                _xl=io.BytesIO()
                with pd.ExcelWriter(_xl,engine="openpyxl") as _w: detail_df[_DET_COLS].to_excel(_w,index=False,sheet_name="Error Report")
                _xl.seek(0)
                st.download_button("📊 Download as Excel (.xlsx)",data=_xl.read(),file_name=f"error_report_{dtag}.xlsx",mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",key="err_xl_dl",use_container_width=True)
            except ImportError: st.warning("Install openpyxl to enable Excel export.")
        else: st.button("📊 No errors to export",disabled=True,key="err_xl_dis",use_container_width=True)


# 17. AUDITOR LOGS
def render_auditor_logs(df,col_company,col_binder,col_agent_email=None):
    agent_opts=_get_opts(df,col_agent_email); company_opts=_get_opts(df,col_company); binder_opts=_get_opts(df,col_binder)
    srch_binder,srch_agent,srch_company=render_deep_search_strip("logs",col_binder,col_agent_email,col_company,binder_options=binder_opts,agent_options=agent_opts,company_options=company_opts)
    done_df=df[df[COL_STATUS]==VAL_DONE]
    if done_df.empty: st.info(t("logs_no_data")); return
    done_df=apply_deep_search(done_df,srch_binder,srch_agent,srch_company,col_binder,col_agent_email,col_company)
    if done_df.empty: st.info(t("logs_no_data")); return
    display_cols=[COL_AUDITOR,COL_DATE,COL_EVAL,COL_FEEDBACK]
    if col_company and col_company in done_df.columns: display_cols.insert(1,col_company)
    if col_binder and col_binder in done_df.columns: display_cols.insert(1,col_binder)
    if col_agent_email and col_agent_email in done_df.columns: display_cols.insert(2,col_agent_email)
    seen_c=set(); display_cols=[c for c in display_cols if c in done_df.columns and not (c in seen_c or seen_c.add(c))]
    auditor_list=sorted([a for a in done_df[COL_AUDITOR].unique() if str(a).strip() not in ("","-")],key=str.lower)
    all_opt=t("logs_filter_all"); sel_aud=st.selectbox(t("logs_auditor_sel"),options=[all_opt]+auditor_list,key="logs_auditor_sel")
    view_df=done_df[done_df[COL_AUDITOR]==sel_aud] if sel_aud!=all_opt else done_df
    total_p=len(view_df); uniq_a=view_df[COL_AUDITOR].nunique()
    valid_dates=pd.to_datetime(view_df[COL_DATE],format="%Y-%m-%d %H:%M:%S",errors="coerce").dropna()
    dr_str=(f"{valid_dates.min().strftime('%Y-%m-%d')} - {valid_dates.max().strftime('%Y-%m-%d')}" if not valid_dates.empty else "-")
    st.markdown(f"""<div class="log-summary-card"><div class="log-stat-row">
    <div class="log-stat"><span class="log-stat-value">{total_p}</span><span class="log-stat-label">{t('logs_total')}</span></div>
    <div class="log-stat-divider"></div>
    <div class="log-stat"><span class="log-stat-value">{uniq_a}</span><span class="log-stat-label">{t('logs_auditors')}</span></div>
    <div class="log-stat-divider"></div>
    <div class="log-stat"><span class="log-stat-value" style="font-size:1.05rem;">{dr_str}</span><span class="log-stat-label">{t('logs_date_range')}</span></div>
    </div></div>""",unsafe_allow_html=True)
    table_df=view_df[display_cols].copy()
    if COL_DATE in table_df.columns:
        table_df["_sort"]=pd.to_datetime(table_df[COL_DATE],format="%Y-%m-%d %H:%M:%S",errors="coerce")
        table_df=table_df.sort_values("_sort",ascending=False,na_position="last").drop(columns=["_sort"]).reset_index(drop=True)
    render_paginated_table(table_df,page_key="page_logs")
    full_view=view_df.copy()
    if COL_DATE in full_view.columns:
        full_view["_sort"]=pd.to_datetime(full_view[COL_DATE],format="%Y-%m-%d %H:%M:%S",errors="coerce")
        full_view=full_view.sort_values("_sort",ascending=False,na_position="last").drop(columns=["_sort"]).reset_index(drop=True)
    st.markdown(f"<hr class='divider'/><div class='section-title'>🔍 {t('inspector_title')}</div>",unsafe_allow_html=True)
    st.caption(t("inspector_hint"))
    _label_col=col_binder or col_company or (display_cols[0] if display_cols else None)
    def _row_label(i,row):
        aud=str(row.get(COL_AUDITOR,"")).strip() or "?"; dt=str(row.get(COL_DATE,"")).strip()[:10] or "?"
        hint=str(row[_label_col]).strip()[:40] if (_label_col and _label_col in row) else ""
        return f"#{i}  |  {aud}  |  {dt}  |  {hint}" if hint else f"#{i}  |  {aud}  |  {dt}"
    inspector_opts=[t("inspector_select")]+[_row_label(i,row) for i,row in full_view.iterrows()]
    sel_inspect=st.selectbox("",inspector_opts,key="logs_inspector_sel",label_visibility="collapsed")
    if sel_inspect!=t("inspector_select"):
        try: row_idx=int(sel_inspect.split("|")[0].replace("#","").strip())
        except (ValueError,IndexError): row_idx=None
        if row_idx is not None and 0<=row_idx<len(full_view):
            insp_row=full_view.iloc[row_idx]
            auditor_v=str(insp_row.get(COL_AUDITOR,"-")).strip() or "-"; date_v=str(insp_row.get(COL_DATE,"-")).strip() or "-"
            eval_v=str(insp_row.get(COL_EVAL,"-")).strip() or "-"; binder_v=str(insp_row.get(col_binder or "","-")).strip() if col_binder else "-"
            st.markdown(f"<div class='inspector-panel'><div class='inspector-meta'><div>Auditor&nbsp;&nbsp;<span>{_html.escape(auditor_v)}</span></div><div>Date&nbsp;&nbsp;<span>{_html.escape(date_v)}</span></div><div>Evaluation&nbsp;&nbsp;<span>{_html.escape(eval_v)}</span></div>{'<div>Binder&nbsp;&nbsp;<span>'+_html.escape(binder_v)+'</span></div>' if col_binder else ''}</div></div>",unsafe_allow_html=True)
            if COL_LOG in full_view.columns:
                trail=str(insp_row.get(COL_LOG,"")).strip()
                with st.expander(f"📜  {t('inspector_audit_trail')}",expanded=True):
                    if trail: st.code(trail,language="text")
                    else: st.info(t("inspector_empty_trail"))
            if COL_FEEDBACK in full_view.columns:
                fb=str(insp_row.get(COL_FEEDBACK,"")).strip()
                with st.expander(f"🛠️  {t('inspector_feedback')}",expanded=True):
                    if fb: st.code(fb,language="text")
                    else: st.info(t("inspector_empty_feedback"))
    csv_buf=io.StringIO(); table_df.to_csv(csv_buf,index=False,encoding="utf-8-sig"); csv_bytes=csv_buf.getvalue().encode("utf-8-sig")
    dtag=datetime.now(TZ).strftime("%Y%m%d"); atag=(sel_aud.replace("@","_").replace(".","_") if sel_aud!=all_opt else "all_auditors")
    st.markdown(f"""<div class="export-strip"><div><div style="font-weight:700;">{t('logs_export_hdr')}</div>
    <div style="font-size:.68rem;color:var(--text-muted);">{t('logs_export_sub')} — {total_p} rows</div></div></div>""",unsafe_allow_html=True)
    st.download_button(label=t("logs_export_btn"),data=csv_bytes,file_name=f"audit_log_{atag}_{dtag}.csv",mime="text/csv",key="logs_csv_download")


# =============================================================================
# 18. USER ADMIN  ·  v21.1 — Unified Permission Editor
# =============================================================================

def _render_staff_directory(staff: pd.DataFrame) -> None:
    """Render the read-only staff directory table."""
    if staff.empty or "email" not in staff.columns:
        st.info("No auditor accounts registered yet.")
        return

    show_cols = [c for c in [
        "email", "role", "created_at", "force_reset",
        "recovery_email", "allowed_tabs", "can_reopen",
        "allowed_projects", "allowed_registers"
    ] if c in staff.columns]

    tbl = staff[show_cols].copy().reset_index()
    th_html = "<tr><th class='row-idx'>#</th>" + "".join(
        f"<th>{_html.escape(c)}</th>" for c in show_cols
    ) + "</tr>"
    td_html = ""
    for _, row in tbl.iterrows():
        tr = f"<td class='row-idx'>{row['index']}</td>"
        for c in show_cols:
            val = str(row.get(c, "")) or "-"
            if c == "role":
                safe_role = val if val in VALID_ROLES else "auditor"
                tr += f"<td><span class='role-badge-{safe_role}'>{_html.escape(val.title())}</span></td>"
            elif c == "force_reset":
                clr = "#DC2626" if val.upper() == "TRUE" else "#16A34A"
                tr += f"<td><span style='color:{clr};font-weight:700;font-size:.75rem;'>{val}</span></td>"
            elif c == "recovery_email":
                masked_val = mask_email(val) if val not in ("-", "", "nan", "None") else "-"
                tr += f"<td style='color:var(--text-muted);font-size:.75rem;'>{_html.escape(masked_val)}</td>"
            elif c in ("allowed_tabs", "allowed_projects", "allowed_registers"):
                if val not in ("-", "", "nan", "None"):
                    pills = "".join(
                        f"<span style='display:inline-block;background:var(--indigo-50);color:var(--indigo-600);"
                        f"border:1px solid var(--indigo-100);border-radius:var(--radius-full);"
                        f"padding:1px 7px;font-size:.58rem;font-weight:700;margin:1px;'>"
                        f"{_html.escape(tt.strip())}</span>"
                        for tt in val.split(",") if tt.strip()
                    )
                    tr += f"<td style='white-space:normal;'>{pills}</td>"
                else:
                    tr += "<td><span style='color:var(--text-muted);font-size:.72rem;'>None</span></td>"
            elif c == "can_reopen":
                clr = "#16A34A" if val.upper() == "TRUE" else "#9CA3AF"
                icon = "✅" if val.upper() == "TRUE" else "❌"
                tr += f"<td><span style='color:{clr};font-weight:700;font-size:.75rem;'>{icon} {val}</span></td>"
            else:
                tr += f"<td>{_html.escape(val[:40])}</td>"
        td_html += f"<tr>{tr}</tr>"

    st.markdown(
        f"<div class='gov-table-wrap'><table class='gov-table'>"
        f"<thead>{th_html}</thead><tbody>{td_html}</tbody></table></div>",
        unsafe_allow_html=True,
    )


def _render_unified_permission_editor(staff: pd.DataFrame) -> None:
    """
    Unified User Profile & Permission Editor.

    Reads current settings for the selected user, presents a single
    st.form with all five permission fields, and on submit writes
    all changes in ONE gspread batch_update() call.
    """
    if staff.empty or "email" not in staff.columns:
        st.info("No users registered yet.")
        return

    email_list = staff["email"].tolist()

    # ── Section header ────────────────────────────────────────────────
    st.markdown(
        """
        <div style="display:flex;align-items:center;gap:14px;margin-bottom:6px;">
          <div style="width:40px;height:40px;border-radius:12px;flex-shrink:0;
               background:linear-gradient(135deg,#EEF2FF 0%,#E0E7FF 100%);
               display:flex;align-items:center;justify-content:center;font-size:1.2rem;">✏️</div>
          <div>
            <div style="font-size:1.05rem;font-weight:800;color:var(--text-primary);">
              Unified User Profile &amp; Permission Editor
            </div>
            <div style="font-size:.74rem;color:var(--text-muted);font-weight:500;margin-top:2px;">
              Select a user to view and edit all their settings in one place.
              All changes are written in a single API call.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── User selector (OUTSIDE the form so changing it re-renders defaults) ──
    ue_sel = st.selectbox(
        "Select user to edit",
        options=email_list,
        key="ue_user_sel",
        help="Switching users will reload the form with their current settings.",
        label_visibility="visible",
    )

    # Fetch current values for the selected user
    user_row = staff[staff["email"] == ue_sel]
    if user_row.empty:
        st.warning("User not found in staff list.")
        return

    def _get_str(col: str) -> str:
        if col not in user_row.columns: return ""
        v = str(user_row[col].values[0]).strip()
        return "" if v.lower() in ("nan", "none") else v

    cur_role      = _get_str("role") or "auditor"
    cur_reopen    = _get_str("can_reopen").upper() == "TRUE"
    cur_tabs_raw  = _get_str("allowed_tabs")
    cur_tabs      = _parse_allowed_tabs(cur_tabs_raw) if cur_tabs_raw else []
    cur_ap_raw    = _get_str("allowed_projects")
    cur_ap        = (["ALL"] if not cur_ap_raw or cur_ap_raw.upper() == "ALL"
                     else [x.strip() for x in cur_ap_raw.split(",") if x.strip()])
    cur_ar_raw    = _get_str("allowed_registers")
    cur_ar        = (["ALL"] if not cur_ar_raw or cur_ar_raw.upper() == "ALL"
                     else [x.strip() for x in cur_ar_raw.split(",") if x.strip()])

    all_projs = list(get_visible_projects("admin").keys())

    # ── Subtle current-state summary banner ──────────────────────────
    role_badge = {"admin": "role-badge-admin", "manager": "role-badge-manager",
                  "auditor": "role-badge-auditor"}.get(cur_role, "role-badge-auditor")
    reopen_badge = (
        "<span style='color:#16A34A;font-weight:700;font-size:.72rem;'>✅ Can re-open</span>"
        if cur_reopen else
        "<span style='color:#9CA3AF;font-weight:700;font-size:.72rem;'>❌ No re-open</span>"
    )
    st.markdown(
        f"""
        <div style="background:var(--surface-2);border:1px solid var(--border);
             border-left:3px solid var(--indigo-500);border-radius:var(--radius-md);
             padding:10px 16px;margin:10px 0 18px;display:flex;align-items:center;
             gap:14px;flex-wrap:wrap;">
          <span style="font-size:.65rem;font-weight:800;text-transform:uppercase;
               letter-spacing:.10em;color:var(--text-muted);">Current state</span>
          <span class="{role_badge}">{_html.escape(cur_role.title())}</span>
          {reopen_badge}
          <span style="font-size:.72rem;color:var(--text-secondary);font-weight:600;">
            Menus: {_html.escape(cur_tabs_raw or "None")}
          </span>
          <span style="font-size:.72rem;color:var(--text-secondary);font-weight:600;">
            Projects: {_html.escape(cur_ap_raw or "ALL")}
          </span>
          <span style="font-size:.72rem;color:var(--text-secondary);font-weight:600;">
            Registers: {_html.escape(cur_ar_raw or "ALL")}
          </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── The unified form — key is scoped to the selected email so Streamlit
    #    re-renders fresh defaults whenever the user selector changes ──────
    form_key = f"unified_perm_form__{ue_sel.replace('@','_').replace('.','_')}"
    with st.form(form_key):

        st.markdown(
            "<div style='font-size:.62rem;font-weight:800;text-transform:uppercase;"
            "letter-spacing:.10em;color:var(--indigo-600);margin-bottom:14px;'>"
            "🔐 Identity &amp; Access Control</div>",
            unsafe_allow_html=True,
        )

        # Row 1 — Role + Can Re-open
        col_role, col_reopen, col_spacer = st.columns([1.4, 1, 1.6], gap="medium")
        with col_role:
            new_role = st.selectbox(
                "Role",
                options=VALID_ROLES,
                index=VALID_ROLES.index(cur_role) if cur_role in VALID_ROLES else 0,
                format_func=lambda r: r.title(),
                help="Sets the user's system-wide access tier.",
            )
        with col_reopen:
            st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
            new_reopen = st.checkbox(
                "Can re-open processed records",
                value=cur_reopen,
                help="Allows the user to move archived records back to Pending.",
            )

        st.markdown("<hr style='border-top:1px dashed var(--border);margin:16px 0 14px;'/>",
                    unsafe_allow_html=True)

        # Row 2 — Portal Menus (full width)
        st.markdown(
            "<div style='font-size:.62rem;font-weight:800;text-transform:uppercase;"
            "letter-spacing:.10em;color:var(--indigo-600);margin-bottom:8px;'>"
            "🗂️ Portal Menus</div>",
            unsafe_allow_html=True,
        )
        new_tabs = st.multiselect(
            "Allowed Menus",
            options=ALL_TAB_OPTIONS,
            default=[tab for tab in cur_tabs if tab in ALL_TAB_OPTIONS],
            help="Which portal tabs the user can see. Leave empty to hide all tabs.",
            label_visibility="collapsed",
        )

        st.markdown("<hr style='border-top:1px dashed var(--border);margin:16px 0 14px;'/>",
                    unsafe_allow_html=True)

        # Row 3 — Data Access (2 columns)
        st.markdown(
            "<div style='font-size:.62rem;font-weight:800;text-transform:uppercase;"
            "letter-spacing:.10em;color:var(--indigo-600);margin-bottom:8px;'>"
            "📂 Data Access</div>",
            unsafe_allow_html=True,
        )
        col_projs, col_regs = st.columns(2, gap="medium")
        with col_projs:
            valid_ap = [p for p in cur_ap if p == "ALL" or p in all_projs]
            new_ap = st.multiselect(
                "Allowed Projects",
                options=["ALL"] + all_projs,
                default=valid_ap if valid_ap else ["ALL"],
                help="Select 'ALL' to grant access to every project.",
            )
        with col_regs:
            valid_ar = [r for r in cur_ar if r == "ALL" or r in VISIBLE_SHEETS]
            new_ar = st.multiselect(
                "Allowed Registers (Tabs)",
                options=["ALL"] + VISIBLE_SHEETS,
                default=valid_ar if valid_ar else ["ALL"],
                help="Select 'ALL' to grant access to every register.",
            )

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        # ── Single prominent save button ─────────────────────────────
        submitted = st.form_submit_button(
            "💾  Save All Changes",
            use_container_width=True,
            type="primary",
        )

    # ── On submit: one batch_update call, then clear cache + rerun ───
    if submitted:
        ap_str = "ALL" if "ALL" in new_ap or not new_ap else ",".join(new_ap)
        ar_str = "ALL" if "ALL" in new_ar or not new_ar else ",".join(new_ar)

        field_values = {
            "role":              new_role,
            "can_reopen":        "TRUE" if new_reopen else "FALSE",
            "allowed_tabs":      ",".join(new_tabs),
            "allowed_projects":  ap_str,
            "allowed_registers": ar_str,
        }

        with st.spinner(f"Saving permissions for {ue_sel}…"):
            try:
                gc  = get_gspread_client()
                uws = gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)

                # 1 API call — read header row
                header_row = _gsheets_call(uws.row_values, 1)
                # 1 API call — locate the user's row
                user_cell  = _gsheets_call(uws.find, ue_sel)

                if not user_cell:
                    st.error(f"User '{ue_sel}' not found in the sheet."); return

                # Build the batch — one entry per field that exists in the header
                batch = []
                for col_name, value in field_values.items():
                    if col_name in header_row:
                        col_idx = header_row.index(col_name) + 1
                        batch.append({
                            "range":  rowcol_to_a1(user_cell.row, col_idx),
                            "values": [[value]],
                        })

                # 1 API call — write everything at once
                if batch:
                    _gsheets_call(uws.batch_update, batch)

                # Bust the users cache so the directory refreshes
                _fetch_users_cached.clear()

                st.toast(f"✅ All permissions saved for {ue_sel}.", icon="✅")
                time.sleep(0.6)
                st.rerun()

            except gspread.exceptions.APIError as api_err:
                st.error(f"Google Sheets API error: {api_err}")
            except Exception as exc:
                st.error(f"Unexpected error: {exc}")


def render_user_admin():
    # ── Ensure all required columns exist (runs fast; idempotent) ────
    _ensure_recovery_email_col()
    _ensure_allowed_tabs_col()
    _ensure_can_reopen_col()
    _ensure_data_access_cols()

    # Fresh staff data after column-ensure calls
    staff_raw = _fetch_users_cached()
    staff = pd.DataFrame(staff_raw) if staff_raw else pd.DataFrame()
    if not staff.empty and "role" not in staff.columns:
        # Backfill role column if somehow missing
        try:
            gc = get_gspread_client(); uws = gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
            col_idx = len(staff.columns) + 1
            _gsheets_call(uws.update_cell, 1, col_idx, "role")
            for i in range(2, len(staff) + 2):
                _gsheets_call(uws.update_cell, i, col_idx, "auditor")
            _fetch_users_cached.clear()
            staff_raw = _fetch_users_cached()
            staff = pd.DataFrame(staff_raw) if staff_raw else pd.DataFrame()
        except: pass

    # =========================================================================
    # TOP SECTION — Add User (left)  |  Staff Directory + Revoke Access (right)
    # =========================================================================
    cl, cr = st.columns([1, 1], gap="large")

    # ── Left: Register New User + Update Password ─────────────────────
    with cl:
        st.markdown(f"<div class='section-title'>{t('add_auditor')}</div>", unsafe_allow_html=True)
        with st.form("add_user_form"):
            nu_e   = st.text_input("Official Email", placeholder="user@agents.tax.gov.krd")
            nu_rec = st.text_input(
                "Recovery Email (Personal / Gmail)",
                placeholder="e.g. firstname@gmail.com",
                help="Used to receive OTP codes for password reset.",
            )
            nu_p   = st.text_input(
                "Temporary Password", type="password",
                help="The user will be forced to change this on first login.",
            )
            nu_r   = st.selectbox(t("role_label"), VALID_ROLES, format_func=lambda r: r.title())
            nu_tabs = st.multiselect(
                "Portal Menus", options=ALL_TAB_OPTIONS, default=["Worklist"],
                help="Select which portal menus this user is permitted to access.",
            )
            all_projs_new = list(get_visible_projects("admin").keys())
            nu_projs = st.multiselect(
                "Allowed Projects", options=["ALL"] + all_projs_new, default=["ALL"],
                help="Select 'ALL' to grant access to everything.",
            )
            nu_regs = st.multiselect(
                "Allowed Registers (Tabs)", options=["ALL"] + VISIBLE_SHEETS, default=["ALL"],
                help="Select 'ALL' to grant access to every tab.",
            )
            if st.form_submit_button("Register User", use_container_width=True):
                if nu_e.strip() and nu_p.strip():
                    if not staff.empty and nu_e.lower().strip() in staff.get("email", pd.Series()).values:
                        st.error(t("dup_email"))
                    else:
                        ap_str = "ALL" if "ALL" in nu_projs or not nu_projs else ",".join(nu_projs)
                        ar_str = "ALL" if "ALL" in nu_regs  or not nu_regs  else ",".join(nu_regs)
                        gc  = get_gspread_client()
                        uws = gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
                        _gsheets_call(uws.append_row, [
                            nu_e.lower().strip(), hash_pw(nu_p.strip()), nu_r, now_str(),
                            "TRUE", nu_rec.lower().strip(), ",".join(nu_tabs), "FALSE",
                            ap_str, ar_str,
                        ])
                        _fetch_users_cached.clear()
                        st.success(f"{nu_e} registered as {nu_r} successfully.")
                        time.sleep(0.7); st.rerun()
                else:
                    st.warning(t("fill_fields"))

        st.markdown(f"<div class='section-title'>{t('update_pw')}</div>", unsafe_allow_html=True)
        if not staff.empty and "email" in staff.columns:
            with st.form("upd_pw_form"):
                se  = st.selectbox("Select staff", staff["email"].tolist(), key="upd_pw_sel")
                np_ = st.text_input("New Password", type="password")
                if st.form_submit_button("Update Password", use_container_width=True):
                    if np_.strip():
                        gc   = get_gspread_client()
                        uws  = gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
                        cell = _gsheets_call(uws.find, se)
                        if cell:
                            _gsheets_call(uws.update_cell, cell.row, 2, hash_pw(np_.strip()))
                            st.success(f"Password updated for {se}.")
                            time.sleep(0.7); st.rerun()

    # ── Right: Staff Directory + Revoke Access ────────────────────────
    with cr:
        st.markdown(f"<div class='section-title'>{t('staff_dir')}</div>", unsafe_allow_html=True)
        _render_staff_directory(staff)

        if not staff.empty and "email" in staff.columns:
            st.markdown(f"<div class='section-title'>{t('remove_user')}</div>", unsafe_allow_html=True)
            de = st.selectbox("Select to revoke", ["-"] + staff["email"].tolist(), key="del_sel")
            if de != "-":
                if st.button(f"Revoke access — {_html.escape(de)}", key="del_btn"):
                    gc   = get_gspread_client()
                    uws  = gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
                    cell = _gsheets_call(uws.find, de)
                    if cell:
                        _gsheets_call(uws.delete_rows, cell.row)
                        _fetch_users_cached.clear()
                        st.success(f"{de} revoked.")
                        time.sleep(0.7); st.rerun()

    # =========================================================================
    # BOTTOM SECTION — Unified Permission Editor (full width)
    # =========================================================================
    st.markdown("<hr class='divider' style='margin:28px 0;'/>", unsafe_allow_html=True)
    _render_unified_permission_editor(staff)


# PROJECT ADMIN + BACKUP + CONFIGURE SHEET RANGES
def render_project_admin() -> None:
    st.markdown("""<div style="display:flex;align-items:center;gap:16px;margin-bottom:28px;padding-bottom:22px;border-bottom:1px solid var(--border);">
    <div style="width:46px;height:46px;border-radius:14px;flex-shrink:0;background:linear-gradient(135deg,#EEF2FF 0%,#E0E7FF 100%);display:flex;align-items:center;justify-content:center;font-size:1.5rem;">⚙️</div>
    <div><div style="font-size:1.50rem;font-weight:800;color:var(--text-primary);line-height:1.1;">System & Project Settings</div>
    <div style="font-size:.76rem;color:var(--text-muted);font-weight:500;margin-top:3px;">Manage Google Sheets connections, backups, and reading ranges</div></div></div>""",unsafe_allow_html=True)
    
    st.markdown("<div class='section-title' style='font-size:.95rem;'>🗂️ Project Registry</div>",unsafe_allow_html=True)
    st.caption("Manage which Google Sheets projects are available in the portal.")
    df_raw=_fetch_projects_cached().copy(); df_raw["Is_Active"]=df_raw["Is_Active"].astype(bool)
    column_config={
        "Project_Name":st.column_config.TextColumn(label="Project Name",max_chars=80,required=True),
        "Sheet_ID":st.column_config.TextColumn(label="Google Sheet ID",max_chars=100,required=True),
        "Visible_To":st.column_config.SelectboxColumn(label="Visible To",options=VISIBILITY_OPTIONS,required=True),
        "Is_Active":st.column_config.CheckboxColumn(label="Active"),
    }
    edited_df=st.data_editor(df_raw,column_config=column_config,num_rows="dynamic",use_container_width=True,hide_index=True,key="projects_data_editor")
    validation_errors=[]
    if not edited_df.empty:
        if (edited_df["Project_Name"].astype(str).str.strip()=="").any(): validation_errors.append("⚠️ Some rows are missing a Project Name.")
        if (edited_df["Sheet_ID"].astype(str).str.strip()=="").any(): validation_errors.append("⚠️ Some rows are missing a Sheet ID.")
        dup_names=edited_df["Project_Name"].astype(str).str.strip(); dup_names=dup_names[dup_names!=""]
        if dup_names.duplicated().any(): validation_errors.append("⚠️ Duplicate Project Names detected.")
    for err in validation_errors: st.warning(err)
    st.markdown(f"<div style='background:#F0FDF4;border:1px solid #A7F3D0;border-radius:12px;padding:14px 18px;margin-top:16px;font-size:.80rem;font-weight:600;'><strong>{len(edited_df)}</strong> project(s) · <strong>{int(edited_df['Is_Active'].sum()) if not edited_df.empty else 0}</strong> active</div>",unsafe_allow_html=True)
    if st.button("💾  Save Project Registry",key="save_projects_btn",use_container_width=True,disabled=bool(validation_errors),type="primary"):
        clean_df=edited_df.copy()
        clean_df=clean_df[clean_df["Project_Name"].astype(str).str.strip().ne("")|clean_df["Sheet_ID"].astype(str).str.strip().ne("")].reset_index(drop=True)
        with st.spinner("Writing to Google Sheets..."):
            ok,msg=save_projects_to_sheet(clean_df)
        if ok: st.toast(msg,icon="✅"); time.sleep(0.8); st.rerun()
        else: st.error(msg)

    # ── FULL PROJECT BACKUP ──────────────────────────────────────────────────
    st.markdown("<hr class='divider'/>",unsafe_allow_html=True)
    st.markdown("<div class='section-title' style='font-size:.95rem;'>📥 Full Project Backup</div>",unsafe_allow_html=True)
    st.caption("Export every worksheet in a project into a single Excel workbook. Each Google Sheet tab becomes one Excel sheet.")

    df_proj=_fetch_projects_cached()
    active_projects=df_proj[df_proj["Is_Active"]==True] if not df_proj.empty else pd.DataFrame()

    if active_projects.empty:
        st.info("No active projects found in the registry.")
    else:
        proj_map={row["Project_Name"]:row["Sheet_ID"] for _,row in active_projects.iterrows() if row["Project_Name"] and row["Sheet_ID"]}
        if not proj_map:
            st.info("No valid projects available for backup.")
        else:
            backup_proj=st.selectbox("Select project to back up",options=list(proj_map.keys()),key="backup_proj_sel")
            if st.session_state.get("backup_last_sel")!=backup_proj:
                st.session_state.backup_excel_data=None; st.session_state.backup_excel_name=""
                st.session_state.backup_excel_proj=""; st.session_state.backup_last_sel=backup_proj

            if st.button("⚙️  Generate Full Backup",key="backup_gen_btn",use_container_width=False):
                backup_sid=proj_map[backup_proj]
                with st.spinner(f"Fetching all worksheets from '{backup_proj}'…"):
                    try:
                        gc=get_gspread_client(); spr=gc.open_by_key(backup_sid)
                        all_worksheets=spr.worksheets()
                        xl_buf=io.BytesIO()
                        with pd.ExcelWriter(xl_buf,engine="openpyxl") as writer:
                            sheets_written=0
                            for ws in all_worksheets:
                                try:
                                    raw=_gsheets_call(ws.get_all_values)
                                    if not raw:
                                        pd.DataFrame().to_excel(writer,sheet_name=ws.title[:31],index=False); continue
                                    df_ws,_,_=_raw_to_dataframe(raw)
                                    df_ws.to_excel(writer,sheet_name=ws.title[:31],index=False); sheets_written+=1
                                except Exception as ws_err:
                                    pd.DataFrame({"Error":[str(ws_err)]}).to_excel(writer,sheet_name=ws.title[:31],index=False)
                        xl_buf.seek(0)
                        dtag=datetime.now(TZ).strftime("%Y%m%d_%H%M"); safe_name=backup_proj.replace(" ","_").replace("/","_")[:30]
                        st.session_state.backup_excel_data=xl_buf.read(); st.session_state.backup_excel_name=f"backup_{safe_name}_{dtag}.xlsx"
                        st.session_state.backup_excel_proj=backup_proj; st.session_state.backup_last_sel=backup_proj
                        st.success(f"✅ Backup generated: {sheets_written} sheet(s) from '{backup_proj}'.")
                    except ImportError: st.error("openpyxl is required for Excel export. Run: pip install openpyxl")
                    except gspread.exceptions.APIError as e: st.error(f"Google Sheets API error: {e}")
                    except Exception as e: st.error(f"Backup failed: {e}")

            if (st.session_state.get("backup_excel_data") and st.session_state.get("backup_excel_proj")==backup_proj):
                st.markdown(f"""<div style="background:linear-gradient(135deg,#F0FDF4 0%,#EFF6FF 100%);border:1px solid var(--green-200);
                border-radius:var(--radius-md);padding:14px 18px;margin-top:14px;display:flex;align-items:center;gap:16px;">
                <span style="font-size:1.5rem;">📦</span>
                <div><div style="font-size:.80rem;font-weight:700;">Ready to download</div>
                <div style="font-size:.68rem;color:var(--text-muted);">{st.session_state.backup_excel_name} · {len(st.session_state.backup_excel_data)//1024+1} KB</div></div>
                </div>""",unsafe_allow_html=True)
                st.download_button(
                    label=f"⬇️  Download {st.session_state.backup_excel_name}",
                    data=st.session_state.backup_excel_data,
                    file_name=st.session_state.backup_excel_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="backup_download_btn",use_container_width=False,
                )

    # ── CONFIGURE SHEET RANGES ─────────────────────────────────────────
    st.markdown("<hr class='divider'/>",unsafe_allow_html=True)
    st.markdown("<div class='section-title' style='font-size:.95rem;'>⚙️ Configure Sheet Ranges</div>",unsafe_allow_html=True)
    st.caption(
        "Define a precise read range (e.g. A1:Z5000) for each project tab to avoid fetching the entire sheet. "
        "Leave blank or delete a saved range to revert to a full fetch. "
        "Changes take effect immediately on the next data load."
    )

    df_proj_r = _fetch_projects_cached()
    active_r   = df_proj_r[df_proj_r["Is_Active"] == True] if not df_proj_r.empty else pd.DataFrame()

    if active_r.empty:
        st.info("No active projects found. Add a project in the Project Registry above first.")
        return

    proj_map_r = {
        row["Project_Name"]: row["Sheet_ID"]
        for _, row in active_r.iterrows()
        if row["Project_Name"] and row["Sheet_ID"]
    }
    if not proj_map_r:
        st.info("No valid active projects available for range configuration.")
        return

    rc1, rc2 = st.columns([1, 1], gap="medium")
    with rc1:
        range_proj = st.selectbox(t("project_select"), options=list(proj_map_r.keys()), key="range_cfg_proj")
    range_sid = proj_map_r[range_proj]

    range_tabs = []
    with rc2:
        try: range_tabs = _fetch_sheet_metadata(range_sid)
        except Exception as fetch_err: st.error(f"Could not fetch tabs for '{range_proj}': {fetch_err}")

        if range_tabs:
            range_tab = st.selectbox(t("workspace"), options=range_tabs, key="range_cfg_tab")
        else:
            st.warning("No worksheets found for the selected project.")
            return

    existing_range = ""
    try:
        records   = _fetch_ranges_cached()
        if records:
            df_r = pd.DataFrame(records)
            if not df_r.empty and "Sheet_ID" in df_r.columns and "Tab_Name" in df_r.columns:
                match = df_r[
                    (df_r["Sheet_ID"].astype(str).str.strip() == range_sid.strip()) &
                    (df_r["Tab_Name"].astype(str).str.strip() == range_tab.strip())
                ]
                if not match.empty:
                    v = str(match["Read_Range"].values[0]).strip()
                    existing_range = v if v.lower() not in ("", "nan", "none") else ""
    except: pass

    if existing_range:
        st.markdown(
            f"<div style='background:var(--indigo-50);border:1px solid var(--indigo-100);border-left:3px solid var(--indigo-500);"
            f"border-radius:var(--radius-md);padding:10px 16px;margin-bottom:10px;font-size:.80rem;"
            f"color:var(--indigo-600)!important;font-weight:600;'>"
            f"✅ Current saved range for <strong>{_html.escape(range_proj)} → {_html.escape(range_tab)}</strong>: "
            f"<code style='background:#E0E7FF;padding:2px 6px;border-radius:4px;font-size:.78rem;'>{_html.escape(existing_range)}</code>"
            f"</div>", unsafe_allow_html=True
        )
    else:
        st.markdown(
            f"<div style='background:#FFFBEB;border:1px solid var(--amber-200);border-left:3px solid var(--amber-700);"
            f"border-radius:var(--radius-md);padding:10px 16px;margin-bottom:10px;font-size:.80rem;"
            f"color:var(--amber-700)!important;font-weight:600;'>"
            f"ℹ️ No custom range saved for <strong>{_html.escape(range_proj)} → {_html.escape(range_tab)}</strong>. "
            f"Currently using <strong>full sheet fetch</strong> (get_all_values)."
            f"</div>", unsafe_allow_html=True
        )

    new_range = st.text_input(
        "Enter Range  (e.g. A1:Z5000)", value=existing_range,
        key=f"range_cfg_input_{range_proj}_{range_tab}",
        placeholder="e.g. A1:Z5000   —   leave blank to revert to full sheet fetch",
        help="Use standard A1 notation. Rows are capped at this range, so set enough rows for your dataset.",
    )

    save_col, _ = st.columns([1, 3])
    with save_col:
        do_save = st.button("💾 Save Range", key="range_cfg_save_btn", use_container_width=True)

    if do_save:
        new_range_clean = new_range.strip().upper()
        try:
            gc  = get_gspread_client(); spr = gc.open_by_key(MASTER_USERS_ID)
            try: rws = spr.worksheet(RANGES_SHEET)
            except gspread.exceptions.WorksheetNotFound:
                rws = spr.add_worksheet(title=RANGES_SHEET, rows="200", cols="3")
                _gsheets_call(rws.append_row, RANGES_COLS)

            all_records  = _gsheets_call(rws.get_all_records)
            header_row_r = _gsheets_call(rws.row_values, 1)
            range_col_idx = (header_row_r.index("Read_Range") + 1) if "Read_Range" in header_row_r else None

            existing_row_idx = None
            for i, rec in enumerate(all_records, start=2):
                if (str(rec.get("Sheet_ID", "")).strip() == range_sid.strip() and
                        str(rec.get("Tab_Name", "")).strip() == range_tab.strip()):
                    existing_row_idx = i
                    break

            if existing_row_idx:
                if range_col_idx: _gsheets_call(rws.update_cell, existing_row_idx, range_col_idx, new_range_clean)
                else: _gsheets_call(rws.update, f"A{existing_row_idx}", [[range_sid, range_tab, new_range_clean]])
            else:
                _gsheets_call(rws.append_row, [range_sid, range_tab, new_range_clean])

            _fetch_ranges_cached.clear(); _fetch_raw_sheet_cached.clear(); fetch_combined_analytics.clear()

            if new_range_clean: st.success(f"✅ Range saved: **{range_proj} → {range_tab}** will now fetch `{new_range_clean}`.")
            else: st.success(f"✅ Range cleared for **{range_proj} → {range_tab}**.")
            time.sleep(0.8); st.rerun()
        except Exception as api_err: st.error(f"Google Sheets API error while saving range: {api_err}")

    try:
        all_ranges = _fetch_ranges_cached()
        if all_ranges:
            df_ranges = pd.DataFrame(all_ranges)
            if not df_ranges.empty and "Sheet_ID" in df_ranges.columns:
                proj_ranges = df_ranges[df_ranges["Sheet_ID"].astype(str).str.strip() == range_sid.strip()]
                if not proj_ranges.empty:
                    st.markdown(f"<div class='section-title' style='font-size:.72rem;margin-top:20px;'>📋 Saved Ranges for {_html.escape(range_proj)}</div>", unsafe_allow_html=True)
                    th_r = "<tr><th>Tab Name</th><th>Read Range</th><th>Fetch Mode</th></tr>"
                    td_r = ""
                    for _, row_r in proj_ranges.iterrows():
                        tab_n = _html.escape(str(row_r.get("Tab_Name", "")).strip())
                        rng_v = str(row_r.get("Read_Range", "")).strip()
                        if rng_v and rng_v.lower() not in ("nan", "none", ""): mode = f"<span class='s-chip s-eval-good'>Custom: {_html.escape(rng_v)}</span>"
                        else: mode = "<span class='s-chip s-pending'>Full Sheet</span>"
                        td_r += f"<tr><td style='font-weight:600;'>{tab_n}</td><td style='font-family:var(--mono);font-size:.75rem;'>{_html.escape(rng_v) or '—'}</td><td>{mode}</td></tr>"
                    st.markdown(f"<div class='gov-table-wrap'><table class='gov-table'><thead>{th_r}</thead><tbody>{td_r}</tbody></table></div>", unsafe_allow_html=True)
    except: pass


# 19. MAIN CONTROLLER
def main():
    cookie_manager=stx.CookieManager(key="portal_cm")

    if st.session_state.get("logged_in",False):
        current_user=st.session_state.user_email
        if current_user.lower()!="admin":
            try:
                df_u=pd.DataFrame(_fetch_users_cached())
                if df_u.empty or current_user not in df_u["email"].values:
                    for key in list(st.session_state.keys()): del st.session_state[key]
                    try: cookie_manager.delete(_COOKIE_NAME,key="live_val_del_cookie")
                    except: pass
                    st.error("⛔ Your access has been revoked by the administrator."); st.stop()
            except: pass

    if not st.session_state.get("logged_in",False):
        try:
            raw_cookie=cookie_manager.get(cookie=_COOKIE_NAME)
            if raw_cookie:
                parts=str(raw_cookie).split("|",1)
                if len(parts)==2:
                    c_email,c_role=parts[0].strip(),parts[1].strip()
                    user_still_valid=False
                    if c_role=="admin" and c_email.lower()=="admin":
                        user_still_valid=True
                    else:
                        try:
                            df_u=pd.DataFrame(_fetch_users_cached())
                            if not df_u.empty and "email" in df_u.columns:
                                user_still_valid=c_email in df_u["email"].values
                        except: pass
                    if user_still_valid and c_role in (VALID_ROLES+["admin"]):
                        st.session_state.logged_in=True; st.session_state.user_email=c_email; st.session_state.user_role=c_role
                        if c_role=="admin":
                            st.session_state.allowed_tabs=list(ALL_TAB_OPTIONS)
                            st.session_state.allowed_projects=["ALL"]
                            st.session_state.allowed_registers=["ALL"]
                        else:
                            try:
                                df_u=pd.DataFrame(_fetch_users_cached())
                                _row=df_u[df_u["email"]==c_email]
                                if not _row.empty:
                                    if "allowed_tabs" in _row.columns:
                                        st.session_state.allowed_tabs=_parse_allowed_tabs(str(_row["allowed_tabs"].values[0]))
                                    else: st.session_state.allowed_tabs=[]
                                    
                                    if "allowed_projects" in _row.columns:
                                        ap_raw = str(_row["allowed_projects"].values[0]).strip()
                                        st.session_state.allowed_projects = ["ALL"] if ap_raw.upper() == "ALL" or not ap_raw else [x.strip() for x in ap_raw.split(",") if x.strip()]
                                    else: st.session_state.allowed_projects=["ALL"]
                                    
                                    if "allowed_registers" in _row.columns:
                                        ar_raw = str(_row["allowed_registers"].values[0]).strip()
                                        st.session_state.allowed_registers = ["ALL"] if ar_raw.upper() == "ALL" or not ar_raw else [x.strip() for x in ar_raw.split(",") if x.strip()]
                                    else: st.session_state.allowed_registers=["ALL"]
                                else:
                                    st.session_state.allowed_tabs=[]; st.session_state.allowed_projects=["ALL"]; st.session_state.allowed_registers=["ALL"]
                            except: 
                                st.session_state.allowed_tabs=[]; st.session_state.allowed_projects=["ALL"]; st.session_state.allowed_registers=["ALL"]
                    else:
                        try: cookie_manager.delete(_COOKIE_NAME,key="delete_invalid_cookie")
                        except: pass
        except: pass

    try:
        inject_css()

        def _on_ws_change():
            for k in ("wl_binder","wl_license","arch_binder","arch_license","wl_company","arch_auditor","arch_company"):
                st.session_state[k]=None
            for prefix in ("anal","logs"):
                st.session_state[f"{prefix}_binder"]=None; st.session_state[f"{prefix}_agent"]=None; st.session_state[f"{prefix}_company"]=None
            for pk in ("page_worklist","page_archive","page_logs"): st.session_state.pop(pk,None)
            st.session_state["logs_inspector_sel"]=t("inspector_select"); st.session_state["local_cache_key"]=None
            st.session_state["local_df"]=None; st.session_state["local_headers"]=None; st.session_state["local_col_map"]=None
            _clear_review_state()

        def _on_project_change():
            _on_ws_change()

        try:
            master_titles=_fetch_sheet_metadata(MASTER_USERS_ID)
            if USERS_SHEET not in master_titles:
                gc=get_gspread_client(); spr=gc.open_by_key(MASTER_USERS_ID)
                uw=spr.add_worksheet(title=USERS_SHEET,rows="500",cols="10")
                _gsheets_call(uw.append_row,["email","password","role","created_at","force_reset","recovery_email","allowed_tabs","can_reopen","allowed_projects","allowed_registers"])
                _fetch_sheet_metadata.clear()
        except: pass

        try: _fetch_ranges_cached()
        except: pass

        if not st.session_state.logged_in:
            render_login(cookie_manager); return

        role=st.session_state.user_role; is_admin=(role=="admin")
        user_allowed_tabs=st.session_state.get("allowed_tabs",[])
        
        user_can_reopen = False
        if is_admin:
            user_can_reopen = True
        else:
            try:
                df_u = pd.DataFrame(_fetch_users_cached())
                if not df_u.empty and "can_reopen" in df_u.columns:
                    _row = df_u[df_u["email"] == st.session_state.user_email]
                    if not _row.empty:
                        user_can_reopen = str(_row["can_reopen"].values[0]).strip().upper() == "TRUE"
            except: pass

        role_label={"admin":t("role_admin"),"manager":t("role_manager"),"auditor":t("role_auditor")}.get(role,role.title())
        badge_cls={"admin":"role-badge-admin","manager":"role-badge-manager","auditor":"role-badge-auditor"}.get(role,"role-badge-auditor")

        h_left,h_right=st.columns([4,1],vertical_alignment="center")
        with h_left:
            ts_str=datetime.now(TZ).strftime("%A, %d %B %Y  -  %H:%M")
            st.markdown(f"""<div style="display:flex;align-items:center;gap:20px;margin-bottom:20px;">
            <div><div class="page-title">{_html.escape(t('portal_title'))}</div>
            <div class="page-subtitle">{_html.escape(t('ministry'))}</div></div>
            <div class="page-timestamp" style="margin-top:5px;">{ts_str}</div></div>""",unsafe_allow_html=True)
        with h_right:
            with st.popover("👤 Account",use_container_width=True):
                st.markdown(f"<div style='font-size:0.85rem;font-weight:700;'>{_html.escape(st.session_state.user_email)}</div>",unsafe_allow_html=True)
                st.markdown(f"<div style='margin-bottom:15px;'><span class='{badge_cls}'>{role_label}</span></div>",unsafe_allow_html=True)
                if role in ("admin","manager"):
                    COOLDOWN=600
                    if "last_refresh_time" not in st.session_state: st.session_state.last_refresh_time=0
                    time_passed=time.time()-st.session_state.last_refresh_time; can_refresh=not (role=="manager" and time_passed<COOLDOWN)
                    def _do_refresh():
                        _fetch_raw_sheet_cached.clear(); _fetch_users_cached.clear(); _fetch_projects_cached.clear()
                        _fetch_sheet_metadata.clear(); _fetch_ranges_cached.clear()
                        fetch_combined_analytics.clear()
                        st.session_state.local_cache_key=None; st.session_state.last_refresh_time=time.time()
                    if can_refresh: st.button("🔄 Refresh Data",key="top_refresh",use_container_width=True,on_click=_do_refresh)
                    else: st.button(f"⏳ Wait {max(1,int((COOLDOWN-time_passed)/60))} min",key="top_refresh_disabled",disabled=True,use_container_width=True)
                with st.expander(f"🔒 {t('update_pw')}",expanded=False):
                    with st.form("top_pw_form"):
                        new_pw=st.text_input(t("password_field"),type="password")
                        if st.form_submit_button(t("update_pw"),use_container_width=True):
                            if new_pw.strip():
                                try:
                                    gc=get_gspread_client(); uws=gc.open_by_key(MASTER_USERS_ID).worksheet(USERS_SHEET)
                                    cell=_gsheets_call(uws.find,st.session_state.user_email)
                                    if cell: _gsheets_call(uws.update_cell,cell.row,2,hash_pw(new_pw.strip())); _fetch_users_cached.clear(); st.success("Password updated!"); time.sleep(1); st.rerun()
                                except Exception as e: st.error(f"Error: {e}")
                            else: st.warning("Enter a new password.")
                if st.button(f"🚪 {t('sign_out')}",use_container_width=True,key="top_logout"):
                    try: cookie_manager.delete(_COOKIE_NAME,key="logout_del_top")
                    except: pass
                    for key in list(st.session_state.keys()): del st.session_state[key]
                    st.rerun()

        visible_projects=get_visible_projects(role)
        
        # [v21] Filter projects based on user permissions
        project_names=list(visible_projects.keys())
        user_allowed_projects = st.session_state.get("allowed_projects", ["ALL"])
        if not is_admin and "ALL" not in user_allowed_projects:
            project_names = [p for p in project_names if p in user_allowed_projects]

        if not project_names:
            st.warning("⚠️ No active projects assigned to your account.")
            if is_admin:
                st.info("Since you are an admin, please use the 'Project Admin' tab (if enabled) to add projects.")
            if st.button(f"🚪 {t('sign_out')}",key="no_proj_logout"):
                try: cookie_manager.delete(_COOKIE_NAME,key="logout_del_proj")
                except: pass
                for key in list(st.session_state.keys()): del st.session_state[key]
                st.rerun()
            return

        sel_col,tab_col=st.columns([1,1],gap="medium")
        with sel_col:
            if st.session_state.get("selected_project") not in project_names: st.session_state["selected_project"]=project_names[0]
            selected_project=st.selectbox(t("project_select"),options=project_names,key="selected_project",on_change=_on_project_change,help="Only projects assigned to you are shown.")
        current_sid=visible_projects[selected_project]
        st.markdown(f"<div class='project-banner'><span class='project-label'>📂 Active Project</span><span style='font-size:.88rem;font-weight:700;'>{_html.escape(selected_project)}</span><span style='font-size:.65rem;opacity:.55;margin-left:auto;font-family:var(--mono);'>{current_sid[:20]}…</span></div>",unsafe_allow_html=True)

        all_titles=[]
        try: all_titles=_fetch_sheet_metadata(current_sid)
        except Exception as e: st.error(f"Cannot open project '{selected_project}': {e}")
        atm={title.strip().lower():title for title in all_titles}
        available=[atm[s.strip().lower()] for s in VISIBLE_SHEETS if s.strip().lower() in atm]
        
        # [v21] Filter registers (tabs) based on user permissions
        user_allowed_registers = st.session_state.get("allowed_registers", ["ALL"])
        if not is_admin and "ALL" not in user_allowed_registers:
            available = [t for t in available if t in user_allowed_registers]

        df=pd.DataFrame(); headers=[]; col_map={}; ws_title=None; fetched_at="-"
        if not available:
            st.warning("You do not have access to any registers in this project, or none exist.")
        else:
            with tab_col: ws_title=st.selectbox(t("workspace"),options=available,key="ws_sel",on_change=_on_ws_change)
            try: df,headers,col_map,fetched_at=get_local_data(current_sid,ws_title)
            except gspread.exceptions.WorksheetNotFound: st.error(f"Worksheet '{ws_title}' not found in '{selected_project}'.")
            except gspread.exceptions.APIError as e: st.error(f"{t('retry_warning')}\n\n{e}")

        col_binder=detect_column(headers,"binder"); col_company=detect_column(headers,"company")
        col_license=detect_column(headers,"license"); col_agent_email=detect_column(headers,"agent_email")

        if not df.empty:
            # گۆڕینی کاتەکە بۆ سیستەمی 12 سەعاتی و AM/PM
            from datetime import datetime
            raw_time = fetched_at.split(" ")[1] if " " in fetched_at else fetched_at
            time_only = datetime.strptime(raw_time, "%H:%M:%S").strftime("%I:%M:%S %p")
            
            st.markdown(f"""
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:15px; margin-top:5px;">
                <div class='section-title' style="margin:0;">{t('overview')}</div>
                <div style="background:#F8FAFC; border:1px solid #E2E8F0; padding:6px 14px; border-radius:99px; font-size:0.72rem; font-weight:700; color:#475569; display:flex; align-items:center; gap:8px; box-shadow:var(--shadow-sm);">
                    <span style="font-size:15px; animation: pulseIcon 2s infinite;">⏱️</span> 
                    <span style="text-transform: uppercase; letter-spacing: 0.05em; margin-top:2px;">Last Refresh:</span> 
                    <span style="font-family:var(--mono); color:#4F46E5; font-weight:800; font-size:0.85rem; letter-spacing: 1px; background:#EEF2FF; padding:2px 8px; border-radius:6px; border:1px solid #C7D2FE;">{time_only}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            total_n=len(df); done_n=int((df[COL_STATUS]==VAL_DONE).sum()); pending_n=total_n-done_n
            pct=done_n/total_n if total_n else 0
            m1,m2,m3=st.columns(3)
            m1.metric(t("total"),total_n); m2.metric(t("processed"),done_n,delta=f"{int(pct*100)}%")
            m3.metric(t("outstanding"),pending_n,delta=f"{100-int(pct*100)}% remaining",delta_color="inverse")
            st.markdown(f"<div class='prog-labels'><span>{t('processed')}</span><span>{int(pct*100)}%</span></div><div class='prog-wrap'><div class='prog-fill' style='width:{int(pct*100)}%;'></div></div>",unsafe_allow_html=True)

        TAB_LABEL_MAP = {
            "Worklist":        t("tab_worklist"),
            "Archive":         t("tab_archive"),
            "Analytics":       t("tab_analytics"),
            "Raw Logs":        "📋 Raw Logs",
            "Error Analytics": "🔎 Error Analytics",
            "User Admin":      t("tab_users"),
            "Project Admin":   "⚙️ Project Admin",
        }
        tabs_to_show=[tab for tab in ALL_TAB_OPTIONS if tab in user_allowed_tabs]
        has_raw_logs   = "Raw Logs"        in user_allowed_tabs
        has_err_anal   = "Error Analytics" in user_allowed_tabs
        has_logs_tab   = has_raw_logs or has_err_anal

        display_tabs = []
        for tab in tabs_to_show:
            if tab in ("Raw Logs","Error Analytics"):
                if t("tab_logs") not in display_tabs: display_tabs.append(t("tab_logs"))
            else: display_tabs.append(TAB_LABEL_MAP[tab])

        if not display_tabs:
            st.markdown(f"<div class='rbac-banner'>{t('rbac_notice')}</div>",unsafe_allow_html=True)
            return

        rendered_tabs=st.tabs(display_tabs)
        tab_obj_map={label:obj for label,obj in zip(display_tabs,rendered_tabs)}

        if t("tab_worklist") in tab_obj_map:
            with tab_obj_map[t("tab_worklist")]:
                if not df.empty and ws_title:
                    pv=df[df[COL_STATUS]!=VAL_DONE]; pd_=pv.copy(); pd_.index=pd_.index+2
                    render_worklist(current_sid,pd_,df,headers,col_map,ws_title,col_binder,col_company,col_license)

        if t("tab_archive") in tab_obj_map:
            with tab_obj_map[t("tab_archive")]:
                if not df.empty and ws_title:
                    dv=df[df[COL_STATUS]==VAL_DONE].copy(); dv.index=dv.index+2
                    render_archive(current_sid,dv,df,col_map,ws_title,user_can_reopen,col_binder=col_binder,col_company=col_company,col_license=col_license)

        if t("tab_analytics") in tab_obj_map:
            with tab_obj_map[t("tab_analytics")]:
                if not df.empty:
                    render_analytics(df,current_sid,col_agent_email=col_agent_email,col_binder=col_binder,col_company=col_company)

        if has_logs_tab and t("tab_logs") in tab_obj_map:
            with tab_obj_map[t("tab_logs")]:
                sub_tab_labels=[]
                if has_raw_logs:  sub_tab_labels.append("📋 Auditor Logs")
                if has_err_anal:  sub_tab_labels.append("🔎 Error Analytics")
                if len(sub_tab_labels)==1:
                    if has_raw_logs:
                        if df.empty: st.warning(t("empty_sheet"))
                        else:        render_auditor_logs(df,col_company,col_binder,col_agent_email)
                    else: render_error_analytics(current_sid)
                else:
                    sub_tab_raw,sub_tab_err=st.tabs(sub_tab_labels)
                    with sub_tab_raw:
                        if df.empty: st.warning(t("empty_sheet"))
                        else:        render_auditor_logs(df,col_company,col_binder,col_agent_email)
                    with sub_tab_err: render_error_analytics(current_sid)

        if t("tab_users") in tab_obj_map:
            with tab_obj_map[t("tab_users")]:
                render_user_admin()

        if "⚙️ Project Admin" in tab_obj_map:
            with tab_obj_map["⚙️ Project Admin"]:
                render_project_admin()

    except Exception as exc:
        st.error(f"System Error: {exc}")
        with st.expander("Technical Details",expanded=False): st.exception(exc)

if __name__=="__main__":
    main()
