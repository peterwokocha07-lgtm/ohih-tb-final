import os
import json
import datetime as dt
from typing import Dict, Any, Optional

import pandas as pd
import requests
import streamlit as st

try:
    import plotly.express as px
except Exception:
    px = None

# ============================================================
# BRANDING (OHIH-TB) + NATIONAL DASHBOARD LOOK (NO PATCHING)
# ============================================================
TB_ICON = "🫁"
APP_SHORT = "OHIH-TB"
APP_FULL = "One Health Intelligence Hub for TB (OHIH-TB)"

st.set_page_config(page_title=APP_SHORT, layout="wide")

st.markdown(
    """
<style>
/* --- Top bar --- */
.ohih-topbar{
  background: linear-gradient(90deg, #0b3b8f 0%, #0ea5e9 40%, #16a34a 70%, #ef4444 100%);
  padding: 14px 16px;
  border-radius: 16px;
  color: white;
  box-shadow: 0 10px 28px rgba(0,0,0,0.14);
  margin-bottom: 12px;
}
.ohih-title{
  font-size: 26px;
  font-weight: 800;
  letter-spacing: .2px;
  margin: 0;
}
.ohih-sub{
  margin-top: 4px;
  font-size: 13px;
  opacity: .92;
}
.ohih-badges{
  display:flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 10px;
}
.ohih-badge{
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding: 6px 10px;
  border-radius: 999px;
  background: rgba(255,255,255,.16);
  border: 1px solid rgba(255,255,255,.22);
  font-size: 12px;
  font-weight: 600;
}
.ohih-kpis{
  display:flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 12px;
}
.ohih-kpi{
  background: rgba(255,255,255,.12);
  border: 1px solid rgba(255,255,255,.20);
  border-radius: 14px;
  padding: 10px 12px;
  min-width: 160px;
}
.ohih-kpi .k{
  font-size: 11px;
  opacity: .90;
  margin-bottom: 4px;
}
.ohih-kpi .v{
  font-size: 18px;
  font-weight: 800;
}
.ohih-kpi .s{
  font-size: 11px;
  opacity: .90;
  margin-top: 2px;
}

/* --- Section header --- */
.ohih-section{
  padding: 10px 12px;
  border-radius: 14px;
  border: 1px solid rgba(2,6,23,.10);
  background: rgba(255,255,255,.70);
  box-shadow: 0 8px 18px rgba(2,6,23,.06);
  margin: 8px 0 12px 0;
}
.ohih-section h2{
  margin: 0;
  font-size: 20px;
  font-weight: 800;
}

/* --- Subtle divider --- */
hr{
  border: none;
  border-top: 1px solid rgba(2,6,23,.10);
  margin: 12px 0;
}
</style>
""",
    unsafe_allow_html=True,
)

def section(title: str):
    st.markdown(
        f"""
<div class="ohih-section">
  <h2>{TB_ICON} {title}</h2>
</div>
""",
        unsafe_allow_html=True,
    )

# =========================
# CONFIG / SECRETS
# =========================
def safe_secret(name: str, default: str = "") -> str:
    try:
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets.get(name))
    except Exception:
        pass
    return os.getenv(name, default)

import streamlit as st

SUPABASE_URL = st.secrets.get("SUPABASE_URL", "").strip()
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY", "").strip()

AUTH_BASE = f"{SUPABASE_URL}/auth/v1"
REST_BASE = f"{SUPABASE_URL}/rest/v1"

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Missing SUPABASE_URL or SUPABASE_ANON_KEY in Streamlit Secrets.")
    st.stop()

# Build API base URLs from SUPABASE_URL
AUTH_BASE = f"{SUPABASE_URL}/auth/v1"
REST_BASE = f"{SUPABASE_URL}/rest/v1"

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

# =========================
# REST HELPERS
# =========================
def rest_headers(access_token: str = "") -> Dict[str, str]:
    h = {"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"}
    if access_token:
        h["Authorization"] = f"Bearer {access_token}"
    return h

def rest_get(table: str, access_token: str, params: Dict[str, str]) -> requests.Response:
    url = f"{REST_BASE}/{table}"
    return requests.get(url, headers=rest_headers(access_token), params=params, timeout=30)

def rest_post(table: str, access_token: str, payload: Any) -> requests.Response:
    url = f"{REST_BASE}/{table}"
    h = rest_headers(access_token)
    h["Prefer"] = "return=representation"
    return requests.post(url, headers=h, data=json.dumps(payload), timeout=30)

def rest_patch(table: str, access_token: str, match_params: Dict[str, str], payload: Any) -> requests.Response:
    url = f"{REST_BASE}/{table}"
    h = rest_headers(access_token)
    h["Prefer"] = "return=representation"
    return requests.patch(url, headers=h, params=match_params, data=json.dumps(payload), timeout=30)

def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def df_select(table: str, params: Dict[str, str]) -> pd.DataFrame:
    tok = st.session_state["access_token"]
    r = rest_get(table, tok, params=params)
    if r.status_code != 200:
        raise RuntimeError(f"{table} load failed: {r.status_code} {r.text}")
    return _clean_df(pd.DataFrame(r.json() or []))

def insert_row(table: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    tok = st.session_state["access_token"]
    r = rest_post(table, tok, payload)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"{table} insert failed: {r.status_code} {r.text}")
    rows = r.json()
    return rows[0] if isinstance(rows, list) and rows else (rows or {})

def safe_select_with_order(table: str, base_params: Dict[str, str], order_candidates) -> pd.DataFrame:
    last_err = None
    for o in order_candidates:
        try:
            p = dict(base_params)
            p["order"] = o
            return df_select(table, p)
        except Exception as e:
            last_err = e
    if last_err:
        raise last_err
    return df_select(table, base_params)

# =========================
# AUTH
# =========================
def auth_sign_in(email: str, password: str) -> Dict[str, Any]:
    url = f"{AUTH_BASE}/token"
    params = {"grant_type": "password"}
    payload = {"email": email, "password": password}
    h = {"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"}
    r = requests.post(url, headers=h, params=params, data=json.dumps(payload), timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Login failed: {r.status_code} {r.text}")
    return r.json()

# =========================
# SESSION
# =========================
def ss_init():
    st.session_state.setdefault("access_token", "")
    st.session_state.setdefault("user_id", "")
    st.session_state.setdefault("profile", {})
    st.session_state.setdefault("facility_name", "")
    st.session_state.setdefault("role", "standard")

ss_init()

def is_logged_in() -> bool:
    return bool(st.session_state.get("access_token")) and bool(st.session_state.get("user_id"))

def logout():
    for k in list(st.session_state.keys()):
        del st.session_state[k]
    ss_init()
    st.rerun()

def load_profile() -> Dict[str, Any]:
    tok = st.session_state["access_token"]
    r = rest_get("staff_profiles", tok, params={"select": "*", "limit": "1"})
    if r.status_code != 200:
        raise RuntimeError(f"Profile load failed: {r.status_code} {r.text}")
    rows = r.json()
    return rows[0] if rows else {}

def load_facility_name() -> str:
    tok = st.session_state["access_token"]
    r = rest_get("facilities", tok, params={"select": "facility_name", "limit": "1"})
    if r.status_code != 200:
        return ""
    rows = r.json()
    return str(rows[0].get("facility_name")) if rows else ""

# =========================
# KPI HELPERS (best-effort, never crash)
# =========================
def _safe_len(table: str) -> int:
    try:
        df = df_select(table, {"select": "*", "limit": "1"})
        # PostgREST doesn't return total count unless asked; use quick scan with bigger limit
        df2 = df_select(table, {"select": "*", "limit": "50000"})
        return int(len(df2))
    except Exception:
        return 0

def _who_latest_metrics() -> Dict[str, Any]:
    """
    Pull KPIs from v_who_indicators_monthly if available.
    Works for both organizer and facility user.
    """
    try:
        dfw = df_select("v_who_indicators_monthly", {"select": "*", "limit": "50000"})
        if dfw.empty or "month" not in dfw.columns:
            return {}
        if "facility_id" in dfw.columns and st.session_state.get("role") != "organizer":
            # facility-only
            facid = str(st.session_state.get("profile", {}).get("facility_id"))
            dfw = dfw[dfw["facility_id"].astype(str) == facid]
        dfw["month"] = pd.to_datetime(dfw["month"], errors="coerce")
        latest = dfw["month"].max()
        cur = dfw[dfw["month"] == latest].copy()
        return {
            "month": latest,
            "presumptive": int(cur.get("presumptive_total", pd.Series([0])).sum()),
            "confirmed": int(cur.get("confirmed_tb", pd.Series([0])).sum()),
            "genx_pos": int(cur.get("genexpert_positive", pd.Series([0])).sum()),
            "genx_neg": int(cur.get("genexpert_negative", pd.Series([0])).sum()),
        }
    except Exception:
        return {}

def render_topbar():
    prof = st.session_state.get("profile") or {}
    fac_name = st.session_state.get("facility_name") or "—"
    role = st.session_state.get("role") or "standard"

    k = _who_latest_metrics()
    month_str = "—"
    if k.get("month") is not None and pd.notna(k.get("month")):
        month_str = pd.to_datetime(k["month"]).strftime("%Y-%m")

    presumptive = k.get("presumptive", 0)
    confirmed = k.get("confirmed", 0)
    genx_pos = k.get("genx_pos", 0)

    # fallback if WHO view not ready
    if not k:
        presumptive = 0
        confirmed = 0
        genx_pos = 0

    st.markdown(
        f"""
<div class="ohih-topbar">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;">
    <div>
      <div class="ohih-title">{TB_ICON} {APP_FULL}</div>
      <div class="ohih-sub">National-grade TB Intelligence • RLS + Auth • Multi-facility isolation • WHO reporting-ready</div>
      <div class="ohih-badges">
        <span class="ohih-badge">🏥 Facility: {fac_name}</span>
        <span class="ohih-badge">🛡️ Role: {role}</span>
        <span class="ohih-badge">🔒 RLS: ON</span>
        <span class="ohih-badge">📡 Surveillance: ACTIVE</span>
      </div>
    </div>
    <div class="ohih-kpis">
      <div class="ohih-kpi"><div class="k">Reporting Month</div><div class="v">{month_str}</div><div class="s">Latest WHO indicators</div></div>
      <div class="ohih-kpi"><div class="k">Presumptive TB</div><div class="v">{presumptive}</div><div class="s">This month</div></div>
      <div class="ohih-kpi"><div class="k">Confirmed TB</div><div class="v">{confirmed}</div><div class="s">This month</div></div>
      <div class="ohih-kpi"><div class="k">GeneXpert +</div><div class="v">{genx_pos}</div><div class="s">This month</div></div>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

# =========================
# SIDEBAR
# =========================
with st.sidebar:
    st.subheader(f"{TB_ICON} Session")
    if is_logged_in():
        st.write("User:", st.session_state.get("user_id"))
        st.write("Role:", st.session_state.get("role"))
        st.write("Facility:", st.session_state.get("facility_name"))
        if st.button("Logout"):
            logout()
    st.divider()

# =========================
# LOGIN
# =========================
if not is_logged_in():
    render_topbar()
    section("Login")
    st.subheader("Login (Email + Password)")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    if st.button("Login", type="primary"):
        out = auth_sign_in(email.strip(), password)
        st.session_state["access_token"] = out["access_token"]
        st.session_state["user_id"] = out["user"]["id"]

        prof = load_profile()
        if not prof:
            st.error("No staff profile found for this user. Fix staff_profiles in Supabase.")
            st.stop()

        st.session_state["profile"] = prof
        st.session_state["role"] = prof.get("role", "standard")
        st.session_state["facility_name"] = load_facility_name()
        st.success("Login OK")
        st.rerun()
    st.stop()

# =========================
# CONTEXT
# =========================
profile = st.session_state.get("profile") or {}
facility_id = profile.get("facility_id")
if not facility_id:
    st.error("staff_profiles.facility_id missing. Fix in Supabase.")
    st.stop()

# =========================
# COMMON HELPERS
# =========================
def patient_picker() -> Optional[str]:
    dfp = safe_select_with_order(
        "patients",
        {"select": "patient_id,full_name,created_at", "limit": "5000"},
        ["created_at.desc", "updated_at.desc", "patient_id.desc"],
    )
    if dfp.empty:
        st.info("No patients yet. Add one first.")
        return None
    labels = (dfp["patient_id"].astype(str) + " — " + dfp["full_name"].astype(str)).tolist()
    chosen = st.selectbox("Select patient", labels)
    return chosen.split(" — ")[0].strip()

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df

def parse_bool_detected(v: Any) -> bool:
    s = str(v).strip().lower()
    return ("detected" in s) and ("not" not in s)

def classify_resistance(rr: bool, inh: bool, fq: bool, bdq: bool, lzd: bool) -> str:
    if not any([rr, inh, fq, bdq, lzd]):
        return "Drug-sensitive"
    if rr and inh:
        if fq and (bdq or lzd):
            return "XDR-TB"
        if fq:
            return "Pre-XDR"
        return "MDR-TB"
    if rr:
        return "RR-TB"
    return "Drug-sensitive"

# =========================
# PAGES
# =========================
def page_home():
    render_topbar()
    section("Home")
    st.success("✅ Authenticated. RLS isolates data per facility. WHO Dashboard + GIS + Alerts enabled.")
    st.write("Facility ID:", facility_id)
    st.write("Role:", st.session_state.get("role"))

def page_patients():
    render_topbar()
    section("Patients")

    with st.expander("➕ Register new patient", expanded=True):
        full_name = st.text_input("Full name *")
        age = st.number_input("Age", 0, 120, 30)
        sex = st.selectbox("Sex", ["Male", "Female", "Other"])
        phone = st.text_input("Phone")
        address = st.text_area("Address")
        if st.button("Save patient", type="primary"):
            if not full_name.strip():
                st.error("Full name required.")
                st.stop()
            payload = {
                "facility_id": facility_id,
                "full_name": full_name.strip(),
                "age": int(age),
                "sex": sex,
                "phone": phone.strip(),
                "address": address.strip(),
                "created_at": now_iso(),
            }
            out = insert_row("patients", payload)
            st.success(f"Saved ✅ Patient: {out.get('patient_id')}")
            st.rerun()

    dfp = safe_select_with_order("patients", {"select": "*", "limit": "5000"}, ["created_at.desc", "updated_at.desc", "patient_id.desc"])
    st.dataframe(dfp, use_container_width=True, hide_index=True)

def page_diagnosis_events():
    render_topbar()
    section("Diagnosis Events")
    pid = patient_picker()
    if not pid:
        st.stop()

    tb_probability = st.slider("TB Probability", 0.0, 1.0, 0.2, 0.01)
    category = st.selectbox("Category", ["LOW", "MODERATE", "HIGH", "CONFIRMED TB"])
    genexpert = st.selectbox("GeneXpert", ["Not done", "Positive", "Negative"])
    smear = st.selectbox("Smear", ["Not done", "Positive", "Negative"])
    cxr = st.selectbox("CXR", ["Not done", "Suggestive", "Not suggestive"])
    notes = st.text_area("Notes")

    if st.button("Save event", type="primary"):
        payload = {
            "facility_id": facility_id,
            "patient_id": pid,
            "tb_probability": float(tb_probability),
            "category": category,
            "genexpert": genexpert,
            "smear": smear,
            "cxr": cxr,
            "notes": notes.strip(),
            "timestamp": now_iso(),
        }
        insert_row("events", payload)
        st.success("Saved ✅")
        st.rerun()

    dfe = safe_select_with_order("events", {"select": "*", "limit": "5000"}, ["timestamp.desc", "created_at.desc", "event_id.desc"])
    st.dataframe(dfe, use_container_width=True, hide_index=True)

def page_dots():
    render_topbar()
    section("DOTS")
    pid = patient_picker()
    if not pid:
        st.stop()

    date = st.date_input("Date", value=dt.date.today())
    dose_taken = st.checkbox("Dose taken", True)
    note = st.text_input("Note")

    if st.button("Save DOTS", type="primary"):
        payload = {
            "facility_id": facility_id,
            "patient_id": pid,
            "date": date.isoformat(),
            "dose_taken": bool(dose_taken),
            "note": note.strip(),
            "created_at": now_iso(),
        }
        r = rest_post("dots_daily", st.session_state["access_token"], payload)
        if r.status_code in (200, 201):
            st.success("Saved ✅")
            st.rerun()
        else:
            if "duplicate key" in r.text.lower() or r.status_code == 409:
                match = {"facility_id": f"eq.{facility_id}", "patient_id": f"eq.{pid}", "date": f"eq.{date.isoformat()}"}
                rp = rest_patch("dots_daily", st.session_state["access_token"], match, {"dose_taken": bool(dose_taken), "note": note.strip(), "updated_at": now_iso()})
                if rp.status_code in (200, 204):
                    st.success("Updated ✅")
                    st.rerun()
                else:
                    st.error(f"DOTS save failed: {rp.status_code} {rp.text}")
            else:
                st.error(f"DOTS save failed: {r.status_code} {r.text}")

    dfd = safe_select_with_order("dots_daily", {"select": "*", "limit": "5000"}, ["date.desc"])
    st.dataframe(dfd, use_container_width=True, hide_index=True)

def page_adherence():
    render_topbar()
    section("Adherence")
    pid = patient_picker()
    if not pid:
        st.stop()

    missed_7 = st.number_input("Missed doses (last 7 days)", 0, 7, 0)
    missed_28 = st.number_input("Missed doses (last 28 days)", 0, 28, 0)
    missed_streak = st.selectbox("Longest missed streak", ["0 days", "1–2 days", "3–6 days", "1 week", "2 weeks", "3 weeks", "1 month+"])
    completed = st.checkbox("Completed regimen", False)

    adh_7 = max(0.0, 100.0 * (1 - missed_7 / 7))
    adh_28 = max(0.0, 100.0 * (1 - missed_28 / 28))
    flag = missed_28 >= 8
    risk = "High" if flag or missed_streak in ("2 weeks", "3 weeks", "1 month+") else ("Moderate" if missed_streak in ("1 week", "3–6 days") else "Low")

    st.write(f"Adherence 7d: {adh_7:.1f}% | 28d: {adh_28:.1f}% | Risk: {risk}")

    notes = st.text_area("Notes")
    if st.button("Save adherence snapshot", type="primary"):
        payload = {
            "facility_id": facility_id,
            "patient_id": pid,
            "missed_7": int(missed_7),
            "missed_28": int(missed_28),
            "missed_streak": missed_streak,
            "completed": bool(completed),
            "adh_7_pct": float(adh_7),
            "adh_28_pct": float(adh_28),
            "flag_over_25pct": bool(flag),
            "risk_level": risk,
            "notes": notes.strip(),
            "timestamp": now_iso(),
        }
        insert_row("adherence", payload)
        st.success("Saved ✅")
        st.rerun()

    dfa = safe_select_with_order("adherence", {"select": "*", "limit": "5000"}, ["timestamp.desc", "created_at.desc", "created_by.desc"])
    st.dataframe(dfa, use_container_width=True, hide_index=True)

def page_treatment():
    render_topbar()
    section("Treatment")
    pid = patient_picker()
    if not pid:
        st.stop()

    start_date = st.date_input("Start date", value=dt.date.today())
    phase = st.selectbox("Phase", ["Intensive", "Continuation"])
    regimen = st.text_input("Regimen")
    outcome = st.selectbox("Outcome", ["On treatment", "Cured", "Completed", "Failed", "LTFU", "Transferred", "Died"])
    notes = st.text_area("Notes")

    if st.button("Save treatment update", type="primary"):
        payload = {
            "facility_id": facility_id,
            "patient_id": pid,
            "start_date": start_date.isoformat(),
            "phase": phase,
            "regimen": regimen.strip(),
            "outcome": outcome,
            "notes": notes.strip(),
            "updated_at": now_iso(),
        }
        insert_row("tb_treatment", payload)
        st.success("Saved ✅")
        st.rerun()

    dft = safe_select_with_order("tb_treatment", {"select": "*", "limit": "5000"}, ["updated_at.desc", "created_at.desc"])
    st.dataframe(dft, use_container_width=True, hide_index=True)

def page_contact_tracing():
    render_topbar()
    section("Contact Tracing (WHO)")
    st.caption("Register household/close contacts for an index TB patient and track screening status.")

    pid = patient_picker()
    if not pid:
        st.stop()

    dfc = safe_select_with_order("tb_contacts", {"select": "*", "index_patient_id": f"eq.{pid}", "limit": "5000"}, ["created_at.desc", "updated_at.desc"])

    with st.expander("➕ Add new contact", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            name = st.text_input("Contact full name *")
            age = st.number_input("Age", 0, 120, 20, key="ct_age")
            sex = st.selectbox("Sex", ["Male", "Female", "Other"], key="ct_sex")
            phone = st.text_input("Phone", key="ct_phone")
        with c2:
            relationship = st.text_input("Relationship to index", value="", key="ct_rel")
            setting = st.selectbox("Exposure setting", ["household", "workplace", "school", "other"], key="ct_set")
            last_exp = st.date_input("Last exposure date", value=dt.date.today(), key="ct_last")
        with c3:
            cough = st.checkbox("Cough", key="ct_cough")
            fever = st.checkbox("Fever", key="ct_fever")
            night_sweats = st.checkbox("Night sweats", key="ct_ns")
            weight_loss = st.checkbox("Weight loss", key="ct_wl")
            hiv_pos = st.checkbox("HIV positive", key="ct_hiv")
            dm = st.checkbox("Diabetes", key="ct_dm")

        if st.button("Save contact", type="primary"):
            if not name.strip():
                st.error("Contact full name is required.")
                st.stop()

            payload = {
                "facility_id": facility_id,
                "index_patient_id": pid,
                "full_name": name.strip(),
                "age": int(age),
                "sex": sex,
                "phone": phone.strip(),
                "relationship_to_index": relationship.strip(),
                "exposure_setting": setting,
                "last_exposure_date": last_exp.isoformat(),
                "cough": bool(cough),
                "fever": bool(fever),
                "night_sweats": bool(night_sweats),
                "weight_loss": bool(weight_loss),
                "hiv_positive": bool(hiv_pos),
                "diabetes": bool(dm),
                "screening_status": "Pending",
                "created_at": now_iso(),
                "updated_at": now_iso(),
            }
            insert_row("tb_contacts", payload)
            st.success("Saved ✅")
            st.rerun()

    st.subheader("Contacts")
    st.dataframe(dfc, use_container_width=True, hide_index=True)

def page_drug_resistance():
    render_topbar()
    section("Drug Resistance (RR/MDR/XDR)")

    pid = patient_picker()
    if not pid:
        st.stop()

    col1, col2 = st.columns(2)
    with col1:
        rif = st.checkbox("Rifampicin resistant (RR)")
        inh = st.checkbox("Isoniazid resistant (INH)")
        fq = st.checkbox("Fluoroquinolone resistant (FQ)")
    with col2:
        bdq = st.checkbox("Bedaquiline resistant (BDQ)")
        lzd = st.checkbox("Linezolid resistant (LZD)")
        test_method = st.text_input("Test method (GeneXpert / LPA / Culture DST)")

    notes = st.text_area("Notes (optional)")
    resistance_class = classify_resistance(rif, inh, fq, bdq, lzd)
    st.info(f"**Resistance class:** {resistance_class}")

    if st.button("Save resistance record", type="primary"):
        payload = {
            "facility_id": facility_id,
            "patient_id": pid,
            "rifampicin_resistant": bool(rif),
            "isoniazid_resistant": bool(inh),
            "fluoroquinolone_resistant": bool(fq),
            "bedaquiline_resistant": bool(bdq),
            "linezolid_resistant": bool(lzd),
            "resistance_class": resistance_class,
            "test_method": test_method.strip(),
            "notes": notes.strip(),
            "created_at": now_iso(),
        }
        insert_row("tb_drug_resistance", payload)
        st.success("Saved ✅")
        st.rerun()

    dfr = safe_select_with_order("tb_drug_resistance", {"select": "*", "patient_id": f"eq.{pid}", "limit": "5000"}, ["created_at.desc", "updated_at.desc"])
    st.dataframe(dfr, use_container_width=True, hide_index=True)

def page_genexpert_import():
    render_topbar()
    section("GeneXpert Import (CSV)")
    st.caption("Upload a CSV with columns: full_name, age, sex, mtb_result, rif_result, notes")

    up = st.file_uploader("Upload GeneXpert CSV", type=["csv"])
    if not up:
        st.info("Upload a CSV to import.")
        return

    df = pd.read_csv(up)
    df = normalize_cols(df)
    st.dataframe(df.head(20), use_container_width=True)

    col_name = "full_name" if "full_name" in df.columns else ("patient_name" if "patient_name" in df.columns else None)
    col_age = "age" if "age" in df.columns else None
    col_sex = "sex" if "sex" in df.columns else None
    col_mtb = "mtb_result" if "mtb_result" in df.columns else None
    col_rif = "rif_result" if "rif_result" in df.columns else None
    col_notes = "notes" if "notes" in df.columns else None

    if not col_name or not col_mtb or not col_rif:
        st.error("CSV must contain: full_name (or patient_name), mtb_result, rif_result")
        return

    if st.button("IMPORT NOW", type="primary"):
        ok, fail = 0, 0
        for _, row in df.iterrows():
            try:
                full_name = str(row.get(col_name, "")).strip()
                if not full_name:
                    raise ValueError("Blank name")

                age = int(row.get(col_age, 0) or 0) if col_age else 0
                sex = str(row.get(col_sex, "Other") or "Other").strip() if col_sex else "Other"
                notes = str(row.get(col_notes, "") or "").strip() if col_notes else ""

                mtb_detected = parse_bool_detected(row.get(col_mtb))
                rif_detected = parse_bool_detected(row.get(col_rif))

                q = df_select("patients", {"select": "patient_id,full_name", "full_name": f"eq.{full_name}", "limit": "1"})
                if not q.empty:
                    pid = str(q.iloc[0]["patient_id"])
                else:
                    outp = insert_row("patients", {
                        "facility_id": facility_id,
                        "full_name": full_name,
                        "age": int(age),
                        "sex": sex,
                        "phone": "",
                        "address": "",
                        "created_at": now_iso(),
                    })
                    pid = str(outp.get("patient_id"))

                category = "CONFIRMED TB" if mtb_detected else "LOW"
                genx = "Positive" if mtb_detected else "Negative"
                insert_row("events", {
                    "facility_id": facility_id,
                    "patient_id": pid,
                    "tb_probability": 0.95 if mtb_detected else 0.10,
                    "category": category,
                    "genexpert": genx,
                    "smear": "Not done",
                    "cxr": "Not done",
                    "notes": f"GeneXpert Import | MTB={row.get(col_mtb)} | RIF={row.get(col_rif)} | {notes}".strip(),
                    "timestamp": now_iso(),
                })

                if rif_detected:
                    insert_row("tb_drug_resistance", {
                        "facility_id": facility_id,
                        "patient_id": pid,
                        "rifampicin_resistant": True,
                        "isoniazid_resistant": False,
                        "fluoroquinolone_resistant": False,
                        "bedaquiline_resistant": False,
                        "linezolid_resistant": False,
                        "resistance_class": "RR-TB",
                        "test_method": "GeneXpert",
                        "notes": "Auto-created from GeneXpert import",
                        "created_at": now_iso(),
                    })

                ok += 1
            except Exception:
                fail += 1

        st.success(f"Import complete ✅ Success={ok} Failed={fail}")
        st.rerun()

def page_who_dashboard():
    render_topbar()
    section("WHO Dashboard")
    st.caption("Uses v_who_indicators_monthly view. Organizer sees national; others see facility only.")

    dfw = df_select("v_who_indicators_monthly", {"select": "*", "limit": "50000"})
    if dfw.empty:
        st.info("No data yet. Add diagnosis events or import GeneXpert.")
        return

    if "facility_id" not in dfw.columns:
        st.error(f"v_who_indicators_monthly returned no facility_id. Columns seen: {list(dfw.columns)}")
        st.stop()

    is_org = (st.session_state.get("role") == "organizer")
    if not is_org:
        dfw = dfw[dfw["facility_id"].astype(str) == str(facility_id)]

    dfw["month"] = pd.to_datetime(dfw["month"], errors="coerce")
    latest_month = dfw["month"].max()
    cur = dfw[dfw["month"] == latest_month].copy()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Month", latest_month.strftime("%Y-%m") if pd.notna(latest_month) else "N/A")
    c2.metric("Presumptives", int(cur.get("presumptive_total", pd.Series([0])).sum()))
    c3.metric("Confirmed TB", int(cur.get("confirmed_tb", pd.Series([0])).sum()))
    c4.metric("GeneXpert +", int(cur.get("genexpert_positive", pd.Series([0])).sum()))

    st.subheader("Monthly trend")
    trend_cols = [c for c in ["presumptive_total", "confirmed_tb", "genexpert_positive"] if c in dfw.columns]
    trend = dfw.groupby("month")[trend_cols].sum().reset_index()
    st.line_chart(trend.set_index("month"))

def page_gis_heatmap():
    render_topbar()
    section("GIS Heatmap (Nigeria)")
    st.caption("Uses v_outbreak_facility view. Add latitude/longitude to facilities for mapping.")

    if px is None:
        st.error("Plotly not installed. Add 'plotly' to requirements.txt then: pip install -r requirements.txt")
        return

    dfm = df_select("v_outbreak_facility", {"select": "*", "limit": "50000"})
    if dfm.empty:
        st.info("No facilities/events yet.")
        return

    is_org = (st.session_state.get("role") == "organizer")
    if (not is_org) and ("facility_id" in dfm.columns):
        dfm = dfm[dfm["facility_id"].astype(str) == str(facility_id)]

    dfm["latitude"] = pd.to_numeric(dfm.get("latitude"), errors="coerce")
    dfm["longitude"] = pd.to_numeric(dfm.get("longitude"), errors="coerce")
    dfm["confirmed_tb"] = pd.to_numeric(dfm.get("confirmed_tb"), errors="coerce").fillna(0).astype(int)

    states = sorted([str(x) for x in dfm.get("state", pd.Series([])).dropna().unique().tolist() if str(x).strip()])
    col1, col2, col3 = st.columns(3)
    with col1:
        state = st.selectbox("State", ["All"] + states)

    if state != "All" and "state" in dfm.columns:
        lgas = sorted([str(x) for x in dfm[dfm["state"] == state].get("lga", pd.Series([])).dropna().unique().tolist() if str(x).strip()])
    else:
        lgas = sorted([str(x) for x in dfm.get("lga", pd.Series([])).dropna().unique().tolist() if str(x).strip()])

    with col2:
        lga = st.selectbox("LGA", ["All"] + lgas)
    with col3:
        min_cases = st.number_input("Min confirmed TB", 0, 100000, 0)

    dff = dfm.copy()
    if state != "All" and "state" in dff.columns:
        dff = dff[dff["state"] == state]
    if lga != "All" and "lga" in dff.columns:
        dff = dff[dff["lga"] == lga]
    dff = dff[dff["confirmed_tb"] >= int(min_cases)]
    dff = dff.dropna(subset=["latitude", "longitude"])

    if dff.empty:
        st.warning("No facilities with coordinates match your filters.")
        st.markdown("Quick fix SQL (paste in Supabase SQL Editor):")
        st.code(f"""
update public.facilities
set state='Rivers', lga='Port Harcourt', latitude=4.8156, longitude=7.0498
where facility_id='{facility_id}';
""")
        return

    fig = px.density_mapbox(
        dff,
        lat="latitude",
        lon="longitude",
        z="confirmed_tb",
        radius=25,
        zoom=4.2,
        height=560,
        hover_name="facility_name" if "facility_name" in dff.columns else None,
        hover_data={c: True for c in ["state", "lga", "confirmed_tb", "total_events", "last_event_ts"] if c in dff.columns},
    )
    fig.update_layout(mapbox_style="open-street-map", margin={"l": 0, "r": 0, "t": 0, "b": 0})
    st.plotly_chart(fig, use_container_width=True)

def page_outbreak_alerts():
    render_topbar()
    section("Outbreak Alerts")
    st.caption("Uses v_hotspots view + tb_outbreak_alerts table (7d vs previous 28d).")

    try:
        dfh = df_select("v_hotspots", {"select": "*", "limit": "50000"})
    except Exception as e:
        st.error("Could not load v_hotspots. Ensure you ran the Outbreak Alerts SQL block.")
        st.exception(e)
        return

    if dfh.empty:
        st.info("No data yet.")
        return

    if st.session_state.get("role") != "organizer" and "facility_id" in dfh.columns:
        dfh = dfh[dfh["facility_id"].astype(str) == str(facility_id)]

    show_cols = [c for c in ["facility_name", "state", "lga", "confirmed_7d", "confirmed_prev_28d", "ratio", "hotspot_level"] if c in dfh.columns]
    st.subheader("Hotspot ranking (last 7 days)")
    st.dataframe(dfh[show_cols].sort_values("confirmed_7d", ascending=False), use_container_width=True, hide_index=True)

def page_exports():
    render_topbar()
    section("Exports")
    tables = ["patients", "events", "dots_daily", "adherence", "tb_treatment", "tb_contacts", "tb_drug_resistance", "tb_outbreak_alerts"]
    cols = st.columns(4)
    for i, t in enumerate(tables):
        try:
            df = df_select(t, {"select": "*", "limit": "50000"})
            cols[i % 4].download_button(f"{t}.csv", df.to_csv(index=False).encode("utf-8"), f"{t}.csv", "text/csv")
        except Exception:
            cols[i % 4].caption(f"{t} not ready")

def page_national_view():
    render_topbar()
    section("National View (Organizer)")
    if st.session_state.get("role") != "organizer":
        st.warning("Organizer only.")
        return

    df_fac = df_select("facilities", {"select": "*", "limit": "50000"})
    df_evt = df_select("events", {"select": "*", "limit": "50000"})
    st.subheader("Facilities")
    st.dataframe(df_fac, use_container_width=True, hide_index=True)
    st.subheader("Events")
    st.dataframe(df_evt, use_container_width=True, hide_index=True)

# =========================
# MENU + ROUTER
# =========================
menu = [
    f"{TB_ICON} Home",
    f"{TB_ICON} Patients",
    f"{TB_ICON} Diagnosis Events",
    f"{TB_ICON} DOTS",
    f"{TB_ICON} Adherence",
    f"{TB_ICON} Treatment",
    f"{TB_ICON} Contact Tracing",
    f"{TB_ICON} Drug Resistance",
    f"{TB_ICON} GeneXpert Import",
    f"{TB_ICON} WHO Dashboard",
    f"{TB_ICON} GIS Heatmap",
    f"{TB_ICON} Outbreak Alerts",
    f"{TB_ICON} Exports",
]
if st.session_state.get("role") == "organizer":
    menu.append(f"{TB_ICON} National View")

page = st.sidebar.radio("Menu", menu)

if page.endswith("Home"):
    page_home()
elif page.endswith("Patients"):
    page_patients()
elif page.endswith("Diagnosis Events"):
    page_diagnosis_events()
elif page.endswith("DOTS"):
    page_dots()
elif page.endswith("Adherence"):
    page_adherence()
elif page.endswith("Treatment"):
    page_treatment()
elif page.endswith("Contact Tracing"):
    page_contact_tracing()
elif page.endswith("Drug Resistance"):
    page_drug_resistance()
elif page.endswith("GeneXpert Import"):
    page_genexpert_import()
elif page.endswith("WHO Dashboard"):
    page_who_dashboard()
elif page.endswith("GIS Heatmap"):
    page_gis_heatmap()
elif page.endswith("Outbreak Alerts"):
    page_outbreak_alerts()
elif page.endswith("Exports"):
    page_exports()
elif page.endswith("National View"):
    page_national_view()
else:
    page_home()
