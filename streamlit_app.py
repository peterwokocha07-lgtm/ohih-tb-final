import os
import json
import datetime as dt
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import requests
import streamlit as st

try:
    import plotly.express as px
except Exception:
    px = None

# ============================================================
# BRANDING (OHIH-TB) + NATIONAL DASHBOARD LOOK
# ============================================================
TB_ICON = "🫁"
AI_ICON = "🧠"
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
.ohih-title{ font-size: 26px; font-weight: 800; letter-spacing: .2px; margin: 0; }
.ohih-sub{ margin-top: 4px; font-size: 13px; opacity: .92; }
.ohih-badges{ display:flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
.ohih-badge{
  display:inline-flex; align-items:center; gap:6px; padding: 6px 10px;
  border-radius: 999px; background: rgba(255,255,255,.16);
  border: 1px solid rgba(255,255,255,.22); font-size: 12px; font-weight: 600;
}
.ohih-kpis{ display:flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; }
.ohih-kpi{
  background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.20);
  border-radius: 14px; padding: 10px 12px; min-width: 160px;
}
.ohih-kpi .k{ font-size: 11px; opacity: .90; margin-bottom: 4px; }
.ohih-kpi .v{ font-size: 18px; font-weight: 800; }
.ohih-kpi .s{ font-size: 11px; opacity: .90; margin-top: 2px; }

/* --- Section header --- */
.ohih-section{
  padding: 10px 12px; border-radius: 14px;
  border: 1px solid rgba(2,6,23,.10); background: rgba(255,255,255,.70);
  box-shadow: 0 8px 18px rgba(2,6,23,.06); margin: 8px 0 12px 0;
}
.ohih-section h2{ margin: 0; font-size: 20px; font-weight: 800; }

/* --- Alert cards --- */
.ohih-alert{
  border-radius: 16px; padding: 12px 14px;
  border: 1px solid rgba(2,6,23,.12);
  box-shadow: 0 10px 24px rgba(2,6,23,.08);
  margin: 10px 0 12px 0;
}
.ohih-alert h3{ margin:0; font-size:16px; font-weight:900; }
.ohih-alert p{ margin:6px 0 0 0; font-size:13px; opacity:.95; }
.ohih-alert ul{ margin:8px 0 0 16px; font-size:13px; }
.ohih-alert.low{ background: rgba(34,197,94,.10); }
.ohih-alert.watch{ background: rgba(245,158,11,.12); }
.ohih-alert.high{ background: rgba(239,68,68,.12); }
.ohih-alert.critical{ background: rgba(220,38,38,.16); border-color: rgba(220,38,38,.35); }

/* --- Subtle divider --- */
hr{ border: none; border-top: 1px solid rgba(2,6,23,.10); margin: 12px 0; }
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
# CONFIG / SECRETS (CLEAN)
# =========================
def safe_secret(name: str, default: str = "") -> str:
    try:
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets.get(name))
    except Exception:
        pass
    return os.getenv(name, default)


SUPABASE_URL = safe_secret("SUPABASE_URL", "").strip()
SUPABASE_ANON_KEY = safe_secret("SUPABASE_ANON_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    st.error("Missing SUPABASE_URL or SUPABASE_ANON_KEY in Streamlit Secrets.")
    st.stop()

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
    return requests.post(url, headers=h, json=payload, timeout=30)


def rest_patch(table: str, access_token: str, match_params: Dict[str, str], payload: Any) -> requests.Response:
    url = f"{REST_BASE}/{table}"
    h = rest_headers(access_token)
    h["Prefer"] = "return=representation"
    return requests.patch(url, headers=h, params=match_params, json=payload, timeout=30)


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
# AUTH (CLEAN + RELIABLE)
# =========================
def auth_sign_in(email: str, password: str) -> Dict[str, Any]:
    url = f"{AUTH_BASE}/token"
    params = {"grant_type": "password"}
    payload = {"email": email, "password": password}
    h = {"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"}
    r = requests.post(url, headers=h, params=params, json=payload, timeout=30)
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
# KPI HELPERS (best-effort)
# =========================
def _who_latest_metrics() -> Dict[str, Any]:
    try:
        dfw = df_select("v_who_indicators_monthly", {"select": "*", "limit": "50000"})
        if dfw.empty or "month" not in dfw.columns:
            return {}
        if "facility_id" in dfw.columns and st.session_state.get("role") != "organizer":
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
    fac_name = st.session_state.get("facility_name") or "—"
    role = st.session_state.get("role") or "standard"

    k = _who_latest_metrics()
    month_str = "—"
    if k.get("month") is not None and pd.notna(k.get("month")):
        month_str = pd.to_datetime(k["month"]).strftime("%Y-%m")

    presumptive = k.get("presumptive", 0) if k else 0
    confirmed = k.get("confirmed", 0) if k else 0
    genx_pos = k.get("genx_pos", 0) if k else 0

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
        <span class="ohih-badge">{AI_ICON} AI: PREDICTION</span>
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
# AI OUTBREAK PREDICTION (WEIGHTED PRESUMPTIVES + DRIVERS)
# =========================
def _sigmoid(x: float) -> float:
    if x < -30:
        return 0.0
    if x > 30:
        return 1.0
    import math
    return 1.0 / (1.0 + math.exp(-x))


def _get_bool(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([False] * len(df))
    v = df[col]
    if v.dtype == bool:
        return v.fillna(False)
    s = v.astype(str).str.strip().str.lower()
    return s.isin(["true", "t", "1", "yes", "y"])


def _get_num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df))
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def _ai_driver_definitions() -> List[Tuple[str, str, int]]:
    return [
        ("sx_cough_2w", "Cough ≥ 2 weeks", 3),
        ("sx_hemoptysis", "Hemoptysis", 3),
        ("sx_fever", "Fever", 1),
        ("sx_night_sweats", "Night sweats", 1),
        ("sx_weight_loss", "Weight loss", 1),
        ("sx_chest_pain", "Chest pain / breathlessness", 1),
        ("rf_contact_tb", "Contact with TB case", 2),
        ("rf_prev_tb", "Previous TB treatment", 1),
        ("rf_diabetes", "Diabetes (risk)", 1),
        ("rf_malnutrition", "Malnutrition / underweight (risk)", 1),
        ("comorbid_hiv", "HIV positive", 2),
        ("comorbid_diabetes", "Diabetes (comorbidity)", 1),
        ("comorbid_malnutrition", "Malnutrition (comorbidity)", 1),
        ("comorbid_ckd", "Chronic kidney disease (CKD)", 1),
        ("comorbid_copd", "COPD / chronic lung disease", 1),
        ("comorbid_immunosuppressed", "Immunosuppressed", 1),
        ("comorbid_cancer", "Cancer", 1),
        ("comorbid_smoking", "Smoking", 1),
        ("comorbid_alcohol", "Alcohol use", 1),
    ]


def _presumptive_weight(screening_score: float, screening_band: str, category: str, tb_prob: float) -> float:
    s = float(screening_score or 0.0)
    b = (screening_band or "").upper()
    cat = (category or "").upper()
    p = float(tb_prob or 0.0)

    w = 0.15 + 0.05 * min(10.0, max(0.0, s))
    if "HIGH" in b:
        w += 0.15
    if cat == "HIGH":
        w += 0.10
    if p >= 0.60:
        w += 0.10
    return float(max(0.0, min(0.90, w)))


def _focus_events_for_drivers(df: pd.DataFrame, days_window: int = 30) -> pd.DataFrame:
    if df is None or df.empty or "timestamp_dt" not in df.columns:
        return pd.DataFrame()
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_window)
    df = df[df["timestamp_dt"] >= cutoff].copy()
    if df.empty:
        return df

    band = df.get("screening_band", pd.Series([""] * len(df))).astype(str).str.upper()
    cat = df.get("category", pd.Series([""] * len(df))).astype(str).str.upper()
    genx = df.get("genexpert", pd.Series([""] * len(df))).astype(str).str.lower()

    is_confirmed = (cat == "CONFIRMED TB") | (genx == "positive")
    is_high_presumptive = band.str.contains("HIGH") | (cat == "HIGH")
    return df[is_confirmed | is_high_presumptive].copy()


def _compute_driver_breakdown(dfe_fac: pd.DataFrame, days_window: int = 30) -> pd.DataFrame:
    """
    Returns a dataframe:
      driver, weight, count, pct, contribution (=count*weight)
    computed from last N days of confirmed/high-presumptive events.
    """
    if dfe_fac is None or dfe_fac.empty:
        return pd.DataFrame(columns=["driver", "weight", "count", "pct", "contribution"])

    focus = _focus_events_for_drivers(dfe_fac, days_window=days_window)
    if focus.empty:
        return pd.DataFrame(columns=["driver", "weight", "count", "pct", "contribution"])

    n = len(focus)
    rows = []
    for col, label, wt in _ai_driver_definitions():
        if col not in focus.columns:
            continue
        b = _get_bool(focus, col)
        cnt = int(b.sum())
        if cnt <= 0:
            continue
        pct = 100.0 * cnt / max(1, n)
        rows.append(
            {
                "driver": label,
                "weight": int(wt),
                "count": int(cnt),
                "pct": float(pct),
                "contribution": float(cnt * wt),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["driver", "weight", "count", "pct", "contribution"])

    df = df.sort_values("contribution", ascending=False).reset_index(drop=True)
    return df


def _compute_top_drivers_text(dfe_fac: pd.DataFrame, days_window: int = 30, top_n: int = 6) -> str:
    df = _compute_driver_breakdown(dfe_fac, days_window=days_window)
    if df.empty:
        return "Insufficient driver data"
    df = df.head(top_n).copy()
    return ", ".join([f"{r.driver} ({r.pct:.0f}%)" for r in df.itertuples(index=False)])


def _ai_predict_hotspots(days_back: int = 120) -> pd.DataFrame:
    """
    Predict next 7-day TB surge per facility using TB-SIGNAL time series:
      TB_SIGNAL = confirmed(1.0) + presumptive_weight(0.0–0.9)
    Output includes:
      predicted_next7d_signal, predicted_next7d_confirmed (approx),
      ai_risk_prob, ai_level, growth, top_drivers
    """
    try:
        dfe = df_select("events", {"select": "*", "limit": "50000"})
        if dfe.empty:
            return pd.DataFrame()

        if "facility_id" not in dfe.columns or "timestamp" not in dfe.columns:
            return pd.DataFrame()

        is_org = st.session_state.get("role") == "organizer"
        if not is_org:
            dfe = dfe[dfe["facility_id"].astype(str) == str(facility_id)]

        dfe["timestamp_dt"] = pd.to_datetime(dfe["timestamp"], errors="coerce", utc=True)
        dfe = dfe[dfe["timestamp_dt"].notna()]
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_back)
        dfe = dfe[dfe["timestamp_dt"] >= cutoff]
        if dfe.empty:
            return pd.DataFrame()

        cat = dfe.get("category", pd.Series([""] * len(dfe))).astype(str)
        genx = dfe.get("genexpert", pd.Series([""] * len(dfe))).astype(str)
        band = dfe.get("screening_band", pd.Series([""] * len(dfe))).astype(str)
        score = _get_num(dfe, "screening_score", 0.0)
        tbp = _get_num(dfe, "tb_probability", 0.0)

        is_conf = (cat.astype(str).str.upper() == "CONFIRMED TB") | (genx.astype(str).str.lower() == "positive")
        is_pres = (
            cat.astype(str).str.upper().isin(["HIGH", "MODERATE"])
            | band.astype(str).str.upper().str.contains("HIGH")
            | (tbp >= 0.50)
        )
        is_pres = is_pres & (~is_conf)

        pres_w = []
        for s, b, c, p in zip(score.tolist(), band.tolist(), cat.tolist(), tbp.tolist()):
            pres_w.append(_presumptive_weight(s, b, c, p))
        pres_w = pd.Series(pres_w)

        dfe["tb_signal"] = (is_conf.astype(float) * 1.0) + (is_pres.astype(float) * pres_w)

        dfe["week_start"] = dfe["timestamp_dt"].dt.to_period("W-MON").dt.start_time

        wk = dfe.groupby(["facility_id", "week_start"]).agg(tb_signal_week=("tb_signal", "sum")).reset_index()

        conf_only = dfe[is_conf].copy()
        if not conf_only.empty:
            conf_only["week_start"] = conf_only["timestamp_dt"].dt.to_period("W-MON").dt.start_time
            wk_conf = conf_only.groupby(["facility_id", "week_start"]).size().reset_index(name="confirmed_week")
            wk = wk.merge(wk_conf, on=["facility_id", "week_start"], how="left")
        wk["confirmed_week"] = pd.to_numeric(wk.get("confirmed_week"), errors="coerce").fillna(0).astype(int)

        try:
            dff = df_select("facilities", {"select": "facility_id,facility_name,state,lga,latitude,longitude", "limit": "50000"})
        except Exception:
            dff = pd.DataFrame()

        out_rows = []
        for fac, g in wk.groupby("facility_id"):
            g = g.sort_values("week_start")
            last_week_start = g["week_start"].max()
            start = last_week_start - pd.Timedelta(weeks=10)
            g = g[g["week_start"] >= start].copy()

            idx = pd.date_range(g["week_start"].min(), g["week_start"].max(), freq="W-MON")
            s_sig = g.set_index("week_start")["tb_signal_week"].reindex(idx, fill_value=0.0)
            s_conf = g.set_index("week_start")["confirmed_week"].reindex(idx, fill_value=0)

            last_sig = float(s_sig.iloc[-1]) if len(s_sig) >= 1 else 0.0
            prev_sig = float(s_sig.iloc[-2]) if len(s_sig) >= 2 else 0.0
            prev2_sig = float(s_sig.iloc[-3]) if len(s_sig) >= 3 else 0.0

            last_c = int(s_conf.iloc[-1]) if len(s_conf) >= 1 else 0
            prev_c = int(s_conf.iloc[-2]) if len(s_conf) >= 2 else 0

            base = 0.60 * last_sig + 0.30 * prev_sig + 0.10 * prev2_sig
            growth = (last_sig + 0.50) / (prev_sig + 0.50)
            growth = max(0.60, min(1.90, float(growth)))

            pred_signal = max(0, int(round(base * growth)))
            pred_confirmed = max(0, int(round(min(pred_signal, 0.65 * pred_signal + 0.35 * last_c))))

            import math
            score_ai = (pred_signal - 2.2) + 1.6 * math.log1p(max(0.0, growth - 1.0)) + 0.35 * (last_c - prev_c)
            prob = float(_sigmoid(score_ai))

            if prob >= 0.85 or pred_signal >= 7:
                level = "CRITICAL"
            elif prob >= 0.60 or pred_signal >= 4:
                level = "HIGH"
            elif prob >= 0.35 or pred_signal >= 2:
                level = "WATCH"
            else:
                level = "LOW"

            dfe_fac = dfe[dfe["facility_id"].astype(str) == str(fac)].copy()
            drivers_str = _compute_top_drivers_text(dfe_fac, days_window=30, top_n=6)

            out_rows.append(
                {
                    "facility_id": str(fac),
                    "predicted_next7d_signal": int(pred_signal),
                    "predicted_next7d_confirmed": int(pred_confirmed),
                    "ai_risk_prob": float(prob),
                    "ai_level": level,
                    "last_week_signal": float(last_sig),
                    "prev_week_signal": float(prev_sig),
                    "growth": float(growth),
                    "top_drivers": drivers_str,
                }
            )

        dfp = pd.DataFrame(out_rows)
        if dfp.empty:
            return dfp

        if not dff.empty and "facility_id" in dff.columns:
            dfp = dfp.merge(dff, on="facility_id", how="left")

        dfp["ai_risk_pct"] = (dfp["ai_risk_prob"] * 100.0).round(1)

        dfp = dfp.sort_values(
            ["ai_risk_prob", "predicted_next7d_signal", "predicted_next7d_confirmed"],
            ascending=False
        ).reset_index(drop=True)

        return dfp
    except Exception:
        return pd.DataFrame()


def _render_ai_banner_for_facility(dfp: pd.DataFrame):
    if dfp is None or dfp.empty:
        st.info(f"{AI_ICON} AI Prediction: Not enough data yet. Add more events (presumptive/confirmed) first.")
        return

    is_org = st.session_state.get("role") == "organizer"
    if is_org:
        row = dfp.iloc[0].to_dict()
        fac_label = row.get("facility_name") or row.get("facility_id")
        msg_title = f"{AI_ICON} AI Prediction: Next 7 days hotspot risk (Top facility: {fac_label})"
    else:
        df_me = dfp[dfp["facility_id"].astype(str) == str(facility_id)]
        if df_me.empty:
            st.info(f"{AI_ICON} AI Prediction: No trend yet for this facility.")
            return
        row = df_me.iloc[0].to_dict()
        msg_title = f"{AI_ICON} AI Prediction: Next 7 days hotspot risk for your facility"

    level = str(row.get("ai_level", "LOW")).upper()
    pred_sig = int(row.get("predicted_next7d_signal", 0))
    pred_conf = int(row.get("predicted_next7d_confirmed", 0))
    prob_pct = float(row.get("ai_risk_pct", 0.0))
    last_sig = float(row.get("last_week_signal", 0.0))
    prev_sig = float(row.get("prev_week_signal", 0.0))
    growth = float(row.get("growth", 1.0))
    drivers = str(row.get("top_drivers", "")).strip()

    css = "low"
    if level == "WATCH":
        css = "watch"
    elif level == "HIGH":
        css = "high"
    elif level == "CRITICAL":
        css = "critical"

    if level in ("HIGH", "CRITICAL"):
        actions = [
            "Increase rapid cough screening at triage/OPD (all adults)",
            "Prioritize GeneXpert for HIGH screening band and contacts",
            "Trigger contact tracing for confirmed cases immediately",
            "Strengthen IPC: masks + ventilation + separation of coughers",
        ]
    elif level == "WATCH":
        actions = [
            "Enhance entry screening; review presumptives daily",
            "Escalate to GeneXpert if symptoms persist/worsen",
            "Audit lab turnaround time and sputum collection quality",
        ]
    else:
        actions = [
            "Continue routine surveillance and patient education",
            "Re-screen persistent cough cases and high-risk contacts",
        ]

    st.markdown(
        f"""
<div class="ohih-alert {css}">
  <h3>{msg_title}</h3>
  <p><b>AI Level:</b> {level} &nbsp; | &nbsp; <b>Predicted TB signal (next 7d):</b> {pred_sig}
     &nbsp; | &nbsp; <b>Predicted confirmed (approx):</b> {pred_conf}
     &nbsp; | &nbsp; <b>Risk Probability:</b> {prob_pct:.1f}%</p>
  <p><b>Trend:</b> last week signal={last_sig:.1f}, previous={prev_sig:.1f}, growth≈{growth:.2f}x</p>
  <p><b>Why AI says risk is high (top drivers, last 30 days):</b> {drivers}</p>
  <p><b>Recommended actions:</b></p>
  <ul>
    {''.join([f'<li>{a}</li>' for a in actions])}
  </ul>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_driver_chart(dfe_all_events: pd.DataFrame, facility_id_selected: str, days_window: int = 30):
    if dfe_all_events is None or dfe_all_events.empty:
        st.info("No event data to compute drivers.")
        return

    df_fac = dfe_all_events[dfe_all_events["facility_id"].astype(str) == str(facility_id_selected)].copy()
    if df_fac.empty:
        st.info("No events for selected facility.")
        return

    df_fac["timestamp_dt"] = pd.to_datetime(df_fac.get("timestamp"), errors="coerce", utc=True)
    df_fac = df_fac[df_fac["timestamp_dt"].notna()]

    drv = _compute_driver_breakdown(df_fac, days_window=days_window)
    if drv.empty:
        st.info("Not enough driver data yet (need confirmed or high presumptive events).")
        return

    st.subheader("Top drivers (symptoms/risk factors) contributing to AI risk")
    st.caption(f"Computed from last {days_window} days of CONFIRMED or HIGH presumptive events. Contribution = count × weight.")
    top = drv.head(12).copy()

    if px is not None:
        fig = px.bar(top, x="contribution", y="driver", orientation="h", hover_data=["count", "pct", "weight"])
        fig.update_layout(height=420, margin={"l": 0, "r": 0, "t": 30, "b": 0})
        st.plotly_chart(fig, use_container_width=True)
    else:
        # fallback
        st.bar_chart(top.set_index("driver")["contribution"])

    st.dataframe(top[["driver", "count", "pct", "weight", "contribution"]], use_container_width=True, hide_index=True)


def _render_ai_map_overlay(dfp: pd.DataFrame):
    section("AI Map Overlay")
    st.caption("Predicted hotspot risk overlay (requires facilities.latitude and facilities.longitude).")

    if px is None:
        st.error("Plotly not installed. Add 'plotly' to requirements.txt then redeploy.")
        return

    if dfp is None or dfp.empty:
        st.info("No AI prediction rows yet.")
        return

    # ensure coords
    if "latitude" not in dfp.columns or "longitude" not in dfp.columns:
        st.warning("Facilities coordinates not found in merged AI table.")
        return

    dfm = dfp.copy()
    dfm["latitude"] = pd.to_numeric(dfm["latitude"], errors="coerce")
    dfm["longitude"] = pd.to_numeric(dfm["longitude"], errors="coerce")
    dfm = dfm.dropna(subset=["latitude", "longitude"])

    if dfm.empty:
        st.warning("No facilities have coordinates yet. Add latitude/longitude in facilities table.")
        st.markdown("Quick fix SQL example (edit values):")
        st.code(
            f"""
update public.facilities
set state='Rivers', lga='Port Harcourt', latitude=4.8156, longitude=7.0498
where facility_id='{facility_id}';
"""
        )
        return

    # Map overlay: size=predicted signal, color=risk %
    hover_cols = {}
    for c in ["facility_name", "state", "lga", "ai_level", "ai_risk_pct", "predicted_next7d_signal", "predicted_next7d_confirmed", "top_drivers"]:
        if c in dfm.columns:
            hover_cols[c] = True

    fig = px.scatter_mapbox(
        dfm,
        lat="latitude",
        lon="longitude",
        size="predicted_next7d_signal" if "predicted_next7d_signal" in dfm.columns else None,
        color="ai_risk_pct" if "ai_risk_pct" in dfm.columns else None,
        hover_name="facility_name" if "facility_name" in dfm.columns else "facility_id",
        hover_data=hover_cols,
        zoom=4.2,
        height=560,
    )
    fig.update_layout(mapbox_style="open-street-map", margin={"l": 0, "r": 0, "t": 0, "b": 0})
    st.plotly_chart(fig, use_container_width=True)


# =========================
# PAGES
# =========================
def page_home():
    render_topbar()
    section("Home")
    st.success("✅ Authenticated. RLS isolates data per facility. WHO + GIS + Alerts + AI Prediction enabled.")
    st.write("Facility ID:", facility_id)
    st.write("Role:", st.session_state.get("role"))

    section("AI Outbreak Prediction")
    dfp = _ai_predict_hotspots(days_back=120)
    _render_ai_banner_for_facility(dfp)

    if dfp is not None and not dfp.empty:
        show_cols = [c for c in [
            "facility_name", "state", "lga",
            "predicted_next7d_signal", "predicted_next7d_confirmed",
            "ai_risk_pct", "ai_level",
            "last_week_signal", "prev_week_signal", "growth",
            "top_drivers"
        ] if c in dfp.columns]

        st.caption("Top predicted hotspot facilities (AI, next 7 days). TB signal = confirmed + weighted presumptives.")
        st.dataframe(dfp[show_cols].head(10), use_container_width=True, hide_index=True)

    # Map overlay on Home (nice!)
    _render_ai_map_overlay(dfp)


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

    dfp = safe_select_with_order(
        "patients",
        {"select": "*", "limit": "5000"},
        ["created_at.desc", "updated_at.desc", "patient_id.desc"],
    )
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

    st.markdown("### TB Screening Symptoms")
    col1, col2 = st.columns(2)
    with col1:
        sx_cough_2w = st.checkbox("Cough ≥ 2 weeks")
        sx_fever = st.checkbox("Fever")
        sx_night_sweats = st.checkbox("Night sweats")
        sx_weight_loss = st.checkbox("Weight loss")
        sx_hemoptysis = st.checkbox("Hemoptysis")
    with col2:
        sx_chest_pain = st.checkbox("Chest pain / breathlessness")
        rf_contact_tb = st.checkbox("Contact with TB case")
        rf_prev_tb = st.checkbox("Previous TB treatment")
        rf_diabetes = st.checkbox("Diabetes (risk)")
        rf_malnutrition = st.checkbox("Malnutrition / underweight (risk)")

    st.markdown("### Comorbidities / Risk Factors")
    col3, col4 = st.columns(2)
    with col3:
        comorbid_hiv = st.checkbox("HIV positive")
        comorbid_diabetes = st.checkbox("Diabetes (comorbidity)")
        comorbid_malnutrition = st.checkbox("Malnutrition")
        comorbid_smoking = st.checkbox("Smoking")
    with col4:
        comorbid_alcohol = st.checkbox("Alcohol use")
        comorbid_ckd = st.checkbox("Chronic kidney disease (CKD)")
        comorbid_copd = st.checkbox("COPD / chronic lung disease")
        comorbid_cancer = st.checkbox("Cancer")
        comorbid_immunosuppressed = st.checkbox("Immunosuppressed (steroids/transplant)")

    score = 0
    score += 3 if sx_cough_2w else 0
    score += 1 if sx_fever else 0
    score += 1 if sx_night_sweats else 0
    score += 1 if sx_weight_loss else 0
    score += 3 if sx_hemoptysis else 0
    score += 1 if sx_chest_pain else 0

    score += 2 if rf_contact_tb else 0
    score += 1 if rf_prev_tb else 0
    score += 1 if rf_diabetes else 0
    score += 1 if rf_malnutrition else 0

    score += 2 if comorbid_hiv else 0
    score += 1 if comorbid_diabetes else 0
    score += 1 if comorbid_malnutrition else 0
    score += 1 if comorbid_ckd else 0
    score += 1 if comorbid_copd else 0
    score += 1 if comorbid_immunosuppressed else 0
    score += 1 if comorbid_cancer else 0

    triad = int(sx_fever) + int(sx_night_sweats) + int(sx_weight_loss)
    presumptive = (
        sx_hemoptysis
        or sx_cough_2w
        or (triad >= 2)
        or (rf_contact_tb and (sx_fever or sx_night_sweats or sx_weight_loss or sx_chest_pain))
    )

    if presumptive or score >= 6:
        screening_band = "HIGH (Presumptive TB)"
        recommendation = (
            "Order GeneXpert ASAP; collect sputum sample. "
            "Apply infection prevention (mask/ventilation). "
            "Urgent clinician review if severe symptoms/hemoptysis. "
            "Notify TB focal person."
        )
    elif score >= 3:
        screening_band = "MODERATE"
        recommendation = (
            "Do smear microscopy and/or CXR if available; review within 48–72 hours. "
            "Escalate to GeneXpert if symptoms persist/worsen."
        )
    else:
        screening_band = "LOW"
        recommendation = (
            "Provide TB education. No urgent test required today. "
            "Re-screen if cough persists or new symptoms develop."
        )

    if genexpert == "Positive":
        screening_band = "CONFIRMED TB"
        recommendation = "Start TB care pathway per guideline; notify TB program; consider DST if indicated."

    st.info(
        f"**Screening score:** {int(score)}  |  **Risk band:** {screening_band}\n\n"
        f"**Next step:** {recommendation}"
    )

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
            "sx_cough_2w": sx_cough_2w,
            "sx_fever": sx_fever,
            "sx_night_sweats": sx_night_sweats,
            "sx_weight_loss": sx_weight_loss,
            "sx_hemoptysis": sx_hemoptysis,
            "sx_chest_pain": sx_chest_pain,
            "rf_contact_tb": rf_contact_tb,
            "rf_prev_tb": rf_prev_tb,
            "rf_diabetes": rf_diabetes,
            "rf_malnutrition": rf_malnutrition,
            "comorbid_hiv": comorbid_hiv,
            "comorbid_diabetes": comorbid_diabetes,
            "comorbid_malnutrition": comorbid_malnutrition,
            "comorbid_smoking": comorbid_smoking,
            "comorbid_alcohol": comorbid_alcohol,
            "comorbid_ckd": comorbid_ckd,
            "comorbid_copd": comorbid_copd,
            "comorbid_cancer": comorbid_cancer,
            "comorbid_immunosuppressed": comorbid_immunosuppressed,
            "screening_score": int(score),
            "screening_band": screening_band,
            "recommendation": recommendation,
        }

        insert_row("events", payload)
        st.success("Saved ✅")
        st.rerun()

    dfe = safe_select_with_order(
        "events",
        {"select": "*", "limit": "5000"},
        ["timestamp.desc", "created_at.desc", "event_id.desc"],
    )
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
                rp = rest_patch(
                    "dots_daily",
                    st.session_state["access_token"],
                    match,
                    {"dose_taken": bool(dose_taken), "note": note.strip()},
                )
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

    dfc = safe_select_with_order(
        "tb_contacts",
        {"select": "*", "index_patient_id": f"eq.{pid}", "limit": "5000"},
        ["created_at.desc", "updated_at.desc"],
    )

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

    dfr = safe_select_with_order(
        "tb_drug_resistance",
        {"select": "*", "patient_id": f"eq.{pid}", "limit": "5000"},
        ["created_at.desc", "updated_at.desc"],
    )
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
                    outp = insert_row(
                        "patients",
                        {
                            "facility_id": facility_id,
                            "full_name": full_name,
                            "age": int(age),
                            "sex": sex,
                            "phone": "",
                            "address": "",
                            "created_at": now_iso(),
                        },
                    )
                    pid = str(outp.get("patient_id"))

                category = "CONFIRMED TB" if mtb_detected else "LOW"
                genx = "Positive" if mtb_detected else "Negative"
                insert_row(
                    "events",
                    {
                        "facility_id": facility_id,
                        "patient_id": pid,
                        "tb_probability": 0.95 if mtb_detected else 0.10,
                        "category": category,
                        "genexpert": genx,
                        "smear": "Not done",
                        "cxr": "Not done",
                        "notes": f"GeneXpert Import | MTB={row.get(col_mtb)} | RIF={row.get(col_rif)} | {notes}".strip(),
                        "timestamp": now_iso(),
                        "screening_score": 0,
                        "screening_band": "CONFIRMED TB" if mtb_detected else "LOW",
                        "recommendation": "Imported from GeneXpert CSV",
                    },
                )

                if rif_detected:
                    insert_row(
                        "tb_drug_resistance",
                        {
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
                        },
                    )

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

    is_org = st.session_state.get("role") == "organizer"
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
        st.error("Plotly not installed. Add 'plotly' to requirements.txt then redeploy.")
        return

    dfm = df_select("v_outbreak_facility", {"select": "*", "limit": "50000"})
    if dfm.empty:
        st.info("No facilities/events yet.")
        return

    is_org = st.session_state.get("role") == "organizer"
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
        st.code(
            f"""
update public.facilities
set state='Rivers', lga='Port Harcourt', latitude=4.8156, longitude=7.0498
where facility_id='{facility_id}';
"""
        )
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


def page_ai_prediction():
    render_topbar()
    section("AI Prediction (Next 7 days)")
    st.caption("AI uses TB signal = confirmed + weighted presumptives (weighted by screening score). It also explains WHY risk is high.")

    dfp = _ai_predict_hotspots(days_back=120)
    _render_ai_banner_for_facility(dfp)

    if dfp is None or dfp.empty:
        st.warning("No AI prediction yet. Add more events (HIGH/MODERATE with screening_score, and confirmed).")
        return

    show_cols = [c for c in [
        "facility_name", "state", "lga",
        "predicted_next7d_signal", "predicted_next7d_confirmed",
        "ai_risk_pct", "ai_level", "growth",
        "top_drivers", "facility_id"
    ] if c in dfp.columns]

    st.subheader("Predicted hotspot ranking")
    st.dataframe(dfp[show_cols].head(50), use_container_width=True, hide_index=True)

    # ---- Driver chart for a selected facility ----
    section("Drivers Chart (Why AI is High)")
    if "facility_name" in dfp.columns:
        labels = (dfp["facility_id"].astype(str) + " — " + dfp["facility_name"].fillna("").astype(str)).tolist()
    else:
        labels = dfp["facility_id"].astype(str).tolist()

    default_label = None
    if st.session_state.get("role") != "organizer":
        # auto-select own facility
        for lab in labels:
            if lab.startswith(str(facility_id)):
                default_label = lab
                break

    chosen = st.selectbox("Select facility to view driver chart", labels, index=(labels.index(default_label) if default_label in labels else 0))
    fac_sel = chosen.split(" — ")[0].strip()

    # load events once for chart
    dfe_all = df_select("events", {"select": "*", "limit": "50000"})
    if st.session_state.get("role") != "organizer":
        dfe_all = dfe_all[dfe_all["facility_id"].astype(str) == str(facility_id)]
    _render_driver_chart(dfe_all, fac_sel, days_window=30)

    # ---- Map overlay in AI page ----
    _render_ai_map_overlay(dfp)


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
    f"{AI_ICON} AI Prediction",
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
elif page.endswith("AI Prediction"):
    page_ai_prediction()
elif page.endswith("Exports"):
    page_exports()
elif page.endswith("National View"):
    page_national_view()
else:
    page_home()
