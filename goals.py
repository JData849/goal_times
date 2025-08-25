# Streamlit app: Sporting Solutions – Goals with Confirmation (DRY & KISS)
# ---------------------------------------------------------------
# Usage (local):
#   1) pip install streamlit requests beautifulsoup4 pandas xlsxwriter python-dateutil
#   2) streamlit run streamlit_ss_goals_app.py
#   3) Paste your Cookie header when prompted in the sidebar (optionally save locally)
#
# Notes:
# - This app *does not* store your password. If you choose "Remember cookie locally",
#   it writes only the Cookie header to a local file .env_ss_cookie (like the original script).
# - Be respectful and only access where you have permission.

from __future__ import annotations

import io
import os
import json
import time
from datetime import datetime
from dateutil import tz
from dateutil.parser import parse as parse_dt

import requests
from bs4 import BeautifulSoup
import pandas as pd
import streamlit as st

# -------------------- CONFIG --------------------
BASE = "https://connect.sportingsolutions.com"
TOOLS_BASE = f"{BASE}/InPlayTraderTools/Support"
FIXTURE_FINDER = "http://supporttools.sportingsolutions.com/Fixture/FixtureFinder"  # legacy http per original
EVENTS_URL = f"{TOOLS_BASE}/Events/IpaEvents/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
)

CONFIRM_WINDOW_DEFAULT = 16  # search within next N event IDs
DOTENV_PATH = ".env_ss_cookie"  # local cookie stash

# -------------------- UI --------------------
st.set_page_config(page_title="Goal Times", page_icon="⚽", layout="wide")
st.title("⚽ Sporting Solutions — Goal Times")


with st.sidebar:
    st.header("Settings")

    # ---- Date & time pickers ----
    st.subheader("Window (local time)")
    from_date = st.date_input("From date", value=pd.to_datetime("2025-08-23").date())
    from_time = st.time_input("From time", value=pd.to_datetime("14:00").time(), step=60)
    to_date = st.date_input("To date", value=pd.to_datetime("2025-08-23").date())
    to_time = st.time_input("To time", value=pd.to_datetime("16:50").time(), step=60)

    # Show human display + internal conversion examples
    from_dt_obj = pd.to_datetime(pd.Timestamp.combine(from_date, from_time)).to_pydatetime()
    to_dt_obj = pd.to_datetime(pd.Timestamp.combine(to_date, to_time)).to_pydatetime()
    st.caption(
        f"Selected: **{from_dt_obj.strftime('%d/%m/%Y %H:%M')}** → internal **{from_dt_obj.strftime('%Y/%m/%d %H:%M:%S')}**\n"
        f"Selected: **{to_dt_obj.strftime('%d/%m/%Y %H:%M')}** → internal **{to_dt_obj.strftime('%Y/%m/%d %H:%M:%S')}**"
    )

    window = st.number_input(
        "Confirmation window (event ID span)",
        min_value=1,
        max_value=200,
        value=CONFIRM_WINDOW_DEFAULT,
        step=1
    )

    st.divider()
    st.subheader("Authentication")

    # ✅ Load cookie securely from Streamlit secrets
    cookie = st.secrets["auth"]["cookie"]

    # Optionally show a confirmation to user (but don’t reveal the cookie!)
    st.success("Cookie loaded from Streamlit secrets ✅")

    run_btn = st.button("Run")


# -------------------- HTTP helpers --------------------

def make_session(cookie_header: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cookie": cookie_header,
    })
    return s


def safe_get(s: requests.Session, url: str, **kwargs) -> requests.Response | None:
    try:
        r = s.get(url, timeout=30, **kwargs)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        st.error(f"GET failed: {url}\n→ {e}")
        return None


def safe_post(url: str, headers: dict, data: dict) -> requests.Response | None:
    try:
        r = requests.post(url, headers=headers, data=data, timeout=30, verify=False)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        st.error(f"POST failed: {url}\n→ {e}")
        return None

# -------------------- Parsers --------------------

def parse_fixture_table(html: bytes | str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = None
    for div in soup.find_all("div"):
        tbl = div.find("table", class_="table table-hover table-striped")
        if tbl:
            table = tbl
            break
    if table is None:
        return pd.DataFrame()

    headings = [th.get_text(strip=True) for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if tds and len(tds) == len(headings):
            rows.append(dict(zip(headings, tds)))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    mask_public = df.apply(lambda r: any("(public)" in str(v).lower() for v in r.to_numpy()), axis=1)
    df = df.loc[mask_public].reset_index(drop=True)
    return df


def parse_events_table(html_text: str) -> pd.DataFrame:
    soup = BeautifulSoup(html_text, "html.parser")
    tbody = soup.select_one("#eventsTable tbody")
    if not tbody:
        return pd.DataFrame(columns=[
            "Id","Type","Description","SourceTimeStampUtc","ReceivedTimeStampUtc","Lag (ms)","SumoLogic"
        ])
    rows = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue
        span = tds[2].find("span")
        description = span.get("title") if span and span.get("title") else tds[2].get_text(strip=True)
        rows.append({
            "Id": tds[0].get_text(strip=True),
            "Type": tds[1].get_text(strip=True),
            "Description": description,
            "SourceTimeStampUtc": tds[3].get_text(strip=True),
            "ReceivedTimeStampUtc": tds[4].get_text(strip=True),
            "Lag (ms)": tds[5].get_text(strip=True),
            "SumoLogic": tds[6].get_text(strip=True),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["Id"] = pd.to_numeric(df["Id"], errors="coerce")
    df["Lag (ms)"] = pd.to_numeric(df["Lag (ms)"], errors="coerce")
    fmt = "%Y/%m/%d %H:%M:%S.%f"
    df["SourceTimeStampUtc"] = pd.to_datetime(df["SourceTimeStampUtc"], errors="coerce", format=fmt)
    df["ReceivedTimeStampUtc"] = pd.to_datetime(df["ReceivedTimeStampUtc"], errors="coerce", format=fmt)
    return df.sort_values("Id").reset_index(drop=True)

# -------------------- Goal ↔ Confirm mapper --------------------

def map_goal_confirm(events: pd.DataFrame, window: int = CONFIRM_WINDOW_DEFAULT) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=[
            "Fixture Name","Competition Name","Which",
            "SourceTimeStampUtc","ConfirmTimeStampUtc","Latency_ms"
        ])

    df = events.copy()
    df["Type"] = df["Type"].fillna("").astype(str)
    df["Description"] = df["Description"].fillna("").astype(str)

    is_goal_home = df["Description"].str.contains(r"\bgoal\s+home\b", case=False, na=False)
    is_goal_away = df["Description"].str.contains(r"\bgoal\s+away\b", case=False, na=False)
    is_confirm = (
        df["Type"].str.strip().str.lower().eq("goalconfirmation")
        | df["Description"].str.contains(r"\bgoal\s+confirmation\b", case=False, na=False)
    )
    conf = df.loc[is_confirm]

    out = []
    for idx, g in df.loc[is_goal_home | is_goal_away].iterrows():
        gid = int(g["Id"]) if pd.notnull(g["Id"]) else None
        if gid is None:
            continue
        gtime = g["SourceTimeStampUtc"]
        which = "Goal Home" if is_goal_home.loc[idx] else "Goal Away"
        win = conf[(conf["Id"] > gid) & (conf["Id"] <= gid + window)]
        if not win.empty:
            c = win.iloc[0]
            ctime = c["SourceTimeStampUtc"]
            latency_ms = (ctime - gtime).total_seconds() * 1000 if pd.notnull(ctime) and pd.notnull(gtime) else None
            out.append({
                "Fixture Name": g.get("Fixture Name", ""),
                "Competition Name": g.get("Competition Name", ""),
                "Which": which,
                "SourceTimeStampUtc": gtime,
                "ConfirmTimeStampUtc": ctime,
                "Latency_ms": latency_ms,
            })

    return pd.DataFrame(out)

# -------------------- Core workflow --------------------

def fetch_fixtures(from_dt_local, to_dt_local) -> pd.DataFrame:    # Accept either strings or datetime objects
    try:
        if isinstance(from_dt_local, str):
            start = parse_dt(from_dt_local)
        else:
            start = from_dt_local
        if isinstance(to_dt_local, str):
            end = parse_dt(to_dt_local)
        else:
            end = to_dt_local
    except Exception:
        st.error("Could not parse your times.")
        return pd.DataFrame()

    to_uk = tz.gettz("Europe/London")
    start = start.astimezone(to_uk) if start.tzinfo else start.replace(tzinfo=to_uk)
    end = end.astimezone(to_uk) if end.tzinfo else end.replace(tzinfo=to_uk)

    payload = {
        "ceType": "FootballOdds",
        "FromDate": start.strftime("%d/%m/%Y"),
        "FromTime": start.strftime("%H:%M"),
        "ToDate": end.strftime("%d/%m/%Y"),
        "ToTime": end.strftime("%H:%M"),
    }

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "http://supporttools.sportingsolutions.com",
        "Referer": "http://supporttools.sportingsolutions.com/Fixture/FixtureFinder",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": USER_AGENT,
    }

    r = safe_post(FIXTURE_FINDER, headers=headers, data=payload)
    if r is None:
        return pd.DataFrame()

    df = parse_fixture_table(r.content)
    if df.empty:
        st.info("No fixtures found in that window (or the table layout changed).")
    return df


def fetch_events_for_fixture(s: requests.Session, fixture_id: str, group_id: str) -> pd.DataFrame:
    headers = {
        "Referer": f"{TOOLS_BASE}/Fixture/Details/{fixture_id}",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": USER_AGENT,
    }
    params = {"fixturegroupid": group_id, "sport": "football"}
    r = safe_get(s, EVENTS_URL, params=params, headers=headers)
    if r is None:
        return pd.DataFrame()
    return parse_events_table(r.text)


def fetch_group_id_from_details(s: requests.Session, fixture_id: str) -> str | None:
    url = f"{TOOLS_BASE}/Fixture/Details/{fixture_id}"
    r = safe_get(s, url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    if r is None:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    tab_content = soup.find('div', class_='tab-content') or soup.find('div', class_='tab-content clearfix')
    if not tab_content:
        return None
    target_td = None
    for td in tab_content.find_all('td'):
        if td.get_text(strip=True).lower() == 'fixture group id':
            target_td = td
            break
    if not target_td:
        return None
    sib = target_td.find_next_sibling('td')
    if not sib:
        return None
    val = sib.get_text(strip=True)
    return val or None

# -------------------- Outputs --------------------

def build_xlsx_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="yyyy-mm-dd hh:mm:ss.000") as writer:
        df.to_excel(writer, index=False, sheet_name="goals")
        ws = writer.sheets["goals"]
        for col_name in ("SourceTimeStampUtc","ConfirmTimeStampUtc"):
            if col_name in df.columns:
                col_idx = list(df.columns).index(col_name)
                ws.set_column(col_idx, col_idx, 23, writer.book.add_format({"num_format": "yyyy-mm-dd hh:mm:ss.000"}))
    return output.getvalue()

# -------------------- Run button logic --------------------

if run_btn:
    cookie = st.secrets["auth"]["cookie"].strip()
    if not cookie:
        st.error("Cookie is missing. Please add it in Streamlit secrets.")
        st.stop()

    session = make_session(cookie)
    fixtures = fetch_fixtures(from_dt_obj, to_dt_obj)

    with st.status("Fetching fixtures…", expanded=True) as status:
        fixtures = fetch_fixtures(from_dt_obj, to_dt_obj)
        if fixtures.empty:
            status.update(label="No fixtures found.", state="warning")
            st.stop()
        st.write(f"Found **{len(fixtures)}** public fixtures.")
        st.dataframe(fixtures, use_container_width=True)
        status.update(label="Fixtures fetched.", state="complete")

    # Identify key columns with fuzzy match
    def _col_like(df: pd.DataFrame, needle: str) -> str | None:
        lower = {c.lower(): c for c in df.columns}
        for k, v in lower.items():
            if needle in k:
                return v
        return None

    col_fixture_id = _col_like(fixtures, "fixture id")
    col_group_id = _col_like(fixtures, "fixture group id")
    col_fixture_name = _col_like(fixtures, "fixture name")
    col_comp_name = _col_like(fixtures, "competition")

    missing = [n for n, c in [
        ("Fixture Id / Ce Id", col_fixture_id),
        ("Fixture Name", col_fixture_name),
        ("Competition Name", col_comp_name),
    ] if c is None]

    if missing:
        st.error("Columns not found in fixture table: " + ", ".join(missing))
        st.stop()

    all_goals = []
    progress = st.progress(0)
    total = len(fixtures)
    st.write(f"Processing **{total}** fixtures…")

    for i, row in fixtures.iterrows():
        fixture_id = str(row[col_fixture_id])
        fix_name = str(row[col_fixture_name])
        comp_name = str(row[col_comp_name])

        # Use column value if present, else fetch from details page
        if col_group_id is not None and col_group_id in fixtures.columns:
            group_id = str(row[col_group_id])
        else:
            group_id = fetch_group_id_from_details(session, fixture_id)
            if not group_id:
                st.write(f"[{i+1}/{total}] {fix_name} — could not find Fixture Group Id; skipping.")
                progress.progress(int((i+1)/total*100))
                continue

        events = fetch_events_for_fixture(session, fixture_id, group_id)
        if events.empty:
            st.write(f"[{i+1}/{total}] {fix_name} — no events.")
            progress.progress(int((i+1)/total*100))
            continue

        events["Fixture Name"] = fix_name
        events["Competition Name"] = comp_name

        goals = map_goal_confirm(events, window=int(window))
        if goals.empty:
            st.write(f"[{i+1}/{total}] {fix_name} — events: {len(events)} | no goal confirmations matched.")
            progress.progress(int((i+1)/total*100))
            time.sleep(0.1)
            continue

        all_goals.append(goals)
        home = (goals["Which"] == "Goal Home").sum()
        away = (goals["Which"] == "Goal Away").sum()
        st.write(f"[{i+1}/{total}] {fix_name} — events: {len(events)} | goals H:{home} A:{away} total:{home+away}")
        progress.progress(int((i+1)/total*100))
        time.sleep(0.1)

    if not all_goals:
        st.warning("No matched goals across fixtures.")
        st.stop()

    final = pd.concat(all_goals, ignore_index=True)

    # Summary table
    summary = (
        final.groupby(["Fixture Name","Competition Name","Which"]).size().unstack(fill_value=0).reset_index()
    )
    st.subheader("Goal counts per fixture")
    st.dataframe(summary, use_container_width=True)

    st.subheader("Matched goals with confirmations")
    st.dataframe(final, use_container_width=True)

    # Downloads
    csv_bytes = final.to_csv(index=False).encode("utf-8")
    xlsx_bytes = build_xlsx_bytes(final)

    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            label="⬇️ Download CSV",
            data=csv_bytes,
            file_name="goals_with_confirmation.csv",
            mime="text/csv",
        )
    with c2:
        st.download_button(
            label="⬇️ Download Excel (.xlsx)",
            data=xlsx_bytes,
            file_name="goals_with_confirmation.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.success("Done.")
