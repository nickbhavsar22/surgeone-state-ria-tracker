"""
Tool: SEC Bulk CSV Ingestion for State-Registered RIAs

Downloads and parses SEC FOIA investment adviser CSV data, filtering
to state-registered firms with AUM in the $70M–$105M target range.

Supports:
  - Current snapshot import (firms table)
  - Historical snapshot import (aum_history table for growth tracking)
  - Auto-download from SEC.gov archive
  - Local CSV/ZIP upload
"""

import io
import time
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from tools.cache_db import (
    init_db, upsert_firms, upsert_aum_history, log_import,
)

SEC_BASE_URL = (
    "https://www.sec.gov/files/investment/data/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers/"
)

SEC_ARCHIVE_URL = "https://www.sec.gov/foia/docs/form-adv-archive-data"

HEADERS = {
    'User-Agent': 'Bhavsar Growth Consulting research@bhavsar.com',
}

# Default AUM target range (in dollars)
DEFAULT_MIN_AUM = 70_000_000
DEFAULT_MAX_AUM = 105_000_000

# Column mapping: SEC CSV column name → our internal field name
COLUMN_MAP = {
    'Primary Business Name': 'company',
    'Organization CRD#': 'crd',
    'SEC Status Effective Date': 'status_date',
    'Latest ADV Filing Date': 'filing_date',
    'SEC Current Status': 'status',
    'Main Office City': 'city',
    'Main Office State': 'state',
    'Main Office Telephone Number': 'phone',
    'Website Address': 'website',
    'Legal Name': 'legal_name',
    '2A(1)': 'sec_registered',
    '2A(2)': 'era',
    '5A': 'employees',
    '5C(1)': 'clients',
    '5F(2)(a)': 'aum_discretionary',
    '5F(2)(b)': 'aum_nondiscretionary',
    '5F(2)(c)': 'aum',
}

# CCO columns (Item 1.J) — may or may not be present in CSV
CCO_COLUMN_MAP = {
    '1J Name': 'cco_name',
    '1J Other Titles': 'cco_title',
    '1J Telephone': 'cco_phone',
    '1J Facsimile': 'cco_fax',
    '1J E-mail': 'cco_email',
}


def _safe_int(val):
    """Convert a value to int, handling commas, whitespace, and blanks."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip().replace(',', '').replace('$', '')
    if not s:
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _safe_str(val):
    """Clean a string value."""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _build_candidate_urls():
    """Build candidate SEC FOIA ZIP URLs for the last 4 months."""
    today = date.today()
    candidates = []
    for months_back in range(0, 4):
        year = today.year
        month = today.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        for day in [1, 2]:
            try:
                d = date(year, month, day)
            except ValueError:
                continue
            stamp = d.strftime('%m%d%y')
            url = f"{SEC_BASE_URL}ia{stamp}.zip"
            candidates.append((url, d.strftime('%Y-%m-%d')))
    return candidates


def download_sec_csv(url=None):
    """Download and extract SEC FOIA CSV from a ZIP URL.

    If no URL provided, tries candidate URLs for the last 4 months.
    Returns (DataFrame, date_label) or (None, None).
    """
    if url:
        urls_to_try = [(url, 'manual')]
    else:
        urls_to_try = _build_candidate_urls()

    for candidate_url, label in urls_to_try:
        try:
            resp = requests.get(candidate_url, headers=HEADERS, timeout=120)
            if resp.status_code == 200:
                zf = zipfile.ZipFile(io.BytesIO(resp.content))
                csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
                if not csv_names:
                    continue
                with zf.open(csv_names[0]) as f:
                    df = pd.read_csv(f, encoding='latin-1', low_memory=False, dtype=str)
                return df, label
        except (requests.RequestException, zipfile.BadZipFile, Exception):
            continue
    return None, None


def load_local_csv(file_path):
    """Load a SEC FOIA CSV from a local file (CSV or ZIP).

    Returns a DataFrame or None.
    """
    path = str(file_path)
    try:
        if path.lower().endswith('.zip'):
            zf = zipfile.ZipFile(path)
            csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
            if not csv_names:
                return None
            with zf.open(csv_names[0]) as f:
                return pd.read_csv(f, encoding='latin-1', low_memory=False, dtype=str)
        else:
            return pd.read_csv(path, encoding='latin-1', low_memory=False, dtype=str)
    except Exception:
        return None


def parse_sec_dataframe(df):
    """Parse raw SEC CSV into cleaned records.

    Returns a list of dicts with standardized field names.
    """
    available = [c for c in COLUMN_MAP.keys() if c in df.columns]
    result = df[available].copy()
    result = result.rename(columns=COLUMN_MAP)

    # Clean numeric columns
    for col in ['employees', 'clients', 'aum', 'aum_discretionary', 'aum_nondiscretionary']:
        if col in result.columns:
            result[col] = result[col].apply(_safe_int)

    # Clean CRD
    result['crd'] = result['crd'].apply(_safe_int)
    result = result.dropna(subset=['crd'])
    result['crd'] = result['crd'].astype(int)

    # Clean string columns
    for col in ['company', 'legal_name', 'city', 'state', 'phone', 'website',
                'status', 'sec_registered', 'era']:
        if col in result.columns:
            result[col] = result[col].apply(_safe_str)

    return result.to_dict('records')


def parse_cco_columns(df):
    """Extract CCO info from CSV columns if present.

    Returns dict mapping CRD → {cco_name, cco_email, cco_phone}.
    """
    available_cco = [c for c in CCO_COLUMN_MAP.keys() if c in df.columns]
    if not available_cco:
        return {}

    cco_data = {}
    crd_col = 'Organization CRD#'
    if crd_col not in df.columns:
        return {}

    for _, row in df.iterrows():
        crd = _safe_int(row.get(crd_col))
        if not crd:
            continue
        cco = {}
        for csv_col, field in CCO_COLUMN_MAP.items():
            if csv_col in df.columns:
                cco[field] = _safe_str(row.get(csv_col))
        if cco.get('cco_name'):
            cco_data[crd] = cco

    return cco_data


def is_state_registered(record):
    """Check if a firm is state-registered (not SEC-registered)."""
    sec_reg = (record.get('sec_registered') or '').strip().upper()
    return sec_reg not in ('Y', 'YES', 'TRUE', '1')


def is_in_aum_range(record, min_aum=None, max_aum=None):
    """Check if a firm's AUM falls within the target range."""
    min_aum = min_aum or DEFAULT_MIN_AUM
    max_aum = max_aum or DEFAULT_MAX_AUM
    aum = record.get('aum')
    if aum is None:
        return False
    return min_aum <= aum <= max_aum


def filter_target_firms(records, min_aum=None, max_aum=None):
    """Filter records to state-registered firms in the target AUM range.

    Returns (target_firms, stats_dict).
    """
    state_registered = [r for r in records if is_state_registered(r)]
    target = [r for r in state_registered if is_in_aum_range(r, min_aum, max_aum)]
    return target, {
        'total': len(records),
        'state_registered': len(state_registered),
        'in_aum_range': len(target),
    }


def import_current_snapshot(df=None, csv_path=None, url=None,
                            min_aum=None, max_aum=None, db_path=None):
    """Import the latest SEC CSV as the current firm snapshot.

    Updates the firms table with state-registered firms in target AUM range.

    Args:
        df: Pre-loaded DataFrame (optional)
        csv_path: Local CSV/ZIP path (optional)
        url: SEC ZIP URL (optional)
        min_aum: Minimum AUM filter (default $70M)
        max_aum: Maximum AUM filter (default $105M)
        db_path: Database path (optional)

    Returns dict with import stats.
    """
    init_db(db_path)

    source_label = 'provided'
    if df is None:
        if csv_path:
            df = load_local_csv(csv_path)
            source_label = str(csv_path)
        else:
            df, source_label = download_sec_csv(url)

    if df is None:
        return {'error': 'Failed to load CSV data', 'firms_imported': 0}

    records = parse_sec_dataframe(df)
    target, stats = filter_target_firms(records, min_aum, max_aum)

    count = upsert_firms(target, db_path=db_path)

    log_import(
        source_file=source_label,
        snapshot_date=date.today().isoformat(),
        total_records=stats['total'],
        state_registered_count=stats['state_registered'],
        target_aum_count=stats['in_aum_range'],
        db_path=db_path,
    )

    return {
        'downloaded': stats['total'],
        'state_registered': stats['state_registered'],
        'firms_imported': count,
        'source': source_label,
    }


def import_historical_snapshot(df=None, csv_path=None, snapshot_date=None,
                               min_aum=None, max_aum=None, db_path=None):
    """Import a historical SEC CSV as an AUM snapshot for growth tracking.

    Stores AUM data in the aum_history table for all state-registered firms
    (not just those in the current target range, since we want to track
    firms that have grown INTO the range over time).

    Args:
        df: Pre-loaded DataFrame (optional)
        csv_path: Local CSV/ZIP path (optional)
        snapshot_date: Date label for this snapshot (YYYY-MM or YYYY)
        min_aum: Not used for history — we track ALL state-registered firms
        max_aum: Not used for history — we track ALL state-registered firms
        db_path: Database path

    Returns dict with import stats.
    """
    init_db(db_path)

    source_label = 'provided'
    if df is None:
        if csv_path:
            df = load_local_csv(csv_path)
            source_label = str(csv_path)
        else:
            return {'error': 'No data source provided', 'history_imported': 0}

    if df is None:
        return {'error': 'Failed to load CSV data', 'history_imported': 0}

    if not snapshot_date:
        snapshot_date = date.today().strftime('%Y-%m')

    records = parse_sec_dataframe(df)

    # For history, track all state-registered firms with any AUM > 0
    # (not just $70M-$105M) so we can see growth into the range
    state_registered = [r for r in records if is_state_registered(r)]
    with_aum = [r for r in state_registered if r.get('aum') and r['aum'] > 0]

    history_records = [{
        'crd': r['crd'],
        'snapshot_date': snapshot_date,
        'aum': r.get('aum'),
        'aum_discretionary': r.get('aum_discretionary'),
        'employees': r.get('employees'),
        'clients': r.get('clients'),
    } for r in with_aum]

    count = upsert_aum_history(history_records, db_path=db_path)

    log_import(
        source_file=source_label,
        snapshot_date=snapshot_date,
        total_records=len(records),
        state_registered_count=len(state_registered),
        target_aum_count=len(with_aum),
        db_path=db_path,
    )

    return {
        'total_records': len(records),
        'state_registered': len(state_registered),
        'history_imported': count,
        'snapshot_date': snapshot_date,
        'source': source_label,
    }


def probe_sec_urls(candidates=None):
    """Probe SEC FOIA URLs with HEAD requests to check availability.

    Returns list of dicts: [{url, date_label, available, size_mb}]
    """
    if candidates is None:
        candidates = _build_candidate_urls()

    results = []
    for url, date_label in candidates:
        try:
            resp = requests.head(url, headers=HEADERS, timeout=15, allow_redirects=True)
            available = resp.status_code == 200
            size_bytes = resp.headers.get('Content-Length')
            size_mb = round(int(size_bytes) / (1024 * 1024), 1) if size_bytes else None
            results.append({
                'url': url,
                'date_label': date_label,
                'available': available,
                'size_mb': size_mb,
            })
        except requests.RequestException:
            results.append({
                'url': url,
                'date_label': date_label,
                'available': False,
                'size_mb': None,
            })
        time.sleep(0.1)

    return results


if __name__ == "__main__":
    result = import_current_snapshot()
    print(f"Import result: {result}")
