"""
SurgeOne — State-Registered RIA Growth Tracker & Outreach Tool

Streamlit dashboard for discovering state-registered RIA firms approaching
the $110M SEC registration threshold and surfacing high-growth targets.

5 sections:
  1. Import Data — upload/download SEC bulk CSVs (current + historical)
  2. Growth Dashboard — filterable table with per-firm growth charts
  3. Hot List — ranked fastest-growing firms by composite score
  4. CCO & Contacts — contact panel with enrichment controls
  5. Export — one-click Lemlist CSV download
"""

import io
import json
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from tools.cache_db import (
    init_db, get_firms, get_firm_by_crd, get_pipeline_stats,
    get_contact_stats, get_enrichment_stats, get_all_contacts_with_firms,
    get_distinct_states, get_aum_history, get_growth_scores, get_hot_list,
    get_snapshot_dates, get_import_history, get_export_history,
    get_contacts_for_firm, get_form_adv,
)
from tools.ingest_bulk_csv import (
    import_current_snapshot, import_historical_snapshot,
    probe_sec_urls, load_local_csv,
)
from tools.growth_analysis import score_all_firms, score_firm, calculate_yoy_growth
from tools.extract_cco import extract_cco_batch
from tools.enrich_contacts import enrich_batch, HUNTER_API_KEY
from tools.export_lemlist import export_lemlist_csv, build_lemlist_dataframe

APP_VERSION = "1.0.0"
LOGO_PATH = Path(__file__).parent / "assets" / "logo.png"

# --- Page Config ---
st.set_page_config(
    page_title="SurgeOne — RIA Growth Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

def _inject_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    [data-testid="stMetric"] {
        background: #181820; border: 1px solid #2A2A35;
        border-radius: 12px; padding: 0.8rem 1rem;
        transition: border-color 0.2s ease, box-shadow 0.2s ease;
    }
    [data-testid="stMetric"]:hover {
        border-color: #7C5CFC; box-shadow: 0 0 12px rgba(124,92,252,0.12);
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-weight: 600; font-size: 1.4rem;
    }
    [data-testid="stMetric"] [data-testid="stMetricLabel"] {
        color: #8B8B9E; font-size: 0.78rem;
        text-transform: uppercase; letter-spacing: 0.04em;
    }

    button[kind="primary"] {
        background: #7C5CFC !important; border: none !important;
        border-radius: 8px !important; font-weight: 600 !important;
    }
    button[kind="primary"]:hover {
        background: #9B7FFF !important;
        box-shadow: 0 0 16px rgba(124,92,252,0.3) !important;
    }

    .stProgress > div > div > div {
        background: linear-gradient(90deg, #7C5CFC, #9B7FFF) !important;
        border-radius: 8px !important;
    }
    .stProgress > div > div {
        background: #2A2A35 !important; border-radius: 8px !important;
    }

    section[data-testid="stSidebar"] {
        background: #16161E !important;
        border-right: 1px solid #3A3A4A !important;
    }
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span,
    section[data-testid="stSidebar"] p {
        color: #E8E8F0 !important;
    }
    section[data-testid="stSidebar"] [data-testid="stMetric"] {
        background: #1E1E28 !important;
        border: 1px solid #3A3A4A !important;
    }
    section[data-testid="stSidebar"] [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
        font-weight: 600 !important;
    }
    section[data-testid="stSidebar"] [data-testid="stMetricLabel"] {
        color: #A0A0B8 !important;
    }
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stSlider label,
    section[data-testid="stSidebar"] .stNumberInput label {
        color: #C8C8DA !important;
        font-weight: 500 !important;
    }
    section[data-testid="stSidebar"] .stCaption {
        color: #9090A8 !important;
    }
    section[data-testid="stSidebar"] hr {
        border-color: #3A3A4A !important;
    }

    [data-testid="stExpander"] {
        background: #181820; border: 1px solid #2A2A35;
        border-radius: 12px; overflow: hidden;
    }

    [data-testid="stDataFrame"] {
        border: 1px solid #2A2A35 !important;
        border-radius: 12px !important; overflow: hidden;
    }

    h2 { margin-top: 0.5rem !important; font-weight: 600 !important; }
    h3 { font-weight: 600 !important; }
    hr { border-color: #2A2A35 !important; opacity: 0.6; }
    </style>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TIER_COLORS = {
    'Hot': '#FF4B4B',
    'Warm': '#FFA348',
    'Cool': '#4B9DFF',
    'Cold': '#6B7280',
}


def _format_aum(val):
    if val is None or pd.isna(val):
        return "—"
    val = float(val)
    if val >= 1_000_000_000:
        return f"${val / 1e9:.1f}B"
    elif val >= 1_000_000:
        return f"${val / 1e6:.0f}M"
    elif val >= 1_000:
        return f"${val / 1e3:.0f}K"
    elif val > 0:
        return f"${val:,.0f}"
    return "—"


def _tier_badge(tier):
    color = TIER_COLORS.get(tier, '#6B7280')
    return f'<span style="background:{color};color:white;padding:2px 10px;border-radius:12px;font-size:0.8rem;font-weight:600;">{tier}</span>'


# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------

def main():
    init_db()
    _inject_css()
    stats = get_pipeline_stats()

    # --- Sidebar ---
    with st.sidebar:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=200)
        else:
            st.title("SurgeOne")

        st.markdown("**RIA Growth Tracker**")
        st.caption(f"v{APP_VERSION} · Bhavsar Growth Consulting")
        st.divider()

        st.caption("Pipeline Stats")
        c1, c2 = st.columns(2)
        c1.metric("Firms", stats['total_firms'])
        c2.metric("Scored", stats['firms_scored'])
        c1.metric("Hot Firms", stats['hot_firms'])
        c2.metric("Snapshots", stats['aum_snapshots'])
        c1.metric("Contacts", stats['total_contacts'])
        c2.metric("With Email", stats['contacts_with_email'])

        st.divider()

        # Filters
        st.caption("Filters")
        states = get_distinct_states()
        selected_state = st.selectbox(
            "State", ["All"] + states, key="filter_state"
        )
        aum_range = st.slider(
            "AUM Range ($M)",
            min_value=50, max_value=120, value=(70, 105),
            key="filter_aum",
        )
        min_growth = st.number_input(
            "Min Growth Rate (%)",
            min_value=0.0, max_value=100.0, value=0.0, step=5.0,
            key="filter_growth",
        )

    # Store filters in session
    st.session_state['filters'] = {
        'state': None if selected_state == "All" else selected_state,
        'min_aum': aum_range[0] * 1_000_000,
        'max_aum': aum_range[1] * 1_000_000,
        'min_growth': min_growth,
    }

    # --- Tabs ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📥 Import Data",
        "📊 Growth Dashboard",
        "🔥 Hot List",
        "👤 CCO & Contacts",
        "📤 Export",
    ])

    with tab1:
        _section_import(stats)
    with tab2:
        _section_growth_dashboard()
    with tab3:
        _section_hot_list()
    with tab4:
        _section_contacts()
    with tab5:
        _section_export()


# ---------------------------------------------------------------------------
# Tab 1: Import Data
# ---------------------------------------------------------------------------

def _section_import(stats):
    st.header("Import SEC Data")

    if stats['total_firms'] > 0:
        st.success(
            f"**{stats['total_firms']}** state-registered firms imported "
            f"({stats['aum_snapshots']} historical snapshots)"
        )
    else:
        st.info(
            "Upload SEC investment adviser CSV data to get started. "
            "You'll need at least one current file and ideally 2-3 historical files for growth analysis."
        )

    col1, col2 = st.columns(2)

    # Current snapshot import
    with col1:
        st.subheader("Current Snapshot")
        st.caption("Imports firms into the main database. Filters to state-registered firms with $70M–$105M AUM.")
        uploaded = st.file_uploader(
            "Upload current SEC CSV or ZIP",
            type=["csv", "zip"],
            key="current_upload",
        )
        if uploaded is not None:
            if st.button("Import Current Snapshot", type="primary", key="btn_import_current"):
                with st.spinner(f"Parsing {uploaded.name}..."):
                    suffix = ".zip" if uploaded.name.endswith(".zip") else ".csv"
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                        tmp.write(uploaded.getvalue())
                        tmp_path = tmp.name
                    filters = st.session_state.get('filters', {})
                    result = import_current_snapshot(
                        csv_path=tmp_path,
                        min_aum=filters.get('min_aum'),
                        max_aum=filters.get('max_aum'),
                    )
                if result.get('error'):
                    st.error(f"Import failed: {result['error']}")
                else:
                    st.success(
                        f"Imported **{result['firms_imported']}** state-registered firms "
                        f"from {result['downloaded']:,} total records "
                        f"({result.get('state_registered', 0):,} state-registered)"
                    )
                    st.rerun()

    # Historical snapshot import
    with col2:
        st.subheader("Historical Snapshots")
        st.caption("Imports AUM history for growth analysis. Upload older SEC CSV files (one per year).")
        hist_uploaded = st.file_uploader(
            "Upload historical SEC CSV or ZIP",
            type=["csv", "zip"],
            key="hist_upload",
            accept_multiple_files=True,
        )
        if hist_uploaded:
            # Auto-detect snapshot dates from filenames
            def _extract_date_from_filename(name):
                """Extract a 4-digit year (optionally with month) from filename."""
                m = re.search(r'(20\d{2})[-_\s]?(0[1-9]|1[0-2])?', name)
                if m:
                    return f"{m.group(1)}-{m.group(2)}" if m.group(2) else m.group(1)
                return name.split('.')[0]

            file_dates = {}
            for f in hist_uploaded:
                file_dates[f.name] = _extract_date_from_filename(f.name)

            st.caption("Detected snapshot dates from filenames:")
            edited_dates = {}
            for f in hist_uploaded:
                edited_dates[f.name] = st.text_input(
                    f"Snapshot date for **{f.name}**",
                    value=file_dates[f.name],
                    key=f"snap_date_{f.name}",
                    help="Auto-detected from filename. Edit if incorrect.",
                )

            if st.button("Import Historical Snapshot(s)", type="primary", key="btn_import_hist"):
                for f in hist_uploaded:
                    s_date = edited_dates.get(f.name, file_dates[f.name])
                    with st.spinner(f"Importing {f.name} as snapshot {s_date}..."):
                        suffix = ".zip" if f.name.endswith(".zip") else ".csv"
                        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                            tmp.write(f.getvalue())
                            tmp_path = tmp.name
                        result = import_historical_snapshot(
                            csv_path=tmp_path,
                            snapshot_date=s_date,
                        )
                    if result.get('error'):
                        st.error(f"Import of {f.name} failed: {result['error']}")
                    else:
                        st.success(
                            f"**{f.name}**: {result['history_imported']:,} AUM records "
                            f"(snapshot: {result['snapshot_date']})"
                        )
                st.rerun()

    # Auto-download option
    with st.expander("Download from SEC.gov"):
        st.caption("SEC.gov may block automated downloads. If this fails, download manually.")
        if st.button("Check Available Files", key="btn_probe", type="secondary"):
            with st.spinner("Probing SEC.gov..."):
                results = probe_sec_urls()
            st.session_state['sec_probe_results'] = results

        if 'sec_probe_results' in st.session_state:
            available = [r for r in st.session_state['sec_probe_results'] if r['available']]
            if not available:
                st.warning("No files detected. Download manually instead.")
            else:
                options = {}
                for r in available:
                    size_str = f" — {r['size_mb']} MB" if r['size_mb'] else ""
                    label = f"{r['date_label']}{size_str}"
                    options[label] = r['url']
                selected = st.radio("Select file:", list(options.keys()), key="sec_select")
                if st.button("Download & Import", type="primary", key="btn_dl_import"):
                    url = options[selected]
                    with st.spinner(f"Downloading {selected}..."):
                        result = import_current_snapshot(url=url)
                    if result.get('error'):
                        st.error(f"Import failed: {result['error']}")
                    else:
                        st.success(f"Imported **{result['firms_imported']}** firms")
                        del st.session_state['sec_probe_results']
                        st.rerun()

    # Import history
    with st.expander("Import History"):
        history = get_import_history()
        if history:
            df = pd.DataFrame(history)
            st.dataframe(df, hide_index=True, use_container_width=True)
        else:
            st.caption("No imports yet.")


# ---------------------------------------------------------------------------
# Tab 2: Growth Dashboard
# ---------------------------------------------------------------------------

def _section_growth_dashboard():
    st.header("Growth Dashboard")

    filters = st.session_state.get('filters', {})
    firms = get_firms(
        state=filters.get('state'),
        min_aum=filters.get('min_aum'),
        max_aum=filters.get('max_aum'),
    )

    if not firms:
        st.info("No firms found. Import data first.")
        return

    # Score firms button
    snapshots = get_snapshot_dates()
    if len(snapshots) >= 2:
        if st.button("Score All Firms", type="primary", key="btn_score"):
            progress = st.progress(0, text="Scoring firms...")
            status = st.empty()

            def _on_progress(current, total, res):
                progress.progress(current / total, text=f"Scoring {current}/{total}...")
                status.text(
                    f"Scored: {res['scored']} | "
                    f"Hot: {res['by_tier'].get('Hot', 0)} | "
                    f"Warm: {res['by_tier'].get('Warm', 0)}"
                )

            result = score_all_firms(progress_callback=_on_progress)
            progress.progress(1.0, text="Scoring complete!")
            status.empty()
            st.success(
                f"Scored **{result['scored']}** firms — "
                f"Hot: {result['by_tier']['Hot']}, "
                f"Warm: {result['by_tier']['Warm']}, "
                f"Cool: {result['by_tier']['Cool']}, "
                f"Cold: {result['by_tier']['Cold']}"
            )
            st.rerun()
    else:
        st.warning(
            f"Need at least 2 historical snapshots for growth analysis. "
            f"Currently have {len(snapshots)}. Import more historical data."
        )

    # Firms table
    scores = get_growth_scores(
        tier=None,
        min_score=filters.get('min_growth'),
    )

    if scores:
        df = pd.DataFrame(scores)
        if filters.get('state') and 'state' in df.columns:
            df = df[df['state'] == filters['state']]

        display_df = df[['crd', 'company', 'state', 'aum', 'yoy_growth_latest',
                         'composite_score', 'tier']].copy()
        display_df['aum'] = display_df['aum'].apply(_format_aum)
        display_df['yoy_growth_latest'] = display_df['yoy_growth_latest'].apply(
            lambda x: f"{x:.1f}%" if pd.notna(x) else "—"
        )

        st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                'crd': st.column_config.NumberColumn('CRD', format='%d'),
                'company': 'Company',
                'state': 'State',
                'aum': 'AUM',
                'yoy_growth_latest': 'YoY Growth',
                'composite_score': st.column_config.ProgressColumn(
                    'Score', min_value=0, max_value=100, format='%d',
                ),
                'tier': 'Tier',
            },
        )

        # Per-firm growth chart
        st.subheader("Growth Chart")
        firm_options = {f"{r['company']} (CRD {r['crd']})": r['crd'] for r in scores}
        selected = st.selectbox("Select firm:", list(firm_options.keys()), key="chart_firm")

        if selected:
            crd = firm_options[selected]
            _render_growth_chart(crd)
    else:
        # Show raw firm data without scores
        df = pd.DataFrame(firms)
        display_cols = ['crd', 'company', 'state', 'aum', 'filing_date', 'website']
        available = [c for c in display_cols if c in df.columns]
        display_df = df[available].copy()
        if 'aum' in display_df.columns:
            display_df['aum'] = display_df['aum'].apply(_format_aum)
        st.dataframe(display_df, hide_index=True, use_container_width=True)


def _render_growth_chart(crd):
    """Render a Plotly growth chart for a single firm."""
    history = get_aum_history(crd)
    if not history or len(history) < 2:
        st.caption("Insufficient historical data for chart.")
        return

    dates = [h['snapshot_date'] for h in history]
    aums = [h['aum'] / 1_000_000 for h in history if h.get('aum')]

    if len(aums) != len(dates):
        st.caption("Missing AUM data in some snapshots.")
        return

    fig = go.Figure()

    # AUM line
    fig.add_trace(go.Scatter(
        x=dates, y=aums,
        mode='lines+markers',
        name='AUM ($M)',
        line=dict(color='#7C5CFC', width=3),
        marker=dict(size=8),
    ))

    # SEC threshold line
    fig.add_hline(
        y=110, line_dash="dash", line_color="#FF4B4B",
        annotation_text="$110M SEC Threshold",
        annotation_position="top right",
    )

    # Target range shading
    fig.add_hrect(
        y0=70, y1=105,
        fillcolor="#7C5CFC", opacity=0.08,
        line_width=0,
        annotation_text="Target Range",
        annotation_position="top left",
    )

    firm = get_firm_by_crd(crd)
    title = firm['company'] if firm else f"CRD {crd}"

    fig.update_layout(
        title=f"{title} — AUM Growth Trend",
        xaxis_title="Snapshot Date",
        yaxis_title="AUM ($M)",
        template="plotly_dark",
        height=400,
        margin=dict(t=50, b=50),
    )

    st.plotly_chart(fig, use_container_width=True)

    # Growth details
    growth_data = calculate_yoy_growth(crd)
    if growth_data['growth_rates']:
        cols = st.columns(4)
        cols[0].metric("Latest YoY", f"{growth_data['latest_growth']:.1f}%")
        cols[1].metric("Avg YoY", f"{growth_data['avg_growth']:.1f}%")
        cols[2].metric("Growth Years", growth_data['growth_years'])
        milestones = growth_data['milestones_crossed']
        cols[3].metric("Milestones", len(milestones))


# ---------------------------------------------------------------------------
# Tab 3: Hot List
# ---------------------------------------------------------------------------

def _section_hot_list():
    st.header("Hot List — Fastest Growing Firms")

    limit = st.selectbox("Show top:", [10, 25, 50, 100], index=1, key="hotlist_limit")
    hot_list = get_hot_list(limit=limit)

    if not hot_list:
        st.info("No scored firms yet. Import data, add historical snapshots, and run scoring.")
        return

    # Summary metrics
    tiers = {}
    for firm in hot_list:
        tier = firm.get('tier', 'Cold')
        tiers[tier] = tiers.get(tier, 0) + 1

    cols = st.columns(4)
    for i, (tier, count) in enumerate(sorted(tiers.items())):
        color = TIER_COLORS.get(tier, '#6B7280')
        cols[i % 4].metric(f"{tier} Firms", count)

    st.divider()

    # Ranked list
    for i, firm in enumerate(hot_list):
        tier = firm.get('tier', 'Cold')
        score = firm.get('composite_score', 0)
        growth = firm.get('yoy_growth_latest')
        aum = _format_aum(firm.get('aum'))
        growth_str = f"{growth:.1f}%" if growth is not None else "—"

        with st.expander(
            f"**#{i+1}** {firm['company']} — {aum} — {growth_str} YoY — "
            f"Score: {score}/100 [{tier}]"
        ):
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Composite Score", f"{score}/100")
            c2.metric("YoY Growth", growth_str)
            c3.metric("AUM", aum)
            c4.metric("State", firm.get('state', '—'))
            c5.metric("Employees", firm.get('employees', '—'))

            # Score breakdown
            details_str = firm.get('score_details')
            if details_str:
                try:
                    details = json.loads(details_str)
                    dims = details.get('dimensions', {})
                    dim_cols = st.columns(5)
                    for j, (dim_name, dim_data) in enumerate(dims.items()):
                        dim_cols[j].metric(
                            dim_name.replace('_', ' ').title(),
                            f"{dim_data['score']}/100",
                        )
                except (json.JSONDecodeError, KeyError):
                    pass

            # Growth chart button
            if st.button(f"View Growth Chart", key=f"chart_{firm['crd']}"):
                _render_growth_chart(firm['crd'])


# ---------------------------------------------------------------------------
# Tab 4: CCO & Contacts
# ---------------------------------------------------------------------------

def _section_contacts():
    st.header("CCO & Contacts")

    contact_stats = get_contact_stats()
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Contacts", contact_stats['total_contacts'])
    c2.metric("With Email", contact_stats['with_email'])
    c3.metric("Firms Processed", contact_stats['firms_processed'])

    st.divider()

    # Action buttons
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("CCO Extraction")
        st.caption("Extract CCO names from Form ADV PDFs for all firms.")
        firms = get_firms()
        crd_list = [f['crd'] for f in firms]

        if st.button("Run CCO Extraction", type="primary", key="btn_cco"):
            progress = st.progress(0, text="Extracting CCO data...")
            status = st.empty()

            def _on_cco_progress(current, total, res):
                progress.progress(current / total, text=f"Processing {current}/{total}...")
                status.text(
                    f"Processed: {res['processed']} | "
                    f"CSV: {res['csv_extracted']} | PDF: {res['pdf_extracted']} | "
                    f"Errors: {res['errors']}"
                )

            result = extract_cco_batch(crd_list, progress_callback=_on_cco_progress)
            progress.progress(1.0, text="CCO extraction complete!")
            status.empty()
            st.success(
                f"Found **{result['contacts_found']}** contacts "
                f"(CSV: {result['csv_extracted']}, PDF: {result['pdf_extracted']})"
            )
            st.rerun()

    with col2:
        st.subheader("Website Enrichment")
        st.caption("Scrape firm websites for additional contact emails and phone numbers.")
        if st.button("Run Website Scrape", type="primary", key="btn_enrich"):
            progress = st.progress(0, text="Scraping websites...")
            status = st.empty()

            def _on_enrich_progress(current, total, res):
                progress.progress(current / total, text=f"Enriching {current}/{total}...")
                status.text(
                    f"Enriched: {res['enriched']} | "
                    f"Emails: {res['emails_found']} | "
                    f"Unresolved: {res['unresolved_count']}"
                )

            result = enrich_batch(crd_list, progress_callback=_on_enrich_progress)
            progress.progress(1.0, text="Enrichment complete!")
            status.empty()
            st.success(
                f"Enriched **{result['enriched']}** firms, "
                f"found **{result['emails_found']}** emails "
                f"({result['unresolved_count']} need manual enrichment)"
            )
            st.rerun()

    st.divider()

    # Contact table
    st.subheader("All Contacts")
    contacts = get_all_contacts_with_firms()
    if contacts:
        df = pd.DataFrame(contacts)
        display_cols = [
            'crd', 'company', 'contact_name', 'contact_title',
            'contact_email', 'contact_phone', 'contact_linkedin',
            'state', 'aum', 'source',
        ]
        available = [c for c in display_cols if c in df.columns]
        display_df = df[available].copy()
        if 'aum' in display_df.columns:
            display_df['aum'] = display_df['aum'].apply(_format_aum)

        st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                'crd': st.column_config.NumberColumn('CRD', format='%d'),
                'company': 'Company',
                'contact_name': 'Name',
                'contact_title': 'Title',
                'contact_email': 'Email',
                'contact_phone': 'Phone',
                'contact_linkedin': st.column_config.LinkColumn('LinkedIn'),
                'state': 'State',
                'aum': 'AUM',
                'source': 'Source',
            },
        )
    else:
        st.info("No contacts yet. Run CCO extraction first.")

    # API usage
    with st.expander("API Usage"):
        api_stats = get_enrichment_stats()
        if api_stats:
            for s in api_stats:
                st.text(f"{s['api_source']}: {s['total_calls']} calls, {s['successes']} successes")
        else:
            st.text("No API calls yet")


# ---------------------------------------------------------------------------
# Tab 5: Export
# ---------------------------------------------------------------------------

def _section_export():
    st.header("Export for Lemlist")

    filters = st.session_state.get('filters', {})

    # Filter controls for export
    col1, col2, col3 = st.columns(3)
    with col1:
        export_tier = st.selectbox(
            "Tier filter",
            ["All", "Hot", "Warm", "Cool", "Cold"],
            key="export_tier",
        )
    with col2:
        states = get_distinct_states()
        export_state = st.selectbox(
            "State filter",
            ["All"] + states,
            key="export_state",
        )
    with col3:
        export_min_growth = st.number_input(
            "Min growth rate (%)",
            min_value=0.0, max_value=100.0, value=0.0, step=5.0,
            key="export_min_growth",
        )

    tier_val = None if export_tier == "All" else export_tier
    state_val = None if export_state == "All" else export_state
    growth_val = export_min_growth if export_min_growth > 0 else None

    # Preview
    preview_df = build_lemlist_dataframe(
        tier=tier_val, state=state_val, min_growth_rate=growth_val,
    )

    if not preview_df.empty:
        st.caption(f"**{len(preview_df)}** records match current filters")

        # Summary
        needs_manual = preview_df['Needs Manual Enrichment'].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Records", len(preview_df))
        c2.metric("With Email", len(preview_df) - needs_manual)
        c3.metric("Needs Manual", needs_manual)

        # Preview table
        with st.expander("Preview Export Data"):
            st.dataframe(preview_df.head(20), hide_index=True, use_container_width=True)

        # Download button
        csv_data, count, filename = export_lemlist_csv(
            tier=tier_val, state=state_val, min_growth_rate=growth_val,
        )

        if csv_data:
            st.download_button(
                label=f"Download Lemlist CSV ({count} records)",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                type="primary",
            )
    else:
        st.info("No data to export. Import firms and run scoring/enrichment first.")

    # Export history
    with st.expander("Export History"):
        history = get_export_history()
        if history:
            st.dataframe(pd.DataFrame(history), hide_index=True, use_container_width=True)
        else:
            st.caption("No exports yet.")


if __name__ == "__main__":
    main()
