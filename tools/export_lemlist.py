"""
Tool: Lemlist-Ready CSV Export

Formats firm + contact + growth data into a CSV that can be directly
imported into a Lemlist campaign.

Columns: First Name, Last Name, Company, Title, Email, Website, AUM,
         Growth Rate, State, LinkedIn URL, Tier, Needs Manual Enrichment
"""

import io
from datetime import datetime

import pandas as pd

from tools.cache_db import (
    init_db, get_firms, get_all_contacts_with_firms, get_growth_scores,
    log_export,
)


def _format_aum(val):
    """Format AUM as a readable dollar amount."""
    if val is None or pd.isna(val):
        return ""
    val = float(val)
    if val >= 1_000_000_000:
        return f"${val / 1_000_000_000:.1f}B"
    elif val >= 1_000_000:
        return f"${val / 1_000_000:.0f}M"
    elif val >= 1_000:
        return f"${val / 1_000:.0f}K"
    elif val > 0:
        return f"${val:,.0f}"
    return ""


def build_lemlist_dataframe(db_path=None, tier=None, state=None,
                            min_growth_rate=None):
    """Build a DataFrame formatted for Lemlist import.

    Joins firm data, contacts, and growth scores into a single flat table.

    Args:
        db_path: Database path.
        tier: Optional tier filter ('Hot', 'Warm', 'Cool', 'Cold').
        state: Optional state filter.
        min_growth_rate: Optional minimum YoY growth rate filter.

    Returns pandas DataFrame with Lemlist-compatible columns.
    """
    init_db(db_path)

    # Get contacts joined with firms
    contacts = get_all_contacts_with_firms(db_path=db_path)
    if not contacts:
        return pd.DataFrame()

    df = pd.DataFrame(contacts)

    # Get growth scores
    scores = get_growth_scores(db_path=db_path)
    if scores:
        scores_df = pd.DataFrame(scores)
        score_cols = ['crd', 'yoy_growth_latest', 'yoy_growth_avg',
                      'composite_score', 'tier']
        available_cols = [c for c in score_cols if c in scores_df.columns]
        df = df.merge(
            scores_df[available_cols],
            on='crd', how='left', suffixes=('', '_score'),
        )

    # Apply filters
    if tier and 'tier' in df.columns:
        df = df[df['tier'] == tier]
    if state and 'state' in df.columns:
        df = df[df['state'] == state]
    if min_growth_rate is not None and 'yoy_growth_latest' in df.columns:
        df = df[df['yoy_growth_latest'] >= min_growth_rate]

    # Flag rows needing manual enrichment
    df['Needs Manual Enrichment'] = df['contact_email'].isna() | (df['contact_email'] == '')

    # Build Lemlist output columns
    output = pd.DataFrame({
        'First Name': df.get('first_name', ''),
        'Last Name': df.get('last_name', ''),
        'Company': df.get('company', ''),
        'Title': df.get('contact_title', ''),
        'Email': df.get('contact_email', ''),
        'Phone': df.get('contact_phone', ''),
        'Website': df.get('website', ''),
        'AUM': df.get('aum', 0).apply(_format_aum) if 'aum' in df.columns else '',
        'AUM Raw': df.get('aum', ''),
        'Growth Rate (%)': df.get('yoy_growth_latest', ''),
        'State': df.get('state', ''),
        'Tier': df.get('tier', ''),
        'Composite Score': df.get('composite_score', ''),
        'LinkedIn URL': df.get('contact_linkedin', ''),
        'CRD': df.get('crd', ''),
        'Source': df.get('source', ''),
        'Needs Manual Enrichment': df['Needs Manual Enrichment'],
    })

    return output


def export_lemlist_csv(db_path=None, tier=None, state=None,
                       min_growth_rate=None):
    """Export a Lemlist-ready CSV string.

    Args:
        db_path: Database path.
        tier: Optional tier filter.
        state: Optional state filter.
        min_growth_rate: Optional minimum YoY growth rate.

    Returns tuple: (csv_string, record_count, filename)
    """
    df = build_lemlist_dataframe(
        db_path=db_path, tier=tier, state=state,
        min_growth_rate=min_growth_rate,
    )

    if df.empty:
        return '', 0, ''

    # Drop the raw AUM column (keep formatted)
    export_df = df.drop(columns=['AUM Raw'], errors='ignore')

    csv_buffer = io.StringIO()
    export_df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filters = []
    if tier:
        filters.append(f"tier={tier}")
    if state:
        filters.append(f"state={state}")
    if min_growth_rate is not None:
        filters.append(f"min_growth={min_growth_rate}%")
    filter_str = ', '.join(filters) if filters else 'none'

    filename = f"ria_growth_lemlist_{timestamp}.csv"

    log_export(filename, len(export_df), filter_str, db_path=db_path)

    return csv_data, len(export_df), filename


if __name__ == "__main__":
    csv_data, count, filename = export_lemlist_csv()
    if count > 0:
        print(f"Exported {count} records to {filename}")
        with open(filename, 'w') as f:
            f.write(csv_data)
    else:
        print("No data to export")
