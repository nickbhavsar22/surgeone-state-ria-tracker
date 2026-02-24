"""
Tool: AUM Growth Analysis & Scoring for State-Registered RIAs

Calculates year-over-year AUM growth rates from historical snapshots
and scores firms on their likelihood of crossing the $110M SEC threshold.

Scoring dimensions (5):
  1. YoY Growth Rate       (30%) — latest AUM growth percentage
  2. Proximity to $110M    (30%) — how close to SEC registration threshold
  3. Consistency           (15%) — years of sustained growth
  4. Acceleration          (15%) — is growth rate speeding up?
  5. Firmographic          (10%) — employee/client/state bonus signals

Tiers: Hot (≥75), Warm (≥50), Cool (≥25), Cold (<25)
"""

import json
import math
from datetime import datetime

from tools.cache_db import (
    get_connection, get_aum_history, get_firm_by_crd, get_firms,
    upsert_growth_score, init_db,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEC_THRESHOLD = 110_000_000  # $110M — SEC registration trigger

# Dimension weights
WEIGHTS = {
    'yoy_growth': 0.30,
    'proximity': 0.30,
    'consistency': 0.15,
    'acceleration': 0.15,
    'firmographic': 0.10,
}

# Tier thresholds
TIERS = {'hot': 75, 'warm': 50, 'cool': 25}

# Top financial states
TOP_FINANCIAL_STATES = {
    "NY", "CA", "TX", "FL", "CT", "MA", "IL", "NJ", "PA", "CO",
}

# Major AUM milestones for threshold crossing detection
AUM_MILESTONES = [50_000_000, 70_000_000, 100_000_000]


# ---------------------------------------------------------------------------
# Growth Calculation
# ---------------------------------------------------------------------------

def calculate_yoy_growth(crd, db_path=None):
    """Calculate year-over-year AUM growth rates for a firm.

    Returns dict:
        {
            'snapshots': [{date, aum}, ...],
            'growth_rates': [float, ...],  # YoY % changes
            'latest_growth': float or None,
            'avg_growth': float or None,
            'growth_years': int,  # consecutive years of positive growth
            'milestones_crossed': [int, ...],  # milestone thresholds crossed
        }
    """
    history = get_aum_history(crd, db_path=db_path)
    if not history:
        return {
            'snapshots': [], 'growth_rates': [], 'latest_growth': None,
            'avg_growth': None, 'growth_years': 0, 'milestones_crossed': [],
        }

    # Filter to snapshots with valid AUM
    snapshots = [
        {'date': h['snapshot_date'], 'aum': h['aum']}
        for h in history if h.get('aum') and h['aum'] > 0
    ]
    snapshots.sort(key=lambda s: s['date'])

    if len(snapshots) < 2:
        return {
            'snapshots': snapshots, 'growth_rates': [], 'latest_growth': None,
            'avg_growth': None, 'growth_years': 0, 'milestones_crossed': [],
        }

    # Calculate YoY growth rates
    growth_rates = []
    for i in range(1, len(snapshots)):
        prev_aum = snapshots[i - 1]['aum']
        curr_aum = snapshots[i]['aum']
        if prev_aum > 0:
            rate = ((curr_aum - prev_aum) / prev_aum) * 100
            growth_rates.append(round(rate, 2))

    # Count consecutive years of positive growth (from most recent backwards)
    growth_years = 0
    for rate in reversed(growth_rates):
        if rate > 0:
            growth_years += 1
        else:
            break

    # Detect milestone crossings
    milestones_crossed = []
    for milestone in AUM_MILESTONES:
        for i in range(1, len(snapshots)):
            if snapshots[i - 1]['aum'] < milestone <= snapshots[i]['aum']:
                milestones_crossed.append(milestone)
                break

    latest_growth = growth_rates[-1] if growth_rates else None
    avg_growth = round(sum(growth_rates) / len(growth_rates), 2) if growth_rates else None

    return {
        'snapshots': snapshots,
        'growth_rates': growth_rates,
        'latest_growth': latest_growth,
        'avg_growth': avg_growth,
        'growth_years': growth_years,
        'milestones_crossed': milestones_crossed,
    }


def detect_rapid_growth(growth_data):
    """Flag firms with rapid growth patterns.

    Rapid growth is defined as:
      - 20%+ YoY AUM increase in consecutive years
      - Crossing major thresholds ($50M→$70M→$100M) within compressed timeframe

    Returns dict: {is_rapid: bool, reasons: [str]}
    """
    reasons = []
    rates = growth_data.get('growth_rates', [])

    # Check for 20%+ consecutive years
    consecutive_high = 0
    for rate in rates:
        if rate >= 20:
            consecutive_high += 1
            if consecutive_high >= 2:
                reasons.append(
                    f"{consecutive_high} consecutive years of 20%+ growth"
                )
        else:
            consecutive_high = 0

    # Check for multiple milestone crossings
    milestones = growth_data.get('milestones_crossed', [])
    if len(milestones) >= 2:
        reasons.append(
            f"Crossed {len(milestones)} AUM milestones: "
            f"{', '.join(f'${m // 1_000_000}M' for m in milestones)}"
        )

    # Check for acceleration
    if len(rates) >= 2 and rates[-1] > rates[-2] and rates[-1] >= 15:
        reasons.append(
            f"Accelerating growth: {rates[-2]:.1f}% → {rates[-1]:.1f}%"
        )

    return {
        'is_rapid': len(reasons) > 0,
        'reasons': reasons,
    }


# ---------------------------------------------------------------------------
# Scoring Dimensions
# ---------------------------------------------------------------------------

def _score_yoy_growth(latest_growth):
    """Score dimension 1: YoY Growth Rate (0–100)."""
    if latest_growth is None:
        return 0
    if latest_growth >= 25:
        return 100
    elif latest_growth >= 20:
        return 90
    elif latest_growth >= 15:
        return 80
    elif latest_growth >= 10:
        return 60
    elif latest_growth >= 5:
        return 40
    elif latest_growth > 0:
        return 20
    return 0


def _score_proximity(aum):
    """Score dimension 2: Proximity to $110M threshold (0–100)."""
    if aum is None or aum <= 0:
        return 0
    if aum >= 105_000_000:
        return 100
    elif aum >= 100_000_000:
        return 95
    elif aum >= 95_000_000:
        return 85
    elif aum >= 90_000_000:
        return 75
    elif aum >= 85_000_000:
        return 65
    elif aum >= 80_000_000:
        return 50
    elif aum >= 75_000_000:
        return 35
    elif aum >= 70_000_000:
        return 25
    return 10


def _score_consistency(growth_years):
    """Score dimension 3: Years of consistent growth (0–100)."""
    if growth_years >= 4:
        return 100
    elif growth_years == 3:
        return 85
    elif growth_years == 2:
        return 70
    elif growth_years == 1:
        return 40
    return 0


def _score_acceleration(growth_rates):
    """Score dimension 4: Rate of acceleration (0–100).

    Is the growth rate increasing, stable, or decelerating?
    """
    if len(growth_rates) < 2:
        return 50  # neutral if insufficient data

    recent = growth_rates[-1]
    previous = growth_rates[-2]

    if recent <= 0:
        return 0

    diff = recent - previous
    if diff >= 10:
        return 100  # strongly accelerating
    elif diff >= 5:
        return 85
    elif diff >= 0:
        return 60  # stable or slightly accelerating
    elif diff >= -5:
        return 35  # slightly decelerating
    return 15  # strongly decelerating


def _score_firmographic(firm):
    """Score dimension 5: Firmographic signals (0–100)."""
    raw = 0

    employees = firm.get('employees') or 0
    if employees >= 10:
        raw += 30
    elif employees >= 5:
        raw += 20
    elif employees >= 2:
        raw += 10

    clients = firm.get('clients') or 0
    if clients >= 100:
        raw += 30
    elif clients >= 50:
        raw += 20
    elif clients >= 10:
        raw += 10

    state = (firm.get('state') or '').upper()
    if state in TOP_FINANCIAL_STATES:
        raw += 20

    if firm.get('website'):
        raw += 20

    return min(100, raw)


# ---------------------------------------------------------------------------
# Composite Scoring
# ---------------------------------------------------------------------------

def score_firm(crd, db_path=None):
    """Score a single firm across all 5 growth dimensions.

    Returns dict with composite score, tier, and per-dimension breakdown.
    """
    firm = get_firm_by_crd(crd, db_path=db_path)
    if not firm:
        return None

    growth_data = calculate_yoy_growth(crd, db_path=db_path)
    rapid = detect_rapid_growth(growth_data)

    # Calculate dimension scores
    d1 = _score_yoy_growth(growth_data['latest_growth'])
    d2 = _score_proximity(firm.get('aum'))
    d3 = _score_consistency(growth_data['growth_years'])
    d4 = _score_acceleration(growth_data['growth_rates'])
    d5 = _score_firmographic(firm)

    composite = round(
        d1 * WEIGHTS['yoy_growth'] +
        d2 * WEIGHTS['proximity'] +
        d3 * WEIGHTS['consistency'] +
        d4 * WEIGHTS['acceleration'] +
        d5 * WEIGHTS['firmographic']
    )
    composite = max(0, min(100, composite))

    if composite >= TIERS['hot']:
        tier = 'Hot'
    elif composite >= TIERS['warm']:
        tier = 'Warm'
    elif composite >= TIERS['cool']:
        tier = 'Cool'
    else:
        tier = 'Cold'

    result = {
        'crd': crd,
        'composite_score': composite,
        'tier': tier,
        'yoy_growth_latest': growth_data['latest_growth'],
        'yoy_growth_avg': growth_data['avg_growth'],
        'growth_years': growth_data['growth_years'],
        'proximity_score': d2,
        'acceleration': d4,
        'is_rapid_growth': rapid['is_rapid'],
        'rapid_growth_reasons': rapid['reasons'],
        'dimensions': {
            'yoy_growth': {'score': d1, 'weight': WEIGHTS['yoy_growth']},
            'proximity': {'score': d2, 'weight': WEIGHTS['proximity']},
            'consistency': {'score': d3, 'weight': WEIGHTS['consistency']},
            'acceleration': {'score': d4, 'weight': WEIGHTS['acceleration']},
            'firmographic': {'score': d5, 'weight': WEIGHTS['firmographic']},
        },
        'milestones_crossed': growth_data['milestones_crossed'],
        'snapshots': growth_data['snapshots'],
        'growth_rates': growth_data['growth_rates'],
    }

    # Store in database
    upsert_growth_score(crd, {
        'yoy_growth_latest': growth_data['latest_growth'],
        'yoy_growth_avg': growth_data['avg_growth'],
        'growth_years': growth_data['growth_years'],
        'proximity_score': d2,
        'acceleration': d4,
        'composite_score': composite,
        'tier': tier,
        'score_details': json.dumps(result, default=str),
    }, db_path=db_path)

    return result


def score_all_firms(db_path=None, progress_callback=None):
    """Score all firms in the database.

    Args:
        db_path: Database path.
        progress_callback: Optional callable(current, total, results_dict).

    Returns dict: {scored, skipped, by_tier: {Hot: n, Warm: n, ...}}
    """
    init_db(db_path)
    firms = get_firms(db_path=db_path)
    total = len(firms)

    results = {
        'scored': 0,
        'skipped': 0,
        'by_tier': {'Hot': 0, 'Warm': 0, 'Cool': 0, 'Cold': 0},
    }

    for i, firm in enumerate(firms):
        crd = firm['crd']
        score_result = score_firm(crd, db_path=db_path)

        if score_result:
            results['scored'] += 1
            tier = score_result['tier']
            results['by_tier'][tier] = results['by_tier'].get(tier, 0) + 1
        else:
            results['skipped'] += 1

        if progress_callback:
            progress_callback(i + 1, total, results)

    return results


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        crd = int(sys.argv[1])
        result = score_firm(crd)
        if result:
            print(f"CRD {crd}: {result['tier']} ({result['composite_score']}/100)")
            print(f"  YoY Growth: {result['yoy_growth_latest']}%")
            print(f"  Growth Years: {result['growth_years']}")
            for dim, data in result['dimensions'].items():
                print(f"  {dim}: {data['score']} (weight: {data['weight']})")
        else:
            print(f"CRD {crd}: Firm not found")
    else:
        result = score_all_firms()
        print(f"Scoring complete: {result}")
