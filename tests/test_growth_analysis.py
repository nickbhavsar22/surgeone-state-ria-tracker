"""Tests for growth_analysis.py — AUM growth scoring."""

import pytest

from tools.cache_db import upsert_firms, upsert_aum_history
from tools.growth_analysis import (
    calculate_yoy_growth, detect_rapid_growth, score_firm, score_all_firms,
    _score_yoy_growth, _score_proximity, _score_consistency,
    _score_acceleration, _score_firmographic,
)


class TestCalculateYoyGrowth:
    def test_growth_with_history(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        upsert_aum_history(sample_aum_history[100001], db_path=db_path)

        result = calculate_yoy_growth(100001, db_path=db_path)
        assert len(result['snapshots']) == 4
        assert len(result['growth_rates']) == 3
        assert result['growth_years'] == 3  # all positive
        assert result['latest_growth'] is not None
        # 78M -> 85M = ~8.97%
        assert 8 < result['latest_growth'] < 10

    def test_growth_with_no_history(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        result = calculate_yoy_growth(100001, db_path=db_path)
        assert result['snapshots'] == []
        assert result['growth_rates'] == []
        assert result['latest_growth'] is None

    def test_growth_with_single_snapshot(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        upsert_aum_history([sample_aum_history[100001][0]], db_path=db_path)
        result = calculate_yoy_growth(100001, db_path=db_path)
        assert len(result['growth_rates']) == 0

    def test_milestone_crossing(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        upsert_aum_history(sample_aum_history[100001], db_path=db_path)
        result = calculate_yoy_growth(100001, db_path=db_path)
        # 55M -> 68M crosses $50M milestone (already above)
        # 55M -> 68M -> 78M -> 85M: crosses $70M milestone
        assert 70_000_000 in result['milestones_crossed']


class TestDetectRapidGrowth:
    def test_rapid_consecutive_growth(self):
        growth_data = {
            'growth_rates': [25.0, 22.0, 18.0],
            'milestones_crossed': [],
        }
        result = detect_rapid_growth(growth_data)
        assert result['is_rapid'] is True

    def test_not_rapid_low_growth(self):
        growth_data = {
            'growth_rates': [5.0, 3.0, 4.0],
            'milestones_crossed': [],
        }
        result = detect_rapid_growth(growth_data)
        assert result['is_rapid'] is False

    def test_rapid_milestone_crossing(self):
        growth_data = {
            'growth_rates': [10.0, 12.0],
            'milestones_crossed': [50_000_000, 70_000_000],
        }
        result = detect_rapid_growth(growth_data)
        assert result['is_rapid'] is True


class TestDimensionScoring:
    def test_yoy_growth_high(self):
        assert _score_yoy_growth(25.0) == 100
        assert _score_yoy_growth(20.0) == 90

    def test_yoy_growth_moderate(self):
        assert _score_yoy_growth(15.0) == 80
        assert _score_yoy_growth(10.0) == 60

    def test_yoy_growth_low(self):
        assert _score_yoy_growth(5.0) == 40
        assert _score_yoy_growth(2.0) == 20

    def test_yoy_growth_negative(self):
        assert _score_yoy_growth(-5.0) == 0

    def test_yoy_growth_none(self):
        assert _score_yoy_growth(None) == 0

    def test_proximity_high(self):
        assert _score_proximity(105_000_000) == 100
        assert _score_proximity(100_000_000) == 95

    def test_proximity_mid(self):
        assert _score_proximity(85_000_000) == 65
        assert _score_proximity(80_000_000) == 50

    def test_proximity_low(self):
        assert _score_proximity(70_000_000) == 25

    def test_proximity_none(self):
        assert _score_proximity(None) == 0

    def test_consistency_full(self):
        assert _score_consistency(4) == 100
        assert _score_consistency(3) == 85

    def test_consistency_partial(self):
        assert _score_consistency(2) == 70
        assert _score_consistency(1) == 40

    def test_consistency_none(self):
        assert _score_consistency(0) == 0

    def test_acceleration_positive(self):
        assert _score_acceleration([10.0, 20.0]) == 100  # +10 diff
        assert _score_acceleration([10.0, 15.0]) == 85   # +5 diff

    def test_acceleration_stable(self):
        assert _score_acceleration([10.0, 12.0]) == 60

    def test_acceleration_decelerating(self):
        assert _score_acceleration([20.0, 16.0]) == 35

    def test_acceleration_insufficient_data(self):
        assert _score_acceleration([10.0]) == 50

    def test_firmographic_full(self):
        firm = {'employees': 15, 'clients': 120, 'state': 'NY', 'website': 'https://test.com'}
        assert _score_firmographic(firm) == 100

    def test_firmographic_minimal(self):
        firm = {'employees': 1, 'clients': 5, 'state': 'WY', 'website': None}
        assert _score_firmographic(firm) == 0


class TestScoreFirm:
    def test_score_firm_with_history(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        for crd_records in sample_aum_history.values():
            upsert_aum_history(crd_records, db_path=db_path)

        result = score_firm(100002, db_path=db_path)
        assert result is not None
        assert 0 <= result['composite_score'] <= 100
        assert result['tier'] in ('Hot', 'Warm', 'Cool', 'Cold')
        assert result['crd'] == 100002
        assert 'dimensions' in result

    def test_score_firm_not_found(self, db_path):
        result = score_firm(999999, db_path=db_path)
        assert result is None


class TestScoreAllFirms:
    def test_score_all(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        for crd_records in sample_aum_history.values():
            upsert_aum_history(crd_records, db_path=db_path)

        result = score_all_firms(db_path=db_path)
        assert result['scored'] == 3
        total_tiers = sum(result['by_tier'].values())
        assert total_tiers == 3
