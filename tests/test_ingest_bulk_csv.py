"""Tests for ingest_bulk_csv.py — SEC data ingestion."""

import pytest
import pandas as pd

from tools.ingest_bulk_csv import (
    _safe_int, _safe_str, parse_sec_dataframe, is_state_registered,
    is_in_aum_range, filter_target_firms, COLUMN_MAP,
)


class TestSafeConversions:
    def test_safe_int_normal(self):
        assert _safe_int('12345') == 12345

    def test_safe_int_with_commas(self):
        assert _safe_int('1,234,567') == 1234567

    def test_safe_int_with_dollar(self):
        assert _safe_int('$85,000,000') == 85000000

    def test_safe_int_float(self):
        assert _safe_int('85000000.0') == 85000000

    def test_safe_int_blank(self):
        assert _safe_int('') is None
        assert _safe_int(None) is None

    def test_safe_int_nan(self):
        assert _safe_int(float('nan')) is None

    def test_safe_str_normal(self):
        assert _safe_str('  hello  ') == 'hello'

    def test_safe_str_blank(self):
        assert _safe_str('') is None
        assert _safe_str(None) is None


class TestFiltering:
    def test_is_state_registered_blank(self):
        assert is_state_registered({'sec_registered': ''}) is True

    def test_is_state_registered_no(self):
        assert is_state_registered({'sec_registered': 'N'}) is True

    def test_is_state_registered_yes(self):
        assert is_state_registered({'sec_registered': 'Y'}) is False

    def test_is_state_registered_none(self):
        assert is_state_registered({'sec_registered': None}) is True

    def test_is_in_aum_range_in_range(self):
        assert is_in_aum_range({'aum': 85_000_000}) is True

    def test_is_in_aum_range_below(self):
        assert is_in_aum_range({'aum': 50_000_000}) is False

    def test_is_in_aum_range_above(self):
        assert is_in_aum_range({'aum': 120_000_000}) is False

    def test_is_in_aum_range_at_boundary(self):
        assert is_in_aum_range({'aum': 70_000_000}) is True
        assert is_in_aum_range({'aum': 105_000_000}) is True

    def test_is_in_aum_range_none(self):
        assert is_in_aum_range({'aum': None}) is False


class TestFilterTargetFirms:
    def test_filter_returns_correct_firms(self):
        records = [
            {'crd': 1, 'sec_registered': 'N', 'aum': 85_000_000},
            {'crd': 2, 'sec_registered': 'Y', 'aum': 85_000_000},  # SEC registered
            {'crd': 3, 'sec_registered': 'N', 'aum': 50_000_000},  # below range
            {'crd': 4, 'sec_registered': 'N', 'aum': 100_000_000},  # in range
            {'crd': 5, 'sec_registered': '', 'aum': 120_000_000},  # above range
        ]
        target, stats = filter_target_firms(records)
        assert len(target) == 2
        assert stats['total'] == 5
        assert stats['state_registered'] == 4
        assert stats['in_aum_range'] == 2
        crds = {r['crd'] for r in target}
        assert crds == {1, 4}


class TestParseSecDataframe:
    def test_parse_basic_csv(self):
        data = {
            'Organization CRD#': ['100001', '100002'],
            'Primary Business Name': ['Firm A', 'Firm B'],
            'Main Office State': ['NY', 'CA'],
            '5F(2)(c)': ['85000000', '95000000'],
            '2A(1)': ['N', 'Y'],
            '5A': ['8', '12'],
            '5C(1)': ['45', '80'],
        }
        df = pd.DataFrame(data)
        records = parse_sec_dataframe(df)
        assert len(records) == 2
        assert records[0]['crd'] == 100001
        assert records[0]['company'] == 'Firm A'
        assert records[0]['aum'] == 85_000_000
        assert records[1]['sec_registered'] == 'Y'

    def test_parse_handles_missing_columns(self):
        data = {
            'Organization CRD#': ['100001'],
            'Primary Business Name': ['Firm A'],
        }
        df = pd.DataFrame(data)
        records = parse_sec_dataframe(df)
        assert len(records) == 1
        assert records[0]['crd'] == 100001

    def test_parse_drops_invalid_crd(self):
        data = {
            'Organization CRD#': ['100001', '', 'bad'],
            'Primary Business Name': ['Firm A', 'Firm B', 'Firm C'],
        }
        df = pd.DataFrame(data)
        records = parse_sec_dataframe(df)
        assert len(records) == 1
