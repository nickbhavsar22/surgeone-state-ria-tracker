"""Shared pytest configuration and fixtures."""

import os
import tempfile
import pytest

from tools.cache_db import init_db, get_connection


@pytest.fixture
def db_path(tmp_path):
    """Create a temporary database for testing."""
    path = str(tmp_path / "test_ria.db")
    init_db(path)
    return path


@pytest.fixture
def sample_firms():
    """Sample firm records for testing."""
    return [
        {
            'crd': 100001,
            'company': 'Alpha Wealth Advisors',
            'legal_name': 'Alpha Wealth Advisors LLC',
            'state': 'NY',
            'city': 'New York',
            'phone': '212-555-0100',
            'website': 'https://alphawealth.com',
            'aum': 85_000_000,
            'aum_discretionary': 80_000_000,
            'aum_nondiscretionary': 5_000_000,
            'employees': 8,
            'clients': 45,
            'sec_registered': 'N',
            'filing_date': '2025-12-01',
            'status': 'APPROVED',
        },
        {
            'crd': 100002,
            'company': 'Beta Capital Management',
            'legal_name': 'Beta Capital Management Inc',
            'state': 'CA',
            'city': 'San Francisco',
            'phone': '415-555-0200',
            'website': 'https://betacapital.com',
            'aum': 95_000_000,
            'aum_discretionary': 90_000_000,
            'aum_nondiscretionary': 5_000_000,
            'employees': 12,
            'clients': 80,
            'sec_registered': 'N',
            'filing_date': '2025-11-15',
            'status': 'APPROVED',
        },
        {
            'crd': 100003,
            'company': 'Gamma Financial Group',
            'legal_name': 'Gamma Financial Group LP',
            'state': 'TX',
            'city': 'Dallas',
            'phone': '214-555-0300',
            'website': 'https://gammafinancial.com',
            'aum': 72_000_000,
            'aum_discretionary': 70_000_000,
            'aum_nondiscretionary': 2_000_000,
            'employees': 5,
            'clients': 30,
            'sec_registered': 'N',
            'filing_date': '2025-10-01',
            'status': 'APPROVED',
        },
    ]


@pytest.fixture
def sample_aum_history():
    """Sample AUM history for growth analysis testing."""
    return {
        100001: [
            {'crd': 100001, 'snapshot_date': '2022-01', 'aum': 55_000_000, 'aum_discretionary': 50_000_000, 'employees': 5, 'clients': 25},
            {'crd': 100001, 'snapshot_date': '2023-01', 'aum': 68_000_000, 'aum_discretionary': 63_000_000, 'employees': 6, 'clients': 32},
            {'crd': 100001, 'snapshot_date': '2024-01', 'aum': 78_000_000, 'aum_discretionary': 73_000_000, 'employees': 7, 'clients': 38},
            {'crd': 100001, 'snapshot_date': '2025-01', 'aum': 85_000_000, 'aum_discretionary': 80_000_000, 'employees': 8, 'clients': 45},
        ],
        100002: [
            {'crd': 100002, 'snapshot_date': '2022-01', 'aum': 60_000_000, 'aum_discretionary': 55_000_000, 'employees': 8, 'clients': 50},
            {'crd': 100002, 'snapshot_date': '2023-01', 'aum': 75_000_000, 'aum_discretionary': 70_000_000, 'employees': 10, 'clients': 62},
            {'crd': 100002, 'snapshot_date': '2024-01', 'aum': 90_000_000, 'aum_discretionary': 85_000_000, 'employees': 11, 'clients': 72},
            {'crd': 100002, 'snapshot_date': '2025-01', 'aum': 95_000_000, 'aum_discretionary': 90_000_000, 'employees': 12, 'clients': 80},
        ],
        100003: [
            {'crd': 100003, 'snapshot_date': '2023-01', 'aum': 50_000_000, 'aum_discretionary': 48_000_000, 'employees': 3, 'clients': 20},
            {'crd': 100003, 'snapshot_date': '2025-01', 'aum': 72_000_000, 'aum_discretionary': 70_000_000, 'employees': 5, 'clients': 30},
        ],
    }
