"""Tests for cache_db.py — SQLite database layer."""

import pytest
from tools.cache_db import (
    init_db, get_connection, upsert_firms, get_firms, get_firm_by_crd,
    get_distinct_states, upsert_aum_history, get_aum_history,
    get_snapshot_dates, upsert_growth_score, get_growth_scores,
    get_hot_list, insert_contact, get_contacts_for_firm,
    get_all_contacts_with_firms, delete_contacts_for_firm,
    get_contact_stats, upsert_form_adv, get_form_adv,
    log_import, get_import_history, log_export, get_export_history,
    get_pipeline_stats,
)


class TestDatabaseInit:
    def test_init_creates_tables(self, db_path):
        conn = get_connection(db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {t['name'] for t in tables}
        assert 'firms' in table_names
        assert 'aum_history' in table_names
        assert 'growth_scores' in table_names
        assert 'form_adv_details' in table_names
        assert 'contacts' in table_names
        assert 'enrichment_log' in table_names
        assert 'import_history' in table_names
        assert 'export_history' in table_names
        conn.close()


class TestFirms:
    def test_upsert_and_get(self, db_path, sample_firms):
        count = upsert_firms(sample_firms, db_path=db_path)
        assert count == 3

        firms = get_firms(db_path=db_path)
        assert len(firms) == 3

    def test_get_firm_by_crd(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        firm = get_firm_by_crd(100001, db_path=db_path)
        assert firm is not None
        assert firm['company'] == 'Alpha Wealth Advisors'
        assert firm['aum'] == 85_000_000

    def test_get_firms_with_state_filter(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        ny_firms = get_firms(db_path=db_path, state='NY')
        assert len(ny_firms) == 1
        assert ny_firms[0]['company'] == 'Alpha Wealth Advisors'

    def test_get_firms_with_aum_filter(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        firms = get_firms(db_path=db_path, min_aum=80_000_000, max_aum=100_000_000)
        assert len(firms) == 2  # Alpha (85M) and Beta (95M)

    def test_get_distinct_states(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        states = get_distinct_states(db_path=db_path)
        assert set(states) == {'CA', 'NY', 'TX'}

    def test_upsert_updates_existing(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        updated = [{'crd': 100001, 'company': 'Alpha Wealth Updated', 'aum': 90_000_000}]
        upsert_firms(updated, db_path=db_path)
        firm = get_firm_by_crd(100001, db_path=db_path)
        assert firm['company'] == 'Alpha Wealth Updated'
        assert firm['aum'] == 90_000_000


class TestAumHistory:
    def test_upsert_and_get(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        records = sample_aum_history[100001]
        count = upsert_aum_history(records, db_path=db_path)
        assert count == 4

        history = get_aum_history(100001, db_path=db_path)
        assert len(history) == 4
        assert history[0]['snapshot_date'] == '2022-01'
        assert history[-1]['aum'] == 85_000_000

    def test_get_snapshot_dates(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        for crd_records in sample_aum_history.values():
            upsert_aum_history(crd_records, db_path=db_path)
        dates = get_snapshot_dates(db_path=db_path)
        assert '2022-01' in dates
        assert '2025-01' in dates

    def test_upsert_deduplicates(self, db_path, sample_firms, sample_aum_history):
        upsert_firms(sample_firms, db_path=db_path)
        records = sample_aum_history[100001]
        upsert_aum_history(records, db_path=db_path)
        upsert_aum_history(records, db_path=db_path)
        history = get_aum_history(100001, db_path=db_path)
        assert len(history) == 4


class TestGrowthScores:
    def test_upsert_and_get(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        upsert_growth_score(100001, {
            'yoy_growth_latest': 8.97,
            'yoy_growth_avg': 16.5,
            'growth_years': 3,
            'proximity_score': 65,
            'acceleration': 35,
            'composite_score': 72,
            'tier': 'Warm',
            'score_details': '{}',
        }, db_path=db_path)

        scores = get_growth_scores(db_path=db_path)
        assert len(scores) == 1
        assert scores[0]['tier'] == 'Warm'
        assert scores[0]['company'] == 'Alpha Wealth Advisors'

    def test_hot_list(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        for i, (crd, score, tier) in enumerate([
            (100001, 72, 'Warm'),
            (100002, 88, 'Hot'),
            (100003, 45, 'Cool'),
        ]):
            upsert_growth_score(crd, {
                'composite_score': score, 'tier': tier,
                'yoy_growth_latest': 10.0, 'yoy_growth_avg': 10.0,
                'growth_years': 2, 'proximity_score': 50,
                'acceleration': 50, 'score_details': '{}',
            }, db_path=db_path)

        hot_list = get_hot_list(limit=10, db_path=db_path)
        assert len(hot_list) == 3
        assert hot_list[0]['crd'] == 100002  # highest score first


class TestContacts:
    def test_insert_and_get(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        insert_contact(100001, {
            'contact_name': 'John Smith',
            'contact_title': 'Chief Compliance Officer',
            'contact_email': 'jsmith@alphawealth.com',
            'source': 'pdf_cco',
            'confidence': 90.0,
        }, db_path=db_path)

        contacts = get_contacts_for_firm(100001, db_path=db_path)
        assert len(contacts) == 1
        assert contacts[0]['first_name'] == 'John'
        assert contacts[0]['last_name'] == 'Smith'

    def test_delete_contacts_for_firm(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        insert_contact(100001, {'contact_name': 'Jane Doe'}, db_path=db_path)
        insert_contact(100001, {'contact_name': 'Bob Jones'}, db_path=db_path)
        assert len(get_contacts_for_firm(100001, db_path=db_path)) == 2

        delete_contacts_for_firm(100001, db_path=db_path)
        assert len(get_contacts_for_firm(100001, db_path=db_path)) == 0

    def test_get_all_contacts_with_firms(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        insert_contact(100001, {'contact_name': 'John Smith'}, db_path=db_path)
        insert_contact(100002, {'contact_name': 'Jane Doe'}, db_path=db_path)

        all_contacts = get_all_contacts_with_firms(db_path=db_path)
        assert len(all_contacts) == 2
        assert all_contacts[0]['company'] is not None


class TestPipelineStats:
    def test_stats_empty_db(self, db_path):
        stats = get_pipeline_stats(db_path=db_path)
        assert stats['total_firms'] == 0
        assert stats['firms_scored'] == 0

    def test_stats_with_data(self, db_path, sample_firms):
        upsert_firms(sample_firms, db_path=db_path)
        stats = get_pipeline_stats(db_path=db_path)
        assert stats['total_firms'] == 3


class TestImportExportHistory:
    def test_import_log(self, db_path):
        log_import('test.csv', '2025-01', 1000, 500, 50, db_path=db_path)
        history = get_import_history(db_path=db_path)
        assert len(history) == 1
        assert history[0]['source_file'] == 'test.csv'

    def test_export_log(self, db_path):
        log_export('output.csv', 50, 'tier=Hot', db_path=db_path)
        history = get_export_history(db_path=db_path)
        assert len(history) == 1
        assert history[0]['record_count'] == 50
