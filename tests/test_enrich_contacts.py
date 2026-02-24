"""Tests for enrich_contacts.py — website scraping and LinkedIn URLs."""

import pytest
from unittest.mock import patch, MagicMock

from tools.enrich_contacts import (
    extract_domain, normalize_url, _is_generic_email,
    _extract_emails_from_html, _extract_phones_from_html,
    build_linkedin_url, build_linkedin_basic_url,
)


class TestExtractDomain:
    def test_basic_url(self):
        assert extract_domain('https://alphawealth.com') == 'alphawealth.com'

    def test_www_prefix(self):
        assert extract_domain('https://www.alphawealth.com') == 'alphawealth.com'

    def test_no_protocol(self):
        assert extract_domain('alphawealth.com') == 'alphawealth.com'

    def test_with_path(self):
        assert extract_domain('https://alphawealth.com/about') == 'alphawealth.com'

    def test_social_media_blocked(self):
        assert extract_domain('https://linkedin.com/company/alpha') is None
        assert extract_domain('https://facebook.com/alpha') is None

    def test_none_input(self):
        assert extract_domain(None) is None
        assert extract_domain('') is None


class TestNormalizeUrl:
    def test_adds_protocol(self):
        assert normalize_url('alphawealth.com') == 'https://alphawealth.com'

    def test_keeps_existing_protocol(self):
        assert normalize_url('https://alphawealth.com') == 'https://alphawealth.com'

    def test_none_input(self):
        assert normalize_url(None) is None


class TestExtractEmailsFromHtml:
    def test_mailto_links(self):
        html = '<a href="mailto:jsmith@alphawealth.com">Email John</a>'
        emails = _extract_emails_from_html(html)
        assert 'jsmith@alphawealth.com' in emails

    def test_text_emails(self):
        html = '<p>Contact us at jsmith@alphawealth.com for more info</p>'
        emails = _extract_emails_from_html(html)
        assert 'jsmith@alphawealth.com' in emails

    def test_filters_generic(self):
        html = '<a href="mailto:info@company.com">Email</a>'
        emails = _extract_emails_from_html(html)
        assert len(emails) == 0

    def test_deduplicates(self):
        html = (
            '<a href="mailto:jsmith@firm.com">Email</a>'
            '<p>Contact jsmith@firm.com</p>'
        )
        emails = _extract_emails_from_html(html)
        assert len(emails) == 1


class TestExtractPhonesFromHtml:
    def test_basic_phone(self):
        html = '<p>Call us at (212) 555-0100</p>'
        phones = _extract_phones_from_html(html)
        assert '212-555-0100' in phones

    def test_dash_format(self):
        html = '<p>Phone: 212-555-0100</p>'
        phones = _extract_phones_from_html(html)
        assert '212-555-0100' in phones

    def test_dot_format(self):
        html = '<p>Phone: 212.555.0100</p>'
        phones = _extract_phones_from_html(html)
        assert '212-555-0100' in phones


class TestLinkedInUrls:
    def test_sales_navigator_url(self):
        url = build_linkedin_url('John', 'Smith', 'Alpha Wealth')
        assert url is not None
        assert 'linkedin.com/sales/search' in url
        assert 'John' in url
        assert 'Smith' in url

    def test_basic_url(self):
        url = build_linkedin_basic_url('John', 'Smith', 'Alpha Wealth')
        assert url is not None
        assert 'linkedin.com/search' in url

    def test_none_inputs(self):
        assert build_linkedin_url(None, None, None) is None

    def test_partial_inputs(self):
        url = build_linkedin_url('John', None, 'Alpha Wealth')
        assert url is not None
        assert 'John' in url
