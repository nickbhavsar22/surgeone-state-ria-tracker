"""Tests for extract_cco.py — CCO extraction."""

import pytest
from unittest.mock import patch, MagicMock

from tools.cache_db import upsert_firms
from tools.extract_cco import (
    _is_valid_person_name, _format_phone, _is_generic_email,
    extract_cco_from_csv_row, extract_contacts_from_pdf,
)


class TestNameValidation:
    def test_valid_name(self):
        assert _is_valid_person_name('John Smith') is True
        assert _is_valid_person_name('Mary Jane Watson') is True

    def test_invalid_single_word(self):
        assert _is_valid_person_name('John') is False

    def test_invalid_too_many_words(self):
        assert _is_valid_person_name('A B C D E F') is False

    def test_invalid_corporate_name(self):
        assert _is_valid_person_name('Alpha Capital LLC') is False
        assert _is_valid_person_name('Beta Wealth Management') is False

    def test_invalid_all_caps_long(self):
        assert _is_valid_person_name('SOME VERY LONG COMPANY') is False

    def test_invalid_title_words_only(self):
        assert _is_valid_person_name('Vice President') is False

    def test_valid_with_middle_initial(self):
        assert _is_valid_person_name('John Q. Smith') is True

    def test_empty_or_none(self):
        assert _is_valid_person_name('') is False
        assert _is_valid_person_name(None) is False


class TestFormatPhone:
    def test_ten_digit(self):
        assert _format_phone('2125550100') == '212-555-0100'

    def test_eleven_digit_with_1(self):
        assert _format_phone('12125550100') == '212-555-0100'

    def test_formatted_input(self):
        assert _format_phone('(212) 555-0100') == '212-555-0100'

    def test_none(self):
        assert _format_phone(None) is None

    def test_empty(self):
        assert _format_phone('') is None


class TestGenericEmail:
    def test_generic_prefixes(self):
        assert _is_generic_email('info@company.com') is True
        assert _is_generic_email('support@company.com') is True
        assert _is_generic_email('compliance@company.com') is True

    def test_generic_domains(self):
        assert _is_generic_email('john@sec.gov') is True
        assert _is_generic_email('john@finra.org') is True

    def test_valid_personal_email(self):
        assert _is_generic_email('jsmith@alphawealth.com') is False

    def test_none_or_invalid(self):
        assert _is_generic_email(None) is True
        assert _is_generic_email('no-at-sign') is True


class TestExtractCcoFromCsvRow:
    def test_valid_cco(self):
        row = {
            'cco_name': 'John Smith',
            'cco_email': 'jsmith@firm.com',
            'cco_phone': '2125550100',
        }
        result = extract_cco_from_csv_row(row)
        assert result is not None
        assert result['cco_name'] == 'John Smith'
        assert result['cco_email'] == 'jsmith@firm.com'
        assert result['cco_phone'] == '212-555-0100'

    def test_missing_name(self):
        row = {'cco_name': '', 'cco_email': 'test@firm.com'}
        assert extract_cco_from_csv_row(row) is None

    def test_name_only(self):
        row = {'cco_name': 'Jane Doe', 'cco_email': '', 'cco_phone': ''}
        result = extract_cco_from_csv_row(row)
        assert result is not None
        assert result['cco_name'] == 'Jane Doe'
        assert result['cco_email'] is None


class TestExtractContactsFromPdf:
    @patch('tools.extract_cco.requests.get')
    def test_pdf_not_found(self, mock_get):
        mock_get.return_value = MagicMock(status_code=404)
        result = extract_contacts_from_pdf(999999)
        assert result == []

    @patch('tools.extract_cco.requests.get')
    @patch('tools.extract_cco.pdfplumber.open')
    def test_pdf_with_cco(self, mock_pdf_open, mock_get):
        mock_get.return_value = MagicMock(status_code=200, content=b'fake-pdf')

        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "J. Chief Compliance Officer\n"
            "Name: Robert Johnson\n"
            "Other titles: Managing Partner\n"
            "Telephone: (212) 555-0100\n"
            "Email: rjohnson@testfirm.com\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdf_open.return_value = mock_pdf

        contacts = extract_contacts_from_pdf(100001)
        assert len(contacts) >= 1
        cco = next((c for c in contacts if 'cco' in c.get('source', '')), None)
        assert cco is not None
        assert cco['name'] == 'Robert Johnson'
