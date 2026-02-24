"""
Tool: Contact Enrichment via Website Scraping + LinkedIn URL Generation

Enrichment layer with pluggable sources:
  a) Website scraping — pull emails/phones from firm websites using BeautifulSoup
  b) LinkedIn Sales Navigator hint — pre-formatted search URL per contact
  c) Hunter.io (optional) — domain search if API key is configured

Respects robots.txt, rate-limits website requests (2s delay).
"""

import os
import re
import logging
import time
from urllib.parse import urlparse, urljoin, quote_plus

import requests
from bs4 import BeautifulSoup

from tools.cache_db import (
    init_db, get_firm_by_crd, insert_contact, delete_contacts_for_firm,
    get_contacts_for_firm, get_unprocessed_crds, upsert_form_adv,
    log_enrichment,
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}

WEBSITE_DELAY = 2.0  # seconds between website requests

# Social media domains to skip
SOCIAL_MEDIA_DOMAINS = {
    'linkedin.com', 'facebook.com', 'twitter.com', 'x.com',
    'instagram.com', 'youtube.com', 'tiktok.com',
}

# Generic email prefixes to filter
GENERIC_EMAIL_PREFIXES = {
    'info', 'support', 'admin', 'contact', 'sales', 'help',
    'office', 'hello', 'general', 'mail', 'noreply', 'no-reply',
    'webmaster', 'postmaster', 'reporting', 'compliance',
    'service', 'billing', 'hr', 'careers', 'media', 'press',
    'subscribe', 'feedback', 'reception', 'welcome',
}

GENERIC_EMAIL_DOMAINS = {
    'sec.gov', 'finra.org', 'example.com', 'gmail.com', 'yahoo.com',
    'hotmail.com', 'outlook.com', 'aol.com',
}

# Pages likely to contain contact info
CONTACT_PAGE_PATTERNS = [
    '/contact', '/about', '/team', '/our-team', '/people',
    '/leadership', '/staff', '/about-us', '/contact-us',
]

# Hunter.io (optional)
try:
    import streamlit as st
    HUNTER_API_KEY = st.secrets.get("HUNTER_API_KEY", os.getenv('HUNTER_API_KEY', ''))
except Exception:
    HUNTER_API_KEY = os.getenv('HUNTER_API_KEY', '')
HUNTER_DOMAIN_SEARCH = 'https://api.hunter.io/v2/domain-search'


# ---------------------------------------------------------------------------
# URL / Domain helpers
# ---------------------------------------------------------------------------

def extract_domain(url):
    """Extract domain from a URL. Returns None for social media domains."""
    if not url:
        return None
    url = url.strip().lower()
    if not url.startswith('http'):
        url = 'https://' + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        domain = domain.split(':')[0]
        if domain.startswith('www.'):
            domain = domain[4:]
        if not domain:
            return None
        for social in SOCIAL_MEDIA_DOMAINS:
            if domain == social or domain.endswith('.' + social):
                return None
        return domain
    except Exception:
        return None


def normalize_url(url):
    """Normalize a website URL to a full https URL."""
    if not url:
        return None
    url = url.strip()
    if not url.startswith('http'):
        url = 'https://' + url
    return url


def _is_generic_email(email):
    """Check if an email is generic/role-based."""
    if not email or '@' not in email:
        return True
    local, _, domain = email.partition('@')
    if domain.lower() in GENERIC_EMAIL_DOMAINS:
        return True
    if local.lower() in GENERIC_EMAIL_PREFIXES:
        return True
    return False


# ---------------------------------------------------------------------------
# Website Scraping
# ---------------------------------------------------------------------------

def _extract_emails_from_html(html_text):
    """Extract email addresses from HTML text."""
    emails = set()
    # mailto: links
    soup = BeautifulSoup(html_text, 'html.parser')
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.startswith('mailto:'):
            email = href.replace('mailto:', '').split('?')[0].strip()
            if '@' in email and not _is_generic_email(email):
                emails.add(email.lower())
    # Regex fallback in text
    text_emails = re.findall(
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        html_text
    )
    for email in text_emails:
        if not _is_generic_email(email):
            emails.add(email.lower())
    return list(emails)


def _extract_phones_from_html(html_text):
    """Extract phone numbers from HTML text."""
    soup = BeautifulSoup(html_text, 'html.parser')
    text = soup.get_text(separator=' ')
    phones = set()
    # Standard US phone patterns
    matches = re.findall(
        r'(?:(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}))',
        text
    )
    for match in matches:
        digits = re.sub(r'\D', '', match)
        if len(digits) == 10:
            formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
            phones.add(formatted)
        elif len(digits) == 11 and digits[0] == '1':
            formatted = f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
            phones.add(formatted)
    return list(phones)


def scrape_website_contacts(url):
    """Scrape a firm's website for contact information.

    Fetches the main page and common contact/about pages, extracts emails
    and phone numbers using BeautifulSoup.

    Args:
        url: Firm website URL.

    Returns dict: {emails: [str], phones: [str], pages_scraped: int}
    """
    base_url = normalize_url(url)
    if not base_url:
        return {'emails': [], 'phones': [], 'pages_scraped': 0}

    all_emails = set()
    all_phones = set()
    pages_scraped = 0

    # Pages to check: main page + contact/about pages
    pages_to_check = [base_url]
    for pattern in CONTACT_PAGE_PATTERNS:
        pages_to_check.append(urljoin(base_url, pattern))

    for page_url in pages_to_check:
        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=15,
                                allow_redirects=True)
            if resp.status_code == 200:
                emails = _extract_emails_from_html(resp.text)
                phones = _extract_phones_from_html(resp.text)
                all_emails.update(emails)
                all_phones.update(phones)
                pages_scraped += 1
            time.sleep(WEBSITE_DELAY)
        except requests.RequestException:
            continue

    return {
        'emails': list(all_emails),
        'phones': list(all_phones),
        'pages_scraped': pages_scraped,
    }


# ---------------------------------------------------------------------------
# LinkedIn Sales Navigator URL
# ---------------------------------------------------------------------------

def build_linkedin_url(first_name, last_name, company):
    """Build a LinkedIn Sales Navigator search URL for a person.

    Returns a URL that can be opened in a browser for manual lookup.
    """
    parts = []
    if first_name:
        parts.append(first_name)
    if last_name:
        parts.append(last_name)
    if company:
        parts.append(company)

    if not parts:
        return None

    query = ' '.join(parts)
    return (
        f"https://www.linkedin.com/sales/search/people"
        f"?query=(keywords%3A{quote_plus(query)})"
    )


def build_linkedin_basic_url(first_name, last_name, company):
    """Build a basic LinkedIn search URL (no Sales Navigator required)."""
    parts = []
    if first_name:
        parts.append(first_name)
    if last_name:
        parts.append(last_name)
    if company:
        parts.append(company)

    if not parts:
        return None

    query = ' '.join(parts)
    return f"https://www.linkedin.com/search/results/people/?keywords={quote_plus(query)}"


# ---------------------------------------------------------------------------
# Hunter.io (optional)
# ---------------------------------------------------------------------------

def hunter_domain_search(domain, company=None, crd=None, db_path=None):
    """Search Hunter.io for contacts at a domain. Returns list of contacts.

    Only runs if HUNTER_API_KEY is configured. Uses 1 credit per call.
    """
    if not HUNTER_API_KEY or not domain:
        return []

    try:
        params = {
            'api_key': HUNTER_API_KEY,
            'domain': domain,
            'limit': 20,
            'type': 'personal',
        }
        if company:
            params['company'] = company

        resp = requests.get(HUNTER_DOMAIN_SEARCH, params=params, timeout=15)

        log_enrichment(
            crd or 0, 'hunter_io', '/domain-search', resp.status_code,
            'success' if resp.status_code == 200 else 'error',
            credits_used=1, db_path=db_path,
        )

        if resp.status_code != 200:
            return []

        data = resp.json().get('data', {})
        contacts = []
        for entry in data.get('emails', []):
            email = entry.get('value')
            first = entry.get('first_name')
            last = entry.get('last_name')
            if not email or not first or not last or _is_generic_email(email):
                continue
            contacts.append({
                'first_name': first,
                'last_name': last,
                'contact_name': f"{first} {last}",
                'contact_title': entry.get('position'),
                'contact_email': email,
                'contact_phone': entry.get('phone_number'),
                'confidence': entry.get('confidence', 0),
                'source': 'hunter_domain_search',
            })
        return contacts

    except requests.RequestException as e:
        logger.error('Hunter.io search failed for %s: %s', domain, e)
        log_enrichment(
            crd or 0, 'hunter_io', '/domain-search', 0, 'error',
            db_path=db_path,
        )
        return []


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def enrich_firm_contacts(crd, db_path=None):
    """Enrich contacts for a single firm.

    Pipeline:
      1. Check existing contacts from CCO extraction
      2. Scrape firm website for additional emails/phones
      3. (Optional) Hunter.io domain search
      4. Generate LinkedIn URLs for all contacts
      5. Flag if contact data could not be auto-resolved

    Returns dict: {contacts_updated: int, emails_found: int, unresolved: bool}
    """
    firm = get_firm_by_crd(crd, db_path=db_path)
    if not firm:
        return {'contacts_updated': 0, 'emails_found': 0, 'unresolved': True}

    existing_contacts = get_contacts_for_firm(crd, db_path=db_path)
    website = firm.get('website')
    domain = extract_domain(website)
    company = firm.get('company') or firm.get('legal_name')

    emails_found = 0
    contacts_updated = 0

    # Step 1: Website scraping
    if website and domain:
        scrape_result = scrape_website_contacts(website)

        log_enrichment(
            crd, 'website_scrape', website, 200,
            'success' if scrape_result['emails'] else 'no_result',
            db_path=db_path,
        )

        # Assign scraped emails to existing contacts missing emails
        scraped_emails = scrape_result['emails']
        scraped_phones = scrape_result['phones']

        for contact in existing_contacts:
            if not contact.get('contact_email') and scraped_emails:
                email = scraped_emails.pop(0)
                delete_contacts_for_firm(crd, db_path=db_path)
                for c in existing_contacts:
                    if c['crd'] == crd and c.get('contact_name') == contact.get('contact_name'):
                        c['contact_email'] = email
                    insert_contact(crd, {
                        'contact_name': c.get('contact_name'),
                        'contact_title': c.get('contact_title'),
                        'contact_email': c.get('contact_email'),
                        'contact_phone': c.get('contact_phone') or (
                            scraped_phones[0] if scraped_phones else None
                        ),
                        'contact_linkedin': build_linkedin_url(
                            c.get('first_name'), c.get('last_name'), company
                        ),
                        'source': c.get('source'),
                        'confidence': 90.0 if c.get('contact_email') else 50.0,
                    }, db_path=db_path)
                    contacts_updated += 1
                emails_found += 1
                break

        # If we found new emails not assigned to existing contacts, create entries
        for email in scraped_emails:
            insert_contact(crd, {
                'contact_name': None,
                'contact_email': email,
                'contact_phone': scraped_phones.pop(0) if scraped_phones else None,
                'source': 'website_scrape',
                'confidence': 40.0,
            }, db_path=db_path)
            emails_found += 1
            contacts_updated += 1

    # Step 2: Hunter.io (optional)
    if HUNTER_API_KEY and domain:
        hunter_contacts = hunter_domain_search(domain, company, crd, db_path)
        for hc in hunter_contacts:
            # Only add if this person isn't already in our contacts
            existing_names = {
                (c.get('first_name', '').lower(), c.get('last_name', '').lower())
                for c in get_contacts_for_firm(crd, db_path=db_path)
                if c.get('first_name') and c.get('last_name')
            }
            name_key = (
                hc.get('first_name', '').lower(),
                hc.get('last_name', '').lower(),
            )
            if name_key not in existing_names:
                hc['contact_linkedin'] = build_linkedin_url(
                    hc.get('first_name'), hc.get('last_name'), company
                )
                insert_contact(crd, hc, db_path=db_path)
                contacts_updated += 1
                emails_found += 1

    # Step 3: Add LinkedIn URLs for all contacts that don't have one
    final_contacts = get_contacts_for_firm(crd, db_path=db_path)
    unresolved = all(
        not c.get('contact_email') for c in final_contacts
    ) if final_contacts else True

    return {
        'contacts_updated': contacts_updated,
        'emails_found': emails_found,
        'unresolved': unresolved,
    }


def enrich_batch(crd_list, db_path=None, progress_callback=None):
    """Enrich contacts for a batch of firms.

    Args:
        crd_list: List of CRD numbers to enrich.
        db_path: Database path.
        progress_callback: Optional callable(current, total, results_dict).

    Returns dict: {enriched, skipped, emails_found, unresolved_count}
    """
    init_db(db_path)

    results = {
        'enriched': 0,
        'skipped': 0,
        'emails_found': 0,
        'unresolved_count': 0,
    }
    total = len(crd_list)

    for i, crd in enumerate(crd_list):
        firm = get_firm_by_crd(crd, db_path=db_path)
        if not firm or not firm.get('website'):
            results['skipped'] += 1
        else:
            result = enrich_firm_contacts(crd, db_path=db_path)
            results['enriched'] += 1
            results['emails_found'] += result['emails_found']
            if result['unresolved']:
                results['unresolved_count'] += 1

        if progress_callback:
            progress_callback(i + 1, total, results)

    return results


def get_unresolved_firms(db_path=None):
    """Get firms where contact data couldn't be auto-resolved.

    Returns list of CRDs flagged for manual enrichment.
    """
    conn = __import__('tools.cache_db', fromlist=['get_connection']).get_connection(db_path)
    try:
        rows = conn.execute("""
            SELECT f.crd, f.company, f.state, f.website
            FROM firms f
            LEFT JOIN contacts c ON f.crd = c.crd AND c.contact_email IS NOT NULL
            WHERE c.id IS NULL
            ORDER BY f.company
        """).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        crd = int(sys.argv[1])
        result = enrich_firm_contacts(crd)
        print(f"CRD {crd}: {result}")
    else:
        print("Usage: python tools/enrich_contacts.py <CRD>")
