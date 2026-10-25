"""Fictional campus IT data and read-only tools (carried over from the Phase 3 project)."""
import re

CATEGORIES = ('account', 'network', 'hardware', 'general')
CRITICAL = ('outage', 'delet', 'compromis', 'smoke', 'swollen', 'all students', 'burning')
KEYWORDS = {
    'account': ('password', 'account', 'login', 'mfa'),
    'network': ('wifi', 'wi-fi', 'network', 'internet', 'vpn'),
    'hardware': ('laptop', 'printer', 'battery', 'hardware'),
}
CAMPUS = {
    'STU-1001': {'role': 'student', 'status': 'active', 'mfa': 'enrolled', 'residence': 'North Hall'},
    'STU-1002': {'role': 'student', 'status': 'locked', 'mfa': 'recovery_required', 'residence': 'East Hall'},
    'STAFF-2001': {'role': 'staff', 'status': 'active', 'mfa': 'enrolled', 'residence': 'Library'},
}
KB = {
    'account': 'Use the campus password portal. MFA recovery requires an in-person ID check. '
               'Account deletion must be reviewed by an authorized administrator; never delete automatically.',
    'network': 'Check the campus status page, reconnect to Campus-Secure, and renew the device DHCP lease. '
               'North Hall access point NH-02 is degraded. Building-wide outages require a network ticket.',
    'hardware': 'Record the asset tag and disconnect unsafe devices. Do not charge swollen batteries. '
                'A technician must inspect smoke, heat, liquid damage, or battery swelling.',
    'general': 'Campus IT supports accounts, Wi-Fi, VPN, printers and laptops. The desk is open 08:00-18:00.',
}
ACCOUNT_ID = re.compile(r'\b(?:STU|STAFF)-\d{4}\b', re.I)


def keyword_triage(text):
    text = text.lower()
    category = next((name for name, words in KEYWORDS.items() if any(w in text for w in words)), 'general')
    return {'category': category, 'severity': 'high' if any(w in text for w in CRITICAL) else 'low'}


def search_knowledge_base(category):
    return KB.get(category, KB['general'])


def lookup_campus_account(account_id):
    return CAMPUS.get(account_id.upper(), {'status': 'not_found', 'advice': 'Ask for a campus account ID.'})


def check_service_status(service):
    return {'service': service, 'status': 'degraded' if service in ('wifi', 'network') else 'operational',
            'source': 'mock campus status registry'}
