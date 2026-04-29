VENDOR_SCHEMA = {
    "name": "Company name (required, non-empty string)",
    "website": "Official website URL (https://...)",
    "email": "Contact email address",
    "phone": "Phone number with country code if available",
    "address": "Full street address",
    "city": "City name",
    "country": "Country name in English (e.g. 'Malaysia', 'United States')",
    "category": "Industry or product category",
    "description": "Company description or profile text (min 20 chars)",
    "linkedin": "LinkedIn company page URL",
    "twitter": "Twitter/X profile URL",
    "booth_number": "Booth, stand, or hall number at the event",
    "event_name": "Name of the event or exhibition",
    "event_location": "Event venue, city, or country",
    "event_date": "Event date or date range (any readable format)",
    "source_url": "URL where vendor was found (required, same as input url)",
}

REQUIRED_FIELDS = {"name", "source_url"}
ALL_FIELDS = set(VENDOR_SCHEMA.keys())

SCHEMA_JSON_EXAMPLE = """{
  "name": "Acme Corp",
  "website": "https://acme.com",
  "email": "info@acme.com",
  "phone": "+1-555-0100",
  "address": "123 Main St",
  "city": "Kuala Lumpur",
  "country": "Malaysia",
  "category": "Defense Technology",
  "description": "Manufacturer of advanced defense systems.",
  "linkedin": "https://linkedin.com/company/acme",
  "twitter": "https://twitter.com/acme",
  "booth_number": "A12",
  "event_name": "DSA 2026",
  "event_location": "KLCC, Kuala Lumpur",
  "event_date": "2026-04-28",
  "source_url": "https://example.com/exhibitors"
}"""


def validate_parser_output(result: object, url: str) -> tuple[bool, str]:
    if not isinstance(result, list):
        return False, f"Expected list, got {type(result).__name__}"
    if len(result) == 0:
        return False, "Parser returned empty list"
    if len(result) > 10000:
        return False, f"Result too large: {len(result)} records"

    errors: list[str] = []
    valid_count = 0
    for i, item in enumerate(result[:20]):
        if not isinstance(item, dict):
            errors.append(f"[{i}] not a dict")
            continue
        missing = REQUIRED_FIELDS - set(item.keys())
        if missing:
            errors.append(f"[{i}] missing: {missing}")
            continue
        name = item.get("name", "")
        if not name or not isinstance(name, str) or len(name.strip()) < 2:
            errors.append(f"[{i}] name empty/too short")
            continue
        valid_count += 1

    if valid_count == 0:
        return False, "No valid records in first 20. " + "; ".join(errors[:3])
    return True, ""
