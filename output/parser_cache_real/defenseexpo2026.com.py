def parse(html: str) -> list[dict]:
    from bs4 import BeautifulSoup
    import re

    soup = BeautifulSoup(html, 'lxml')
    exhibitors = []
    source_url = "https://www.defenseexpo2026.com/exhibitors"
    
    for item in soup.select('.exhibitor-item'):
        name = item.select_one('.company-name').get_text(strip=True)
        country = item.select_one('.country').get_text(strip=True)
        booth = item.select_one('.booth').get_text(strip=True)
        website = item.select_one('.website')['href'] if item.select_one('.website') else None
        
        if name:
            exhibitors.append({
                "name": re.sub(r'^\d+\.\s*|\(\d+\)\s*', '', name),
                "website": website,
                "email": None,
                "phone": None,
                "address": None,
                "city": None,
                "country": country,
                "category": None,
                "description": None,
                "linkedin": None,
                "twitter": None,
                "booth_number": booth,
                "event_name": "Defense Expo 2026",
                "event_location": "TBD",
                "event_date": "2026-04-28",
                "source_url": source_url
            })

    return exhibitors if exhibitors else []