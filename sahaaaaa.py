"""
Test semua tools pipeline pada satu URL — 0 LLM, pure fetch + schema.org + rule-based.
Usage: python test_jufair.py
"""
import sys
import io
import json
import asyncio
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

TARGET_URL = "https://www.sahaexpo.com/en/exhibitor/2j-antennas"

SEP = "─" * 70


def section(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def show_result(label: str, data: dict) -> None:
    if not data:
        print(f"  [{label}] → (kosong / tidak ada data)")
        return
    fields_found = [k for k, v in data.items() if v and k not in ("source_url", "extraction_method", "confidence_score", "is_valid")]
    print(f"  [{label}] → {len(fields_found)} field terisi  (confidence={data.get('confidence_score', 0):.2f})")
    for k, v in data.items():
        if k in ("source_url", "extraction_method", "confidence_score", "is_valid"):
            continue
        val = str(v)
        if len(val) > 120:
            val = val[:120] + "..."
        print(f"    {k:<20} {val}")


# ─── Step 1: Fetch ────────────────────────────────────────────────────────────

section("STEP 1 — FETCH")
print(f"  URL  : {TARGET_URL}")

from backend.tools.fetch_tools import fetch_page_async, _store_and_strip, get_cached_html

async def do_fetch():
    raw = await fetch_page_async(TARGET_URL)
    stripped = _store_and_strip(raw)
    return raw, stripped

raw_result, meta = asyncio.run(do_fetch())

print(f"  status        : {meta['status']}")
print(f"  success       : {meta['success']}")
print(f"  content_length: {meta['content_length']:,} chars")
print(f"  is_js_rendered: {meta['is_js_rendered']}")
print(f"  response_time : {meta['response_time']:.2f}s")
print(f"  final_url     : {meta['final_url']}")
if meta.get("error"):
    print(f"  error         : {meta['error']}")

html = get_cached_html(TARGET_URL)
if not html:
    print("\n  [FAIL] HTML tidak berhasil difetch. Cek koneksi / robots.txt.")
    sys.exit(1)

print(f"\n  HTML tersimpan di cache: {len(html):,} chars")
print(f"  Preview (500 chars pertama):")
print("  " + html[:500].replace("\n", " "))


# ─── Step 2: Schema.org extraction ───────────────────────────────────────────

section("STEP 2 — SCHEMA.ORG EXTRACTION (JSON-LD + microdata + OpenGraph)")

from backend.tools.extract_tools import _extract_schema_org

schema_result = _extract_schema_org(html, TARGET_URL)
show_result("schema_org", schema_result)


# ─── Step 3: Rule-based extraction ───────────────────────────────────────────

section("STEP 3 — RULE-BASED EXTRACTION (CSS selectors + regex)")

from backend.tools.extract_tools import _extract_rule_based
import inspect as _inspect

_fn_file = _inspect.getfile(_extract_rule_based)
_fn_line = _inspect.getsourcelines(_extract_rule_based)[1]
print(f"  [sanity] _extract_rule_based loaded from: {_fn_file}:{_fn_line}")

from bs4 import BeautifulSoup as _BS
_sanity_soup = _BS(html, "lxml")
_hs = _sanity_soup.find_all(["h1", "h2", "h3", "h4"])
print(f"  [sanity] Headings in soup: {[(h.name, h.get_text(strip=True)[:40]) for h in _hs[:6]]}")

rule_result = _extract_rule_based(html, TARGET_URL)
show_result("rule_based", rule_result)


# ─── Step 4: Merge ───────────────────────────────────────────────────────────

section("STEP 4 — MERGE (schema_org + rule_based digabung)")

from backend.tools.extract_tools import _merge_vendor_data

sources = [r for r in [schema_result, rule_result] if r]
if len(sources) > 1:
    merged = _merge_vendor_data(sources)
elif len(sources) == 1:
    merged = sources[0]
    print("  (hanya satu source — tidak ada merge)")
else:
    merged = {}
    print("  (kedua source kosong)")

show_result("merged", merged)


# ─── Step 5: Validate ────────────────────────────────────────────────────────

section("STEP 5 — VALIDATE + CLEAN")

from backend.tools.extract_tools import _validate_vendor

final = _validate_vendor(merged) if merged else {}
show_result("final", final)


# ─── Step 6: Ringkasan ───────────────────────────────────────────────────────

section("RINGKASAN")

if final and final.get("is_valid"):
    print(f"  STATUS  : VALID — vendor ditemukan!")
    print(f"  Nama    : {final.get('name', '-')}")
    print(f"  Website : {final.get('website', '-')}")
    print(f"  Email   : {final.get('email', '-')}")
    print(f"  Phone   : {final.get('phone', '-')}")
    print(f"  Country : {final.get('country', '-')}")
    print(f"  Metode  : {final.get('extraction_method', '-')}")
    print(f"  Score   : {final.get('confidence_score', 0):.2f}")
else:
    print("  STATUS  : TIDAK VALID — tidak ada vendor yang bisa diekstrak")
    print("  Kemungkinan: halaman ini adalah listing (banyak vendor), bukan profil 1 vendor.")
    print("  Untuk listing, kita perlu crawl sub-URL tiap vendor.")

print()


# ─── Step 7: Link analysis (apakah ini listing atau profil?) ─────────────────

section("STEP 6b — RAW LABEL-VALUE PAIRS (debug)")

from bs4 import BeautifulSoup
soup = BeautifulSoup(html, "lxml")

# Tampilkan semua <tr> dengan 2 cell (struktur tabel info)
print("  Table rows (2-cell):")
count = 0
for row in soup.find_all("tr"):
    cells = row.find_all(["td", "th"])
    if len(cells) == 2:
        l = cells[0].get_text(strip=True)[:40]
        v = cells[1].get_text(strip=True)[:60]
        if l and v:
            print(f"    [{l}] → [{v}]")
            count += 1
            if count >= 15:
                break
if count == 0:
    print("    (tidak ada)")

# dl/dt/dd
print("\n  DL/DT/DD pairs:")
count = 0
for dl in soup.find_all("dl"):
    for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
        l = dt.get_text(strip=True)[:40]
        v = dd.get_text(strip=True)[:60]
        if l and v:
            print(f"    [{l}] → [{v}]")
            count += 1
if count == 0:
    print("    (tidak ada)")

# Class-based items
print("\n  Class-based info items (item/info/detail):")
import re as _re
count = 0
for container in soup.find_all(class_=_re.compile(r'item|info|detail', _re.I)):
    children = [c for c in container.children if hasattr(c, "get_text")]
    if len(children) == 2:
        l = children[0].get_text(strip=True)[:40]
        v = children[1].get_text(strip=True)[:60]
        if l and v and l != v:
            print(f"    [{l}] → [{v}]")
            count += 1
            if count >= 10:
                break
if count == 0:
    print("    (tidak ada)")


section("STEP 7 — __NEXT_DATA__ EXTRACTION (Next.js JSON payload)")

# Next.js apps inject all page data as JSON inside <script id="__NEXT_DATA__">
next_script = soup.find("script", id="__NEXT_DATA__")
if not next_script:
    print("  Tidak ada __NEXT_DATA__ — bukan Next.js app atau data di-load via API")
else:
    try:
        next_data = json.loads(next_script.string or "{}")
        print(f"  __NEXT_DATA__ ditemukan! Top-level keys: {list(next_data.keys())}")

        # Cari exhibitor/vendor data di dalam JSON (rekursif sederhana)
        def find_arrays(obj, path="", depth=0, found=None):
            if found is None:
                found = []
            if depth > 6:
                return found
            if isinstance(obj, list) and len(obj) > 2:
                # Cek apakah item pertama terlihat seperti vendor record
                sample = obj[0] if obj else {}
                if isinstance(sample, dict):
                    keys = set(sample.keys())
                    vendor_signals = {"name", "title", "website", "country", "stand",
                                      "description", "logo", "slug", "id", "category"}
                    if len(keys & vendor_signals) >= 2:
                        found.append({"path": path, "count": len(obj), "sample_keys": list(keys)[:10], "sample": sample})
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    find_arrays(v, f"{path}.{k}", depth + 1, found)
            return found

        arrays = find_arrays(next_data)
        if arrays:
            print(f"\n  Ditemukan {len(arrays)} array kandidat vendor:\n")
            for arr in arrays[:5]:
                print(f"  PATH  : {arr['path']}")
                print(f"  COUNT : {arr['count']} item")
                print(f"  KEYS  : {arr['sample_keys']}")
                sample = arr['sample']
                for k in ["name", "title", "website", "country", "stand", "description", "slug"]:
                    if sample.get(k):
                        val = str(sample[k])[:80]
                        print(f"  [{k}] = {val}")
                print()
        else:
            print("  Tidak ada array vendor ditemukan di __NEXT_DATA__")
            print(f"  Raw (500 chars): {next_script.string[:500]}")
    except Exception as e:
        print(f"  Gagal parse __NEXT_DATA__: {e}")


section("STEP 8 — LINK DISCOVERY (semua link internal)")

from urllib.parse import urlparse as _urlparse
from collections import defaultdict

base_domain = _urlparse(TARGET_URL).netloc
all_links = []
pattern_groups: dict = defaultdict(list)

for a in soup.find_all("a", href=True):
    href = a.get("href", "").strip()
    text = a.get_text(strip=True)

    if href.startswith("/"):
        href = f"https://{base_domain}{href}"
    elif not href.startswith("http"):
        continue

    parsed = _urlparse(href)
    if base_domain not in parsed.netloc:
        continue

    path_parts = [p for p in parsed.path.split("/") if p]
    pattern = "/" + "/".join(path_parts[:2]) if len(path_parts) >= 2 else ("/" + path_parts[0] if path_parts else "/")

    all_links.append({"text": text, "href": href, "pattern": pattern})
    if len(pattern_groups[pattern]) < 3:
        pattern_groups[pattern].append({"text": text[:50], "href": href[:100]})

print(f"  Total link internal ({base_domain}): {len(all_links)}")
print(f"  URL patterns: {list(pattern_groups.keys())[:15]}")

print(f"\n  ── Breakdown per pattern ──")
for pattern, links in sorted(pattern_groups.items()):
    count_total = sum(1 for l in all_links if l["pattern"] == pattern)
    print(f"\n  [{pattern}]  ({count_total} links)")
    for link in links:
        print(f"    [{link['text'][:45]}]  →  {link['href']}")

_VENDOR_URL_KEYWORDS = [
    # English
    "exhibitor", "vendor", "company", "booth", "brand",
    "participant", "sponsor", "supplier", "partner",
    # Turkish
    "katilimci", "firma", "tedarikci", "sergileme",
    # Arabic
    "exhib", "sharik", "musharik",
    # Spanish/Portuguese
    "expositor", "empresa", "participante",
    # French
    "exposant", "entreprise", "participant",
    # German
    "aussteller", "unternehmen", "teilnehmer",
]
candidate_patterns = [p for p in pattern_groups if any(
    kw in p.lower() for kw in _VENDOR_URL_KEYWORDS
)]
if candidate_patterns:
    print(f"\n  Kandidat vendor profile pattern: {candidate_patterns}")

print(f"\n{SEP}")
print("  SELESAI — 0 LLM digunakan")
print(SEP)
print()


# ─── Step 9: HTML structure debug ────────────────────────────────────────────

section("STEP 9b — DESCRIPTION DEBUG (h4 siblings + first <p> tags)")

_s2 = _sanity_soup
_h4 = _s2.find("h4")
if _h4:
    print(f"  h4 parent tag : <{_h4.parent.name}> class={_h4.parent.get('class', [])}")
    print(f"  h4 next siblings:")
    for sib in list(_h4.next_siblings)[:5]:
        t = getattr(sib, "get_text", lambda **_: str(sib))(strip=True)
        if t:
            print(f"    <{getattr(sib, 'name', 'text')}> {t[:120]}")
    print(f"\n  h4 parent next siblings (the description should be here):")
    for sib in list(_h4.parent.find_next_siblings())[:5]:
        sib_t = sib.get_text(separator=" ", strip=True)
        print(f"    <{sib.name}> class={sib.get('class',[])} len={len(sib_t)} text={sib_t[:100]}")
    print(f"\n  h4 parent inner text (200 chars):")
    print(f"    {_h4.parent.get_text(separator=' ', strip=True)[:200]}")

print(f"\n  First 15 <p> tags in full soup:")
for i, p in enumerate(_s2.find_all("p")[:15]):
    t = p.get_text(strip=True)
    if t:
        print(f"    [{i}] ({len(t)} chars) {t[:100]}")

_main_s = _s2.find("main") or _s2.find("article")
if _main_s:
    print(f"\n  First 10 <p> inside <main>/<article>:")
    for i, p in enumerate(_main_s.find_all("p")[:10]):
        t = p.get_text(strip=True)
        if t:
            print(f"    [{i}] ({len(t)} chars) {t[:100]}")
else:
    print("\n  (tidak ada <main>/<article> element)")

section("STEP 9 — HTML STRUCTURE DEBUG (headings + meta + social context)")

print("  <title>:")
title_tag = soup.find("title")
print(f"    {title_tag.get_text(strip=True)[:120] if title_tag else '(none)'}")

print("\n  Meta tags:")
for prop in ["og:title", "og:description", "og:type", "og:site_name"]:
    m = soup.find("meta", {"property": prop})
    print(f"    {prop:<25} {m.get('content', '(none)')[:90] if m else '(none)'}")
m_desc = soup.find("meta", {"name": "description"})
print(f"    {'name=description':<25} {m_desc.get('content', '(none)')[:90] if m_desc else '(none)'}")

print("\n  Headings (h1–h4, up to 3 each):")
for tag in ["h1", "h2", "h3", "h4"]:
    for el in soup.find_all(tag)[:3]:
        text = el.get_text(strip=True)[:90]
        if text:
            print(f"    <{tag}>  {text}")

print("\n  Social links with ancestor context:")
for a in soup.find_all("a", href=True):
    href = a.get("href", "")
    if not any(kw in href for kw in ["linkedin.com", "twitter.com", "x.com"]):
        continue
    # Walk up 6 levels to find footer/header/nav
    ancestor_label = "?"
    el = a
    for _ in range(6):
        el = el.parent
        if not el or not hasattr(el, "name"):
            break
        if el.name in ("footer", "header", "nav", "aside"):
            ancestor_label = f"<{el.name}>"
            break
        cls = " ".join(el.get("class", []))[:40]
        if cls:
            ancestor_label = cls
    print(f"    {href[:70]:<70}  in [{ancestor_label}]")
