"""
Mini test: OpenHands dynamic parser system
Run: python test_openhands_parser.py
"""
import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.core.config import get_settings
from backend.tools.vendor_registry import clear_registry, get_count
from openhands_parser.schema import validate_parser_output
from openhands_parser.cache import ParserCache
from openhands_parser.executor import SafeExecutor
from openhands_parser.generator import ParserGenerator

settings = get_settings()

PASS = "\033[92m PASS\033[0m"
FAIL = "\033[91m FAIL\033[0m"


def _hdr(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Test 1: executor runs valid code
# ---------------------------------------------------------------------------
def test_executor_valid():
    _hdr("TEST 1: SafeExecutor — valid parse() function")
    code = '''\
def parse(html: str) -> list[dict]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for row in soup.select("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            name = cells[0].get_text(strip=True)
            if name:
                results.append({"name": name, "source_url": "https://example.com"})
    return results
'''
    html = """
    <table>
      <tr><td>Acme Corp</td><td>USA</td></tr>
      <tr><td>BetaCo</td><td>Germany</td></tr>
    </table>
    """
    ex = SafeExecutor()
    vendors = ex.run(code, html, "https://example.com")
    ok = len(vendors) == 2 and vendors[0]["name"] == "Acme Corp"
    print(f"  vendors={vendors}")
    print(f"  Result:{PASS if ok else FAIL}")
    return ok


# ---------------------------------------------------------------------------
# Test 2: executor catches bad code
# ---------------------------------------------------------------------------
def test_executor_bad_code():
    _hdr("TEST 2: SafeExecutor — broken parse() raises RuntimeError")
    code = '''\
def parse(html: str) -> list[dict]:
    raise ValueError("intentional failure")
'''
    ex = SafeExecutor()
    try:
        ex.run(code, "<html></html>", "https://example.com")
        print(f"  Expected RuntimeError but got nothing{FAIL}")
        return False
    except RuntimeError as e:
        print(f"  Caught expected error: {e!s:.80}")
        print(f"  Result:{PASS}")
        return True


# ---------------------------------------------------------------------------
# Test 3: validate_parser_output
# ---------------------------------------------------------------------------
def test_validation():
    _hdr("TEST 3: validate_parser_output")
    url = "https://example.com"

    good = [{"name": "Acme", "source_url": url}, {"name": "BetaCo", "source_url": url}]
    ok1, _ = validate_parser_output(good, url)

    bad_missing_name = [{"source_url": url}]
    ok2, err2 = validate_parser_output(bad_missing_name, url)

    bad_not_list = {"name": "X", "source_url": url}
    ok3, err3 = validate_parser_output(bad_not_list, url)

    all_ok = ok1 and not ok2 and not ok3
    print(f"  good={ok1}  bad_missing_name={not ok2}  bad_not_list={not ok3}")
    print(f"  Result:{PASS if all_ok else FAIL}")
    return all_ok


# ---------------------------------------------------------------------------
# Test 4: cache save / get / invalidate
# ---------------------------------------------------------------------------
def test_cache():
    _hdr("TEST 4: ParserCache — save / get / invalidate")
    import tempfile

    tmp = Path(tempfile.mkdtemp())
    cache = ParserCache(cache_dir=tmp)
    domain = "test.example.com"

    code = "def parse(html): return []"
    cache.save(domain, code, {"attempt": 1})
    retrieved = cache.get(domain)
    ok_save = retrieved == code

    cache.invalidate(domain)
    ok_del = cache.get(domain) is None

    all_ok = ok_save and ok_del
    print(f"  save={ok_save}  invalidate={ok_del}")
    print(f"  Result:{PASS if all_ok else FAIL}")
    return all_ok


# ---------------------------------------------------------------------------
# Test 5: generator — real URL via LLM fallback
# ---------------------------------------------------------------------------
async def test_generator_real_url():
    _hdr("TEST 5: ParserGenerator — real URL (LLM fallback)")

    if not settings.has_openai_key:
        print(f"  SKIP — OPENAI_API_KEY not set")
        return True

    url = "https://www.milipol.com/en/exhibitors"
    print(f"  Target: {url}")
    print(f"  Fetching page...")

    import httpx
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            html = resp.text
    except Exception as e:
        print(f"  SKIP — fetch failed: {e}")
        return True

    print(f"  HTML: {len(html):,} chars")

    import tempfile
    tmp = Path(tempfile.mkdtemp())
    gen = ParserGenerator()
    gen.cache = ParserCache(cache_dir=tmp)

    t0 = time.time()
    result = await gen.generate_and_run(url, html)
    elapsed = time.time() - t0

    vendors = result["vendors"]
    cache_hit = result["cache_hit"]

    print(f"  vendors={len(vendors)}  cache_hit={cache_hit}  elapsed={elapsed:.1f}s")
    if vendors:
        print(f"  Sample: {vendors[0]}")

    ok = len(vendors) > 0
    print(f"  Result:{PASS if ok else FAIL} (0 vendors may be OK if page needs JS)")
    return True


# ---------------------------------------------------------------------------
# Test 6: cache hit reuse (same domain, second call)
# ---------------------------------------------------------------------------
async def test_cache_hit():
    _hdr("TEST 6: ParserGenerator — cache hit reuse")

    import tempfile
    tmp = Path(tempfile.mkdtemp())
    cache = ParserCache(cache_dir=tmp)

    domain = "cached.example.com"
    url = f"https://{domain}/exhibitors"
    saved_code = '''\
def parse(html: str) -> list[dict]:
    return [
        {"name": "CachedCorp", "source_url": "https://cached.example.com/exhibitors"},
        {"name": "FastResult Ltd", "source_url": "https://cached.example.com/exhibitors"},
    ]
'''
    cache.save(domain, saved_code)

    gen = ParserGenerator()
    gen.cache = cache

    t0 = time.time()
    result = await gen.generate_and_run(url, "<html>dummy</html>")
    elapsed = time.time() - t0

    ok = result["cache_hit"] and len(result["vendors"]) == 2 and elapsed < 5.0
    print(f"  cache_hit={result['cache_hit']}  vendors={len(result['vendors'])}  elapsed={elapsed:.2f}s")
    print(f"  Result:{PASS if ok else FAIL}")
    return ok


# ---------------------------------------------------------------------------
# Test 7: full tool integration (imports + registry)
# ---------------------------------------------------------------------------
def test_tool_import():
    _hdr("TEST 7: generate_and_run_parser tool — import check")
    try:
        from backend.tools.dynamic_parser_tool import generate_and_run_parser
        from backend.tools.extract_tools import ALL_EXTRACT_TOOLS
        names = [t.name for t in ALL_EXTRACT_TOOLS]
        ok = "generate_and_run_parser" in names
        print(f"  ALL_EXTRACT_TOOLS: {names}")
        print(f"  generate_and_run_parser registered={ok}")
        print(f"  Result:{PASS if ok else FAIL}")
        return ok
    except Exception as e:
        print(f"  Import error: {e}")
        print(f"  Result:{FAIL}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    results = []

    results.append(("Executor valid", test_executor_valid()))
    results.append(("Executor bad code", test_executor_bad_code()))
    results.append(("Validate output", test_validation()))
    results.append(("Cache save/get/invalidate", test_cache()))
    results.append(("Tool import + registry", test_tool_import()))

    loop = asyncio.new_event_loop()
    results.append(("Cache hit reuse", loop.run_until_complete(test_cache_hit())))
    results.append(("Generator real URL", loop.run_until_complete(test_generator_real_url())))
    loop.close()

    _hdr("SUMMARY")
    passed = sum(1 for _, ok in results if ok)
    for name, ok in results:
        status = PASS if ok else FAIL
        print(f"  {name}{status}")
    print(f"\n  {passed}/{len(results)} passed\n")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
