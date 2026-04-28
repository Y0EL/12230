import sys
import io
import warnings
warnings.filterwarnings("ignore")
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from ddgs import DDGS
from deep_translator import GoogleTranslator

QUERY_ID = "pameran teknologi pertahanan siber 2025"

REGIONS = [
    ("Global",  "wt-wt", "en",    "en"),
    ("China",   "cn-zh", "zh-CN", "id"),
    ("Jepang",  "jp-ja", "ja",    "id"),
    ("Korea",   "kr-ko", "ko",    "id"),
]

MAX_RESULTS = 5

def translate(text: str, src: str, dest: str) -> str:
    if not text or src == dest:
        return text
    try:
        return GoogleTranslator(source=src, target=dest).translate(text[:500]) or text
    except Exception as e:
        return f"[err: {e}]"

def search_region(query: str, region: str, max_results: int) -> list[dict]:
    try:
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, region=region, safesearch="off", max_results=max_results):
                results.append({
                    "title":   r.get("title", ""),
                    "url":     r.get("href", ""),
                    "snippet": (r.get("body", "") or "")[:200],
                })
        return results
    except Exception as e:
        print(f"  [ERROR] DDG search gagal: {e}")
        return []

def main():
    print()
    print("=" * 72)
    print("  MEGA CRAWLER — TEST MULTI REGION SEARCH")
    print(f"  Query (ID): {QUERY_ID}")
    print("=" * 72)

    for label, region, target_lang, display_lang in REGIONS:
        print()
        print(f"  >>> REGION: {label} [{region}]")
        print("-" * 72)

        if target_lang == "en":
            query_translated = translate(QUERY_ID, "id", "en")
        elif target_lang == "zh-CN":
            query_translated = translate(QUERY_ID, "id", "zh-CN")
        elif target_lang == "ja":
            query_translated = translate(QUERY_ID, "id", "ja")
        elif target_lang == "ko":
            query_translated = translate(QUERY_ID, "id", "ko")
        else:
            query_translated = QUERY_ID

        print(f"  Query diterjemahkan: {query_translated}")
        print()

        results = search_region(query_translated, region, MAX_RESULTS)

        if not results:
            print("  Tidak ada hasil.")
            continue

        print(f"  Ditemukan {len(results)} hasil:\n")

        for i, r in enumerate(results, 1):
            raw_title   = r["title"]
            raw_snippet = r["snippet"]
            url         = r["url"]

            if display_lang == "id" and target_lang not in ("en",):
                title_id   = translate(raw_title,   "auto", "id")
                snippet_id = translate(raw_snippet, "auto", "id")
            else:
                title_id   = raw_title
                snippet_id = raw_snippet

            print(f"  [{i}] {title_id}")
            print(f"       URL     : {url}")
            if target_lang not in ("en",):
                print(f"       Asli    : {raw_title}")
            print(f"       Preview : {snippet_id[:120]}")
            print()

    print("=" * 72)
    print("  SELESAI — Cek URL .cn / .jp / .kr untuk validasi regional")
    print("=" * 72)
    print()

if __name__ == "__main__":
    main()
