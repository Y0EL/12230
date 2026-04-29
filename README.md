# PLAYBOOK: Mega Crawler Bot


## Gambaran Sistem


Mega Crawler Bot adalah sistem otomatis untuk mengumpulkan data vendor dan exhibitor
dari website pameran industri di seluruh dunia. Input berupa query teks, output berupa
file Excel dan CSV berisi nama perusahaan, kontak, negara, kategori, dan detail lainnya.

Stack utama: LangGraph sebagai orkestrator alur kerja, LangChain sebagai wrapper tool,
OpenAI sebagai LLM fallback extraction, DuckDuckGo sebagai sumber pencarian tanpa API key,
httpx dan Playwright sebagai HTTP client berlapis.

Prinsip desain Zero LLM: LLM hanya dipanggil sebagai fallback terakhir ketika metode
deterministik gagal. Target kurang dari 15 persen vendor yang butuh LLM.


---


## Struktur Folder


```
123/
  backend/
    agents/
      base_agent.py          BaseAgent class, semua agent inherit dari sini
      search_agent.py        Discover seed URL via DDG multi-region
      crawler_agent.py       Fetch dan parse halaman, kelola antrian crawl
      extractor_agent.py     Ekstrak data vendor dari HTML
      enrichment_agent.py    Deep crawl website vendor untuk data tambahan
      export_agent.py        Kompilasi dan tulis Excel atau CSV
    tools/
      search_tools.py        DDG search dengan 14 region dan auto-translate
      fetch_tools.py         httpx, curl_cffi, Playwright berlapis
      parse_tools.py         BeautifulSoup parsing dan link scoring
      extract_tools.py       schema.org, rule-based CSS, LLM fallback
      export_tools.py        openpyxl Excel dan pandas CSV
    graph/
      state.py               CrawlerState TypedDict
      nodes.py               Fungsi node LangGraph
      workflow.py            StateGraph compile dan run_crawler entry
    core/
      config.py              Settings dari .env via pydantic-settings
    utils/
      display.py             Rich console output, banner, progress, table
  output/                    Semua file Excel dan CSV hasil run
  run.py                     Entry point CLI
  .env                       Konfigurasi lokal (jangan commit)
  .env.example               Template konfigurasi
  requirements.txt           Dependency Python
  playbook.md                Dokumen ini
```


---


## Alur Kerja LangGraph


```
START
  |
  v
discover_seeds          DDG search multi-region, hasilkan 20 sampai 50 seed URL
  |
  v
crawl_batch             Fetch ratusan URL, scoring link, isi antrian vendor pages
  |
  +-- error tinggi? --> supervisor_check    LLM baca ringkasan error, putuskan aksi
  |
  v
extract_vendors         schema.org lalu rule-based, LLM hanya jika keduanya gagal
  |
  v
enrich_domains          Kunjungi website vendor untuk lengkapi data yang kosong
  |
  v
export_results          Tulis Excel dan CSV ke folder output
  |
  v
END
```

Semua node adalah fungsi Python murni yang menerima CrawlerState dan mengembalikan
dict perubahan state. Tidak ada loop eksplisit di graph karena crawl loop ada di dalam
node crawl_batch itu sendiri.


---


## CrawlerState


Field utama yang dibawa dari node ke node:

```
query               str        Query awal dari user
seed_urls           list       URL hasil pencarian DDG
vendor_pages        list       URL halaman exhibitor yang sudah diklasifikasikan
raw_vendors         list       VendorRecord sebelum validasi
vendors             list       VendorRecord bersih setelah validasi
visited_urls        set        URL yang sudah pernah di-fetch
total_crawled       int        Total URL yang berhasil di-fetch
total_errors        int        Total URL yang gagal
errors              list       Pesan error untuk supervisor
output_excel        str        Path file Excel hasil
output_csv          str        Path file CSV hasil
phase               str        Fase saat ini untuk debugging
stats               CrawlStats Statistik detail per metode ekstraksi
```


---


## Tools Layer


### search_tools.py

`search_exhibitor_events(query)` adalah tool utama discover. Cara kerjanya:

1. Deteksi region dari keyword dalam query menggunakan REGION_MAP
2. Jika ada kata "global", "worldwide", "international" maka aktifkan semua 14 region
3. Untuk setiap region, jalankan beberapa template query di DuckDuckGo
4. Query non-Inggris diterjemahkan otomatis via deep-translator GoogleTranslator
5. Sebelum translate, query dipotong di koma pertama supaya daftar region tidak ikut diterjemahkan
6. Progress bar Rich transient ditampilkan selama proses, hilang setelah selesai

REGION_MAP mencakup: China, Japan, Korea, USA, Europe, Greece, Russia, India, Pakistan,
Southeast Asia, Oceania, Middle East, Asia general, dan Global fallback.

Jumlah template per region disesuaikan otomatis: 6 template untuk 1 atau 2 region,
4 template untuk 3 sampai 5 region, 3 template untuk 6 region ke atas.
Ini mencegah terlalu banyak request DDG yang bisa kena rate limit.


### fetch_tools.py

`fetch_page_async(url)` menggunakan tiga lapisan:

1. httpx dengan HTTP/2 dan random user agent
2. curl_cffi dengan TLS fingerprint Chrome120 jika httpx kena SSL error atau 403/429
3. Playwright headless Chromium jika halaman JS-heavy (React, Angular, Vue, Cloudflare)

Playwright disimpan per event loop menggunakan `loop._crawler_pw_browser` dan
`loop._crawler_pw_instance`. Ini penting di Windows karena setiap `asyncio.new_event_loop()`
membuat loop baru. Menyimpan browser sebagai global akan menyebabkan NoneType error karena
browser dari loop lama tidak bisa dipakai di loop baru.

Cache response di memori selama 1 jam. Deduplikasi URL dengan normalisasi trailing slash.

`fetch_pages_batch_async(urls, on_done)` menjalankan semua fetch secara concurrent dengan
semaphore. Parameter `on_done(url, result)` dipanggil tiap URL selesai untuk live progress.


### parse_tools.py

`extract_links(html, base_url)` mengekstrak semua link dari HTML menggunakan BeautifulSoup.

`classify_exhibitor_links(links, threshold)` memberi skor tiap link berdasarkan keyword
dalam URL dan teks link. Keyword seperti "exhibitor", "vendor", "sponsor", "booth" di URL
mendapat skor 2, di teks link mendapat skor 1. Link dengan skor lebih dari atau sama dengan
threshold diklasifikasikan sebagai potential vendor page.

`score_page_as_event(html, url)` menentukan apakah halaman adalah halaman event atau pameran
berdasarkan keyword konten dan struktur HTML.

`find_exhibitor_list_pages(links, base_url)` mencari link yang kemungkinan adalah halaman
daftar exhibitor seperti /exhibitors, /vendors, /directory.


### extract_tools.py

Urutan ekstraksi untuk setiap vendor page:

1. `extract_schema_org(html, url)` via library extruct. Baca JSON-LD, microdata, OpenGraph.
   Gratis, cepat, akurat jika website mengimplementasikan schema.org.

2. `extract_rule_based(html, url)` via 100+ CSS selector pattern dan regex.
   Mencakup format dari EventsAir, a2z, Stova, Swapcard, Cvent.
   Return dict dengan field yang ditemukan.

3. `extract_with_llm(html, url)` hanya jika dua metode di atas menghasilkan kurang dari
   min_vendor_fields field. HTML dikonversi ke teks via html2text, dipotong 1500 karakter,
   dikirim ke OpenAI dengan schema Pydantic via instructor library.
   max_completion_tokens = 1000 karena model reasoning seperti gpt-5-mini menggunakan
   sekitar 600 token untuk internal reasoning, menyisakan 400 untuk output JSON.

4. `merge_vendor_data(sources)` menggabungkan hasil dari semua metode. Data dari website
   vendor sendiri lebih dipercaya untuk field email dan telepon.

VendorRecord Pydantic schema: name, website, email, phone, address, city, country,
category, description, linkedin, event_name, event_location, event_date, booth_number,
source_url, extraction_method, confidence_score.

Confidence score dihitung dari jumlah field yang terisi. Vendor dengan skor di bawah
0.25 dibuang. Nama yang cocok dengan BAD_NAME_PATTERNS (teks navigasi website) dibuang.


### export_tools.py

`export_to_excel(vendors, query)` menghasilkan file Excel dengan tiga sheet:
Sheet "Vendors" dengan header biru, baris bergantian putih dan abu-abu, lebar kolom otomatis.
Sheet "Summary" dengan statistik run.
Sheet "Metadata" dengan info teknis dan konfigurasi.

Nama file: `vendors_[query_bersih]_[YYYYMMDD_HHMMSS].xlsx`

`export_to_csv(vendors, query)` menghasilkan CSV UTF-8 dengan BOM untuk kompatibilitas Excel Windows.


---


## Konfigurasi (.env)


```
OPENAI_API_KEY=sk-...              Wajib untuk LLM fallback. Kosong = LLM dinonaktifkan otomatis.
OPENAI_MODEL=gpt-4o-mini           Model default. Lihat tabel kompatibilitas di bawah.
MAX_CONCURRENT_REQUESTS=20         Request HTTP paralel secara global.
MAX_DEPTH=3                        Kedalaman crawl dari seed URL.
BATCH_SIZE=500                     Jumlah URL per batch crawl.
REQUEST_TIMEOUT=30                 Timeout tiap request HTTP dalam detik.
REQUEST_DELAY_MIN=0.5              Jeda minimum antar request ke domain sama.
REQUEST_DELAY_MAX=2.0              Jeda maksimum antar request ke domain sama.
LLM_FALLBACK_ENABLED=true          Izinkan LLM extraction fallback.
LLM_SUPERVISOR_ENABLED=true        Izinkan supervisor LLM.
LLM_ERROR_THRESHOLD=10             Jumlah error sebelum supervisor dipanggil.
OUTPUT_DIR=./output                Folder output file Excel dan CSV.
LOG_LEVEL=INFO                     Level log di terminal. DEBUG untuk verbose.
PLAYWRIGHT_HEADLESS=true           Chromium jalan tanpa window.
PLAYWRIGHT_TIMEOUT=30000           Timeout Playwright dalam milidetik.
MIN_VENDOR_FIELDS=3                Jumlah field minimum untuk rule-based dianggap sukses.
MAX_TOTAL_VENDORS=10000            Batas total vendor per run.
```

Kompatibilitas model OpenAI:

```
gpt-4o, gpt-4o-mini, gpt-4-turbo    Model standar, mendukung parameter temperature.
gpt-5-mini, gpt-5, o1, o1-mini      Reasoning model, TIDAK mendukung temperature.
o3, o3-mini, o4-mini                 Reasoning model, TIDAK mendukung temperature.
```

Properti `model_supports_temperature` di Settings digunakan oleh semua kode yang
memanggil API OpenAI untuk menyertakan atau tidak menyertakan parameter temperature.


---


## Cara Menjalankan


### Opsi Cepat: Pakai File BAT (Rekomendasi)

Tiga file `.bat` sudah tersedia di root folder. Cukup double-click atau jalankan di terminal:

| File | Fungsi |
|------|--------|
| `setup.bat` | Setup pertama kali: buat venv, install semua dependency, install Playwright |
| `start_openserp.bat` | Jalankan OpenSERP search engine di background (port 7000) |
| `run.bat` | Jalankan crawler — tinggal isi query saat diminta |

**Urutan yang benar pertama kali:**
```
1. Double-click setup.bat          ← tunggu sampai selesai
2. Isi .env dengan OPENAI_API_KEY  ← wajib
3. Double-click start_openserp.bat ← biarkan terminal ini terbuka
4. Double-click run.bat            ← di terminal baru
```


---


### Setup Manual (tanpa BAT)


#### 1. Install Python dependencies

```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```


#### 2. Konfigurasi .env

```cmd
copy .env.example .env
```

Buka `.env` lalu isi minimal:

```
OPENAI_API_KEY=sk-...         ← wajib untuk LLM extraction & enrichment
OPENAI_MODEL=gpt-4o-mini      ← model yang digunakan
OPENSERP_BASE_URL=http://localhost:7000
OPENSERP_ENABLED=true
```


#### 3. Setup OpenSERP (tanpa Docker)

OpenSERP adalah search engine lokal yang digunakan sebagai sumber utama pencarian.
Tidak perlu Docker — cukup download binary-nya:

1. Download file berikut:
   ```
   https://github.com/karust/openserp/releases/download/v0.7.2/openserp-windows-amd64-0.7.2.tgz
   ```

2. Extract ke folder `C:\openserp\` (atau folder mana saja)

3. Jalankan di terminal tersendiri (biarkan terbuka):
   ```cmd
   C:\openserp\openserp.exe serve -a 0.0.0.0 -p 7000
   ```

4. Verifikasi berjalan — buka browser ke `http://localhost:7000` harus tampil halaman OpenSERP

> OpenSERP harus tetap berjalan selama crawler dipakai. Gunakan `start_openserp.bat`
> supaya tidak perlu mengetik path setiap kali.


#### 4. Jalankan crawler

```cmd
venv\Scripts\activate
python run.py "cyber defense exhibition 2026"
```


---


### Contoh Perintah

```cmd
REM Run standar
python run.py "cyber defense exhibition 2026"

REM Multi region (aktif karena kata "global")
python run.py "cyber defense exhibition global 2026"

REM Region spesifik lewat keyword dalam query
python run.py "cybersecurity expo China USA Oceania 2026"

REM Testing cepat: 50 vendor, tanpa enrichment
python run.py "defense expo 2026" --max 50 --no-enrich

REM Tanpa LLM sama sekali (hanya schema.org + rule-based)
python run.py "security conference 2026" --no-llm

REM Verbose untuk debugging
python run.py "expo 2026" --verbose
```


### Flag CLI lengkap

```
query              Wajib. Teks bebas. Region dideteksi otomatis dari keyword.
--depth, -d        Override MAX_DEPTH dari .env.
--batch, -b        Override BATCH_SIZE dari .env.
--max, -m          Batas maksimum vendor. Berguna untuk testing cepat.
--no-enrich        Skip fase enrichment. Lebih cepat, data lebih sedikit.
--no-llm           Nonaktifkan semua LLM (fallback dan supervisor).
--output, -o       Override folder output.
--verbose, -v      Set LOG_LEVEL=DEBUG.
```

PENTING: flag harus di luar tanda kutip query.
```
Benar : python run.py "cyber defense 2026" --max 10
Salah : python run.py "cyber defense 2026 --max 10"
```


---


## Pipeline Crawl Detail


CrawlerAgent mengelola CrawlQueue dengan tiga set internal: queue pending, processing, visited.
URL yang sudah visited tidak bisa masuk antrian lagi. URL yang sedang diproses masuk ke
processing set. Setelah selesai, URL dipindah ke visited.

Urutan prioritas di antrian: exhibitor list pages (priority 9), seed URL (priority 10),
regular exhibitor links (priority sama dengan score, max 10).

Loop berhenti ketika antrian kosong atau total crawled mencapai batch_size dikali 3.
Batas error juga ada: jika error melebihi llm_error_threshold dikali 3, crawl dihentikan.

Setelah semua batch, vendor pages dideduplikasi berdasarkan URL, diambil 5000 teratas
berdasarkan score.


### Lifecycle event loop di crawler_agent

Tiap batch membuat event loop baru karena crawler_agent.run() berjalan synchronous
dari dalam LangGraph. Sebelum loop ditutup, sistem:
1. Panggil close_playwright() untuk terminate Chromium
2. Cancel semua asyncio task yang masih pending
3. Drain tasks dengan asyncio.gather
4. Tutup loop

Ini mencegah "Task was destroyed but it is pending" di Windows ProactorEventLoop.


---


## Error Umum dan Solusinya


### Playwright NoneType has no attribute send

Penyebab: instance Playwright dari event loop sebelumnya masih dipakai oleh loop baru.
Solusi yang sudah diimplementasi: browser disimpan per loop via `loop._crawler_pw_browser`.
Jika masih muncul, reset otomatis terjadi di outer except yang mengecek string error.


### LLM output kosong atau tidak valid

Penyebab: max_completion_tokens terlalu kecil untuk reasoning model.
gpt-5-mini menggunakan sekitar 600 token untuk internal reasoning, menyisakan 0 untuk output
jika limit dipasang 200 atau kurang.
Solusi yang sudah diimplementasi: max_completion_tokens=1000.


### Translation WARNING untuk query panjang

Penyebab: Google Translate bingung dengan query seperti "cyber defense 2026, CHINA, USA, OCEANIA".
Solusi yang sudah diimplementasi: fungsi `_extract_core_query()` memotong teks di koma pertama
sebelum dikirim ke translator. Deteksi region tetap menggunakan query penuh.


### DDG RequestError untuk region tertentu

Penyebab: DuckDuckGo redirect ke Yahoo untuk beberapa region dan request gagal.
Ini perilaku normal DDG. Error ditangkap per region dan region lain tetap berjalan.


### Tidak ada vendor ditemukan

Kemungkinan: query terlalu spesifik, DDG rate limit, semua seed URL diblokir.
Coba query lebih umum, tunggu beberapa menit, atau cek koneksi internet.


### File output tidak terbuat

Pastikan openpyxl dan pandas terinstall. Periksa permission folder output.
Sistem otomatis membuat folder output jika belum ada, tapi butuh permission write.


### UnicodeEncodeError di terminal

Terjadi saat menampilkan karakter Mandarin atau Jepang di terminal Windows.
Solusi: set environment variable sebelum run.
```
set PYTHONIOENCODING=utf-8
python run.py ...
```


---


## Bottleneck dan Penjelasan Performa


Pertanyaan umum: kenapa crawl 500 URL bisa lama?

1. Request delay per domain: 0.5 sampai 2.0 detik per request ke domain yang sama.
   Ini disengaja untuk menghindari pemblokiran IP. Tidak bisa dihilangkan.

2. Playwright overhead: setiap halaman JS-heavy butuh browser launch, page load, dan
   wait for load state. Satu halaman Playwright bisa memakan 5 sampai 15 detik.

3. Sequential region search: 14 region berjalan satu per satu. Total discover bisa
   2 sampai 5 menit untuk query global. Ini bisa dioptimasi dengan concurrent search.

4. Network latency: request ke server di Asia atau Eropa dari koneksi lokal Indonesia
   bisa 200 sampai 800ms per request bahkan sebelum parsing dimulai.

Untuk testing cepat gunakan: `--max 10 --no-enrich --depth 1`
Untuk production gunakan server dengan koneksi stabil dan IP yang bersih.


---


## Menambah Fitur Baru


### Tambah region baru

Edit REGION_MAP di `backend/tools/search_tools.py`. Format entry:
```python
(["keyword1", "keyword2"],   # keyword trigger dalam query
 "cc-ll",                    # kode region DuckDuckGo (negara-bahasa)
 "lang_code",                # kode bahasa untuk deep-translator
 "Label"),                   # nama region untuk display
```


### Tambah pola ekstraksi baru

Edit CSS_PATTERNS atau REGEX_PATTERNS di `backend/tools/extract_tools.py`.
Inspect elemen HTML website target dengan browser devtools, temukan selector yang konsisten,
tambahkan ke dict pattern dengan field yang sesuai.


### Tambah agent baru

1. Buat `backend/agents/nama_agent.py` yang inherit BaseAgent
2. Implement method `run(self, input_data: dict) -> dict`
3. Buat fungsi node di `backend/graph/nodes.py`
4. Tambah node dan edge di `backend/graph/workflow.py`
5. Tambah field baru di `backend/graph/state.py` jika perlu


### Override konfigurasi tanpa edit .env

Gunakan flag CLI atau edit langsung di `apply_overrides()` di run.py.
Untuk override programmatic bisa juga akses `get_settings()` dan ubah atributnya
sebelum graph dijalankan.


---


## Estimasi Biaya LLM per Run


Untuk run standar target 300 vendor:

```
Supervisor: 8 panggilan x 230 token    = 1.840 token
Extraction fallback: 45 panggilan x 470 token  = 21.150 token
Total estimasi: 23.000 token
Biaya gpt-4o-mini: kurang dari $0.01 per run
```

LLM hanya dipanggil untuk sekitar 10 sampai 15 persen vendor pages karena kebanyakan
website pameran modern sudah mengimplementasikan schema.org atau CSS yang konsisten.


---


## Dependency Penting


```
langchain, langchain-openai    Framework tool dan chain
langgraph                      Orkestrator workflow berbasis graph
openai, instructor             OpenAI client dengan Pydantic schema enforcement
pydantic, pydantic-settings    Data validation dan config management
httpx[http2]                   HTTP client modern dengan HTTP/2
curl_cffi                      HTTP client dengan TLS fingerprint Chrome
playwright                     Headless browser untuk halaman JS-heavy
beautifulsoup4, lxml           HTML parsing
html2text                      Konversi HTML ke teks untuk LLM input
extruct                        Parsing schema.org, JSON-LD, microdata, OpenGraph
tldextract                     Ekstrak domain dari URL
duckduckgo-search              DDG search tanpa API key
fake-useragent                 Random user agent untuk menghindari deteksi bot
deep-translator                Google Translate untuk query multi bahasa
tenacity                       Retry dengan exponential backoff
pandas, openpyxl               Export data ke Excel dan CSV
loguru                         Logging modern dengan format berwarna
rich                           Terminal output yang kaya: progress bar, tabel, panel
python-dotenv                  Load .env ke environment variables
```


---


## Perbandingan Arsitektur: LLM-Agent Crawler vs BeautifulSoup4 Crawler


### Ringkasan Cepat

| Dimensi | BeautifulSoup4 (BS4) | LLM-Agent (Proyek Ini) |
|---|---|---|
| **Paradigma** | Rule-based, deterministic | Autonomous agent, probabilistic |
| **JavaScript** | ❌ Tidak support (butuh Selenium/Playwright manual) | ✅ Native via Playwright terintegrasi |
| **Multi-bahasa** | ❌ Perlu hardcode per bahasa | ✅ Otomatis (LLM paham 50+ bahasa) |
| **Navigasi cerdas** | ❌ URL harus benar dari awal | ✅ Worker mencari halaman exhibitor sendiri jika URL salah |
| **Perubahan struktur HTML** | ❌ Rusak, harus update selector manual | ✅ Adaptif — LLM baca konten, bukan selector |
| **Kualitas ekstraksi** | ⚠️ Sebatas yang ter-tag di HTML | ✅ Kontekstual — paham "ini vendor" vs "ini menu navigasi" |
| **Pagination otomatis** | ⚠️ Harus hardcode pattern per site | ✅ detect_next_button + scroll loop + API interception |
| **Kecepatan** | ✅ Sangat cepat (pure HTTP + regex) | ⚠️ Lebih lambat karena LLM latency |
| **Biaya operasional** | ✅ Nyaris gratis (no API cost) | ⚠️ Ada biaya LLM per token |
| **Maintenance** | ❌ Tinggi — setiap site ganti layout = update kode | ✅ Rendah — tidak ada selector yang perlu di-update |
| **Skalabilitas** | ✅ Mudah horizontal scale (no state) | ✅ Paralel via asyncio worker pool |
| **Determinisme** | ✅ Output selalu sama untuk input sama | ⚠️ Non-deterministic, hasil bisa sedikit bervariasi |
| **Debugging** | ✅ Mudah — trace step by step | ⚠️ LLM reasoning sulit di-trace |
| **Setup awal** | ✅ Cepat, 50-100 baris kode | ❌ Lebih kompleks, multi-file, butuh API key |
| **Fallback saat gagal** | ❌ Error langsung, tidak ada recovery | ✅ Worker retry + partial export jika crash |
| **Klasifikasi konten** | ❌ Tidak ada — semua teks di-scrape | ✅ LLM filter: vendor vs menu vs iklan vs berita |


---


### Detail Per Dimensi


#### 1. Penanganan JavaScript & Dynamic Content

**BS4:**
```
httpx.get(url) → BeautifulSoup(html) → soup.find_all("div.exhibitor")
```
Hanya membaca HTML statis. Konten yang di-render JavaScript (React, Vue, Angular, lazy-load)
tidak terlihat. Solusi workaround biasanya menambahkan Selenium atau Playwright secara manual,
tapi itu artinya menulis dua sistem paralel.

**LLM-Agent:**
```
fetch_page(url) → Playwright headless → tunggu networkidle → ambil rendered HTML
→ LLM baca konten → ekstrak vendor
```
Playwright sudah terintegrasi di fetch_page. Worker agent otomatis pakai Playwright untuk site
yang terdeteksi JS-heavy (ciri: sedikit konten di raw HTML, banyak `<script>`).


#### 2. Penanganan Multi-Bahasa

**BS4:**
```python
# Harus hardcode per bahasa
if lang == "zh":
    name = soup.find("span", class_="company-name-cn").text
elif lang == "ru":
    name = soup.find("div", class_="company-title").text
```
Setiap bahasa baru = kode baru. Karakter CJK dan Cyrillic sering muncul garbled jika encoding
salah.

**LLM-Agent:**
LLM secara native membaca Mandarin, Rusia, Arab, Korea tanpa konfigurasi khusus. Ditambah
`deep-translator` untuk normalisasi nama ke Inggris sebelum validasi. Deteksi bahasa via
`_detect_lang()` dengan fallback CJK/Cyrillic character counting jika tag `<html lang="">` absen.


#### 3. Adaptasi terhadap Perubahan Layout Website

**BS4:**
```python
# Site ganti class dari "exhibitor-card" ke "vendor-tile" → semua break
vendors = soup.find_all("div", class_="exhibitor-card")  # ← error besok
```
Satu perubahan CSS class = scraper mati. Pemeliharaan rutin diperlukan untuk setiap site.
Event tahunan yang situsnya di-rebuild tiap tahun = rebuild scraper tiap tahun.

**LLM-Agent:**
LLM membaca semantik konten, bukan CSS selector. Kalau layout berubah tapi konten tetap
"nama perusahaan, negara, nomor booth", LLM tetap mengekstrak dengan benar.


#### 4. Klasifikasi Konten Cerdas

**BS4:**
```python
# Tidak bisa bedakan vendor vs menu vs artikel berita
all_text = [tag.text for tag in soup.find_all("li")]
# Hasilnya: ["About Us", "PT Maju Bersama", "Contact", "News", "Exhibitor Hall A"]
# Semua masuk database
```
BS4 tidak punya konsep "ini vendor" vs "ini navigasi". Memerlukan regex rules panjang yang
masih sering kecolongan.

**LLM-Agent:**
`_llm_classify_and_extract()` mengirim HTML ke LLM dengan prompt:
> "Apakah halaman ini profil vendor/exhibitor? Kalau ya, ekstrak name/country/website/category."
LLM secara kontekstual memahami perbedaan halaman profil perusahaan vs halaman about-us vs
halaman menu navigasi. Filter `_validate_vendor()` menangkap sisa false positive dengan
translated BAD_NAME_PATTERNS.


#### 5. Pagination & Navigasi

**BS4 (tipikal):**
```python
page = 1
while True:
    url = f"https://site.com/exhibitors?page={page}"
    resp = httpx.get(url)
    if resp.status_code == 404:
        break
    # parse ...
    page += 1
```
Asumsi URL pattern `?page=N`. Tidak jalan untuk:
- "Load More" button (butuh klik)
- Infinite scroll (butuh Playwright)  
- API-driven pagination (butuh intercept XHR)
- Non-standard URL pattern

**LLM-Agent:**
`detect_next_button()` → deteksi tipe pagination dari HTML.
`intercept_api_vendors()` → intercept XHR/fetch call untuk site SPA.
Worker agent loop: scroll → cek item baru → scroll lagi sampai stabil.
Semua ditangani dalam satu tool tanpa hardcode per-site.


---


### Kapan Pilih Mana?

| Use Case | Rekomendasi | Alasan |
|---|---|---|
| **Site dengan struktur stabil, sering diakses** | BS4 | Cepat, murah, deterministic |
| **Data pipeline produksi dengan SLA ketat** | BS4 + Playwright | Lebih predictable untuk monitoring |
| **Crawl event pameran global (multi-bahasa, banyak site baru)** | **LLM-Agent** ✅ | Layout berbeda tiap event, bahasa beragam |
| **One-time data collection dari 50+ domain berbeda** | **LLM-Agent** ✅ | Setup per-site tidak feasible |
| **Budget sangat terbatas, volume tinggi** | BS4 | Zero LLM cost |
| **Site yang ganti layout sering** | **LLM-Agent** ✅ | Tidak butuh maintenance selector |
| **Research / explorasi data baru** | **LLM-Agent** ✅ | Tidak perlu tahu struktur site sebelumnya |


---


### Kesimpulan Jangka Panjang

**BeautifulSoup4** adalah pilihan tepat untuk scraping satu atau beberapa site dengan
struktur yang diketahui dan stabil. Cepat dibangun, mudah di-debug, dan cost operasional
mendekati nol. Tapi untuk setiap site baru atau setiap perubahan layout, developer harus
intervensi manual.

**LLM-Agent Crawler** (proyek ini) adalah investasi yang proper untuk use case jangka panjang
di mana:
- Target site terus berubah (event tahunan, domain baru tiap cycle)
- Konten multi-bahasa tanpa pola konsisten
- Tim tidak punya kapasitas untuk maintain per-site selector
- Kualitas data lebih penting dari kecepatan raw

Trade-off utama: biaya LLM token dan kecepatan lebih lambat. Untuk volume 500-1000 vendor
per run dengan GPT-4o-mini, estimasi biaya sekitar USD 0.10-0.30 per full crawl — sangat
acceptable untuk data yang sebelumnya memerlukan kerja manual berjam-jam.

**Rekomendasi untuk proyek ini:** LLM-Agent adalah arsitektur yang lebih tepat dan lebih
reliable jangka panjang, karena target domain (expo & pameran industri global) memiliki
heterogenitas tinggi dan tidak ada standar layout yang bisa diandalkan lintas organizer.
