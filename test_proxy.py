#!/usr/bin/env python3
"""
Test proxy connectivity dan IP rotation.
Jalankan: python test_proxy.py

Setup cepat (single proxy via SSH tunnel):
  ssh -D 1081 -N -f user@server.example.com
  Lalu set .env: PROXY_ENABLED=true  PROXY_LIST=socks5://127.0.0.1:1081

Setup multi-proxy via HAProxy (lihat haproxy.cfg):
  ssh -D 1081 -N -f user@server1.example.com
  ssh -D 1082 -N -f user@server2.example.com
  haproxy -f haproxy.cfg -D
  Lalu set .env: PROXY_ENABLED=true  PROXY_LIST=socks5://127.0.0.1:1080
"""
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import httpx
from rich.console import Console
from rich.table import Table
from rich import box

console = Console()

IP_CHECK_URLS = [
    "https://api.ipify.org?format=json",
    "https://httpbin.org/ip",
]
TIMEOUT = 15


def _parse_ip(text: str) -> str:
    text = text.strip()
    if text.startswith("{"):
        try:
            j = json.loads(text)
            return j.get("ip") or j.get("origin") or text
        except Exception:
            pass
    m = re.search(r"(\d{1,3}\.){3}\d{1,3}", text)
    return m.group(0) if m else text[:40]


async def get_ip(proxy: str = None) -> tuple[str, float]:
    start = time.time()
    kwargs = {"timeout": TIMEOUT}
    if proxy:
        kwargs["proxy"] = proxy
    for url in IP_CHECK_URLS:
        try:
            async with httpx.AsyncClient(**kwargs) as client:
                r = await client.get(url)
                return _parse_ip(r.text), time.time() - start
        except Exception as e:
            last_err = str(e)
    return f"GAGAL: {last_err}", time.time() - start


async def test_curl_cffi(proxy: str) -> tuple[str, float]:
    start = time.time()
    try:
        from curl_cffi.requests import AsyncSession
        proxy_dict = {"http": proxy, "https": proxy}
        async with AsyncSession(impersonate="chrome120") as s:
            r = await s.get(IP_CHECK_URLS[0], proxies=proxy_dict, timeout=TIMEOUT)
        return _parse_ip(r.text), time.time() - start
    except ImportError:
        return "curl_cffi tidak terinstall", 0.0
    except Exception as e:
        return f"GAGAL: {e}", time.time() - start


async def test_playwright(proxy: str) -> tuple[str, float]:
    start = time.time()
    try:
        from playwright.async_api import async_playwright
        parsed = urlparse(proxy)
        pw_proxy = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
        if parsed.username:
            pw_proxy["username"] = parsed.username
        if parsed.password:
            pw_proxy["password"] = parsed.password

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(proxy=pw_proxy)
            page = await ctx.new_page()
            await page.goto(IP_CHECK_URLS[0], timeout=TIMEOUT * 1000)
            content = await page.content()
            await ctx.close()
            await browser.close()
        return _parse_ip(content), time.time() - start
    except ImportError:
        return "playwright tidak terinstall", 0.0
    except Exception as e:
        return f"GAGAL: {e}", time.time() - start


async def check_haproxy_stats() -> None:
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            r = await client.get("http://127.0.0.1:8404/stats", auth=("admin", "crawlerbot"))
        if r.status_code == 200:
            console.print("[green]HAProxy stats page aktif[/green] di http://127.0.0.1:8404/stats")
        else:
            console.print(f"[yellow]HAProxy stats merespons HTTP {r.status_code}[/yellow]")
    except Exception:
        console.print("[dim]HAProxy stats tidak aktif (haproxy -f haproxy.cfg belum dijalankan)[/dim]")


async def main() -> None:
    console.print("\n[bold]MEGA CRAWLER — Proxy Test[/bold]\n")

    await check_haproxy_stats()

    ip_direct, _ = await get_ip()
    console.print(f"\nIP tanpa proxy : [bold yellow]{ip_direct}[/bold yellow]")

    from backend.core.config import get_settings
    settings = get_settings()

    if settings.proxy_enabled and settings.proxy_list:
        proxies = settings.proxy_list
        console.print(f"Proxy dari .env ({len(proxies)} entri):")
        for p in proxies:
            console.print(f"  {p}")
    else:
        console.print("\n[yellow]PROXY_ENABLED=false atau PROXY_LIST kosong, pakai default test[/yellow]")
        proxies = ["socks5://127.0.0.1:1080", "socks5://127.0.0.1:1081"]
        console.print(f"Mencoba default: {proxies}\n")

    results = []

    for proxy_url in proxies:
        console.rule(f"[bold]{proxy_url}[/bold]")
        httpx_ip, httpx_t = await get_ip(proxy_url)
        curl_ip, curl_t = await test_curl_cffi(proxy_url)
        pw_ip, pw_t = await test_playwright(proxy_url)

        table = Table(box=box.SIMPLE)
        table.add_column("Method", style="cyan")
        table.add_column("IP Keluar")
        table.add_column("Latency")
        table.add_column("Rotasi?")

        for label, ip, t in [("httpx", httpx_ip, httpx_t), ("curl_cffi", curl_ip, curl_t), ("playwright", pw_ip, pw_t)]:
            ok = not ip.startswith("GAGAL") and not ip.startswith("tidak")
            rotated = ok and ip != ip_direct
            rot_label = "[green]YA (IP beda)[/green]" if rotated else ("[yellow]TIDAK (IP sama)[/yellow]" if ok else "[red]GAGAL[/red]")
            table.add_row(label, ip, f"{t:.2f}s", rot_label)

        console.print(table)
        if not httpx_ip.startswith("GAGAL"):
            results.append((proxy_url, httpx_ip))

    if len(results) >= 2:
        console.rule("Ringkasan Rotasi")
        unique_ips = {ip for _, ip in results}
        console.print(f"Proxy aktif : {len(results)}")
        console.print(f"IP unik     : {len(unique_ips)}")
        if len(unique_ips) > 1:
            console.print("[bold green]Rotasi IP berhasil.[/bold green]")
        else:
            console.print("[yellow]Semua proxy keluar dari IP yang sama.[/yellow]")
            console.print("Pastikan tiap SSH tunnel terhubung ke server yang berbeda.")

    console.print("""
[bold]Cara setup IP rotation:[/bold]

  1. SSH tunnel ke beberapa server berbeda:
       ssh -D 1081 -N -f user@server1.com
       ssh -D 1082 -N -f user@server2.com

  2. Jalankan HAProxy (lihat haproxy.cfg):
       haproxy -f haproxy.cfg -D

  3. Set .env:
       PROXY_ENABLED=true
       PROXY_LIST=socks5://127.0.0.1:1080

  4. Jalankan crawler seperti biasa:
       python run.py "cyber defense 2026"
""")


if __name__ == "__main__":
    asyncio.run(main())
