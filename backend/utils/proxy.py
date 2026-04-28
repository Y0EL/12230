import random
import threading
from typing import Optional
from urllib.parse import urlparse

from loguru import logger


class ProxyRotator:
    """
    Round-robin proxy rotator.
    Thread-safe. Supports SOCKS5 and HTTP proxies.
    Format URL: socks5://host:port  |  socks5://user:pass@host:port  |  http://host:port
    """

    def __init__(self, proxies: list[str], rotate: bool = True) -> None:
        self._proxies = [p.strip() for p in proxies if p.strip()]
        self._rotate = rotate
        self._idx = 0
        self._lock = threading.Lock()
        self._dead: set[str] = set()

    @property
    def enabled(self) -> bool:
        return bool(self._active)

    @property
    def _active(self) -> list[str]:
        return [p for p in self._proxies if p not in self._dead]

    def next(self) -> Optional[str]:
        active = self._active
        if not active:
            return None
        if not self._rotate:
            return active[0]
        with self._lock:
            proxy = active[self._idx % len(active)]
            self._idx += 1
        return proxy

    def random(self) -> Optional[str]:
        active = self._active
        return random.choice(active) if active else None

    def mark_dead(self, proxy: str) -> None:
        if proxy and proxy not in self._dead:
            self._dead.add(proxy)
            logger.warning(f"[PROXY] Marked dead (too many errors): {_mask(proxy)}")

    def mark_alive(self, proxy: str) -> None:
        self._dead.discard(proxy)

    def all_proxies(self) -> list[str]:
        return list(self._proxies)

    def stats(self) -> dict:
        return {
            "total": len(self._proxies),
            "active": len(self._active),
            "dead": len(self._dead),
        }


def _mask(proxy: str) -> str:
    parsed = urlparse(proxy)
    if parsed.password:
        return proxy.replace(parsed.password, "***")
    return proxy


_rotator_instance: Optional[ProxyRotator] = None
_rotator_lock = threading.Lock()


def get_proxy_rotator() -> ProxyRotator:
    global _rotator_instance
    if _rotator_instance is None:
        with _rotator_lock:
            if _rotator_instance is None:
                from backend.core.config import get_settings
                s = get_settings()
                _rotator_instance = ProxyRotator(
                    proxies=s.proxy_list if s.proxy_enabled else [],
                    rotate=s.proxy_rotate,
                )
                if _rotator_instance.enabled:
                    logger.info(
                        f"[PROXY] Rotator ready: {len(_rotator_instance.all_proxies())} proxy, "
                        f"rotate={s.proxy_rotate}"
                    )
                else:
                    logger.debug("[PROXY] Proxy dinonaktifkan, koneksi langsung")
    return _rotator_instance


def reset_proxy_rotator() -> None:
    global _rotator_instance
    with _rotator_lock:
        _rotator_instance = None
