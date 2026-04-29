import json
import os
import subprocess
import sys
import tempfile

from loguru import logger

_WRAPPER_TEMPLATE = '''\
import json
import sys
import re
import os

try:
    from bs4 import BeautifulSoup
except ImportError:
    pass

{user_code}

if __name__ == "__main__":
    html_path = sys.argv[1]
    with open(html_path, "r", encoding="utf-8", errors="replace") as _f:
        _html = _f.read()
    try:
        _result = parse(_html)
        if not isinstance(_result, list):
            _result = []
        print(json.dumps(_result, ensure_ascii=False))
    except Exception as _e:
        import traceback
        print(json.dumps({{"__error__": str(_e), "__trace__": traceback.format_exc()[-800:]}}), file=sys.stderr)
        sys.exit(1)
'''


class SafeExecutor:
    def run(
        self,
        code: str,
        html: str,
        url: str,
        timeout: int = 30,
    ) -> list[dict]:
        wrapped = _WRAPPER_TEMPLATE.format(user_code=code)

        html_tmp = code_tmp = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".html", encoding="utf-8", delete=False
            ) as f:
                f.write(html)
                html_tmp = f.name

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", encoding="utf-8", delete=False
            ) as f:
                f.write(wrapped)
                code_tmp = f.name

            proc = subprocess.run(
                [sys.executable, code_tmp, html_tmp],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=tempfile.gettempdir(),
            )

            if proc.returncode != 0:
                err = (proc.stderr or "").strip()[:600]
                raise RuntimeError(f"Parser exited {proc.returncode}: {err}")

            stdout = (proc.stdout or "").strip()
            if not stdout:
                return []

            data = json.loads(stdout)
            if isinstance(data, dict) and "__error__" in data:
                raise RuntimeError(data["__error__"])

            result = data if isinstance(data, list) else []
            logger.debug(f"[EXECUTOR] {url} -> {len(result)} records")
            return result

        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Parser timed out after {timeout}s")
        finally:
            for p in (html_tmp, code_tmp):
                if p:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass
