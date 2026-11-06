"""
WarnBridge version, taken from git.

WarnBridge is installed and updated as a git checkout, so the version is the
nearest release tag plus how far the checkout has moved since then:

    v1.4.0               exactly the release
    v1.4.0-3-g2195978    three commits after v1.4.0 (commit 2195978)
    v1.4.0-dirty         release, but tracked files were changed locally

Please include this string in bug reports. It is shown in the dashboard
header, in the startup log and in the reply to /help.
"""

import subprocess
from functools import lru_cache
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent


@lru_cache(maxsize=1)
def get_version() -> str:
    try:
        out = subprocess.run(
            ["git", "describe", "--tags", "--always", "--dirty"],
            cwd=REPO_DIR, capture_output=True, text=True, timeout=3,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except Exception:
        pass
    return "unbekannt"


if __name__ == "__main__":
    print(get_version())
