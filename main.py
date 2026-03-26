from __future__ import annotations

import os
import sys
from pathlib import Path

from streamlit.web import cli as stcli


APP_PATH = Path(__file__).resolve().parent / "streamlit_app.py"


def main() -> int:
    port = os.environ.get("PORT", "8501")
    host = os.environ.get("HOST", "0.0.0.0")

    sys.argv = [
        "streamlit",
        "run",
        str(APP_PATH),
        "--server.port",
        str(port),
        "--server.address",
        str(host),
        "--server.headless",
        "true",
    ]
    return stcli.main()


if __name__ == "__main__":
    raise SystemExit(main())
