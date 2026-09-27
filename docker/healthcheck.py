#!/usr/bin/env python3
"""Container health check: any HTTP answer (401 included, when a web login is set) means VODgrab is up."""
import os
import sys
import urllib.error
import urllib.request

port = os.environ.get("VODGRAB_PORT", "8765")
try:
    urllib.request.urlopen("http://127.0.0.1:%s/" % port, timeout=4)
except urllib.error.HTTPError:
    pass
except Exception:
    sys.exit(1)
