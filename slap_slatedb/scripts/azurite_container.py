"""Creates a blob container in Azurite, for the :azure tests.

    python3 scripts/azurite_container.py slatedb

Uses Azurite's well-known development account and key.
"""
import base64, hashlib, hmac, sys, urllib.request
from email.utils import formatdate
account = "devstoreaccount1"
key = base64.b64decode("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
container = sys.argv[1]
date = formatdate(usegmt=True)
version = "2021-08-06"
canon_headers = f"x-ms-date:{date}\nx-ms-version:{version}\n"
canon_resource = f"/{account}/{account}/{container}\nrestype:container"
to_sign = "PUT\n\n\n\n\n\n\n\n\n\n\n\n" + canon_headers + canon_resource
sig = base64.b64encode(hmac.new(key, to_sign.encode(), hashlib.sha256).digest()).decode()
req = urllib.request.Request(f"http://127.0.0.1:10000/{account}/{container}?restype=container", method="PUT",
    headers={"x-ms-date": date, "x-ms-version": version, "Authorization": f"SharedKey {account}:{sig}", "Content-Length": "0"})
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    print(opener.open(req).status)
except urllib.error.HTTPError as e:
    print(e.code, e.read()[:200])
