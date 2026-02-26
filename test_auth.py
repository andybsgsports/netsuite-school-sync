import requests
import json
import time
import random
import string
import hmac
import hashlib
import base64
from urllib.parse import quote, urlparse

NS_ACCOUNT      = "11319665"
NS_CONSUMER_KEY = "e98e2352d40c42764b0b7e742d3e8d1a4cc327e1ddf196c572c8f309e0c6b5d8"
NS_CONSUMER_SEC = "1117321c4bce8680548628ef584ccf5179d5b6c56f40d085a0b8e9ccf5a2afc6"
NS_TOKEN_KEY    = "2f263c07d55bc9488fc9f1d18b96bc4a33fd408f3f5fba1cc3d73c91c4df48b8"
NS_TOKEN_SEC    = "10a34befd1beb67e742b4e896f96fb4d341ea7e33d8bbb239ba496b9fcb5bedc"

def make_auth(method, full_url):
    parsed = urlparse(full_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    nonce = "".join(random.choices(string.ascii_letters + string.digits, k=11))
    timestamp = str(int(time.time()))
    oauth_params = {
        "oauth_consumer_key":     NS_CONSUMER_KEY,
        "oauth_nonce":            nonce,
        "oauth_signature_method": "HMAC-SHA256",
        "oauth_timestamp":        timestamp,
        "oauth_token":            NS_TOKEN_KEY,
        "oauth_version":          "1.0",
    }
    all_params = dict(oauth_params)
    if parsed.query:
        for part in parsed.query.split("&"):
            k, v = part.split("=")
            all_params[k] = v
    sorted_params = "&".join(
        f"{quote(k, safe='')}={quote(v, safe='')}"
        for k, v in sorted(all_params.items())
    )
    base_string = "&".join([
        method.upper(),
        quote(base_url, safe=''),
        quote(sorted_params, safe='')
    ])
    signing_key = f"{quote(NS_CONSUMER_SEC, safe='')}&{quote(NS_TOKEN_SEC, safe='')}"
    sig = base64.b64encode(
        hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()
    ).decode()
    return (
        f'OAuth realm="{NS_ACCOUNT}",'
        f'oauth_consumer_key="{NS_CONSUMER_KEY}",'
        f'oauth_token="{NS_TOKEN_KEY}",'
        f'oauth_signature_method="HMAC-SHA256",'
        f'oauth_timestamp="{timestamp}",'
        f'oauth_nonce="{nonce}",'
        f'oauth_version="1.0",'
        f'oauth_signature="{quote(sig, safe="")}"'
    )

# Direct record lookup by ID
print("--- Direct Customer Lookup: ID 2217 ---")
url = "https://11319665.suitetalk.api.netsuite.com/services/rest/record/v1/customer/2217"
resp = requests.get(url, headers={
    "Authorization": make_auth("GET", url),
    "Content-Type": "application/json"
})
print(f"Status: {resp.status_code}")
print(json.dumps(resp.json(), indent=2))
