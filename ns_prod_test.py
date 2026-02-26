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
NS_CONSUMER_KEY = "d3338f58e9f6255cb8c860fd201d6eadf43accb65914a31936dd7a52a005aee0"
NS_CONSUMER_SEC = "c82eef5c4de9b9499dbc8f62756623d360b59cf564e4212e3dd1422002d3b9fc"
NS_TOKEN_KEY    = "f17d690c078ece2f4ecf11068f53d8d2ca644c051e2cd37098516122e1e27a5f"
NS_TOKEN_SEC    = "e281e328dea1b8a1da991199882cc283f06dd71d167aaf87348356e2bb7eda64"

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
