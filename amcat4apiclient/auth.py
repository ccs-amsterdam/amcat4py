import requests
import os
from appdirs import user_cache_dir
from base64 import urlsafe_b64encode, b64encode
from cryptography.fernet import Fernet
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from datetime import datetime, timedelta
from hashlib import sha256
from json import dumps, loads
from random import getrandbits
from re import search
from requests_oauthlib import OAuth2Session
from socket import socket, AF_INET, SOCK_STREAM
from webbrowser import open as browse


def get_middlecat_token(host, callback_port=65432, refresh = "static"):
    middlecat = requests.get(f"{host}/middlecat").json()["middlecat_url"]
    auth_url = f"{middlecat}/authorize" 
    token_url = f"{middlecat}/api/token"
    pkce = pkce_challange()

    auth_params = {
        "resource": host, 
        "refresh": refresh, 
        "session_type": "api_key",
        "code_challenge_method": pkce["method"],
        "code_challenge": pkce["challenge"]
    }

    oauth = OAuth2Session(client_id="amcat4apiclient", redirect_uri=f"http://localhost:{callback_port}/")

    authorization_url, state = oauth.authorization_url(auth_url, **auth_params)

    # open a socket for callbacks
    s = socket(AF_INET, SOCK_STREAM)
    s.bind(("127.0.0.1", callback_port))
    s.listen()

    # open browser for user
    browse(authorization_url)

    # wait for get request with code
    print("Waiting for authorization in browser...")
    conn, addr = s.accept()
    conn.sendall(b"Authentication complete. Please close this page and return to Python.")

    data = conn.recv(1024).decode()
    code = search(r"code=([^&\s]+)", data).group(1)
    conn.close()

    # using the received code, make a request to get the actual token
    headers={"Accept": "application/json", "Content-Type": "application/json"}
    params={"grant_type": "authorization_code", "code": code, "code_verifier": pkce["verifier"], "state": state}
    r = requests.post(token_url, headers=headers, data=dumps(params))
    
    r.raise_for_status()
    token = r.json()
    expires_at = timedelta(seconds=token["expires_in"]) + datetime.now()
    token["expires_at"] = expires_at.strftime("%Y-%m-%dT%H:%M:%S")
    del token["expires_in"]
    cache_token(token, host)
    return token

def token_refresh(token, host):
    middlecat = requests.get(f"{host}/middlecat").json()["middlecat_url"]
    token_url = f"{middlecat}/api/token"
    auth_params = {
        "resource": host, 
        "grant_type": "refresh_token",
        "refresh_mode": token["refresh_rotate"], 
        "session_type": "api_key",
        "refresh_token": token["refresh_token"],
        "client_id": "amcat4apiclient"
    }
    headers={"Accept": "application/json", "Content-Type": "application/json"}
    r = requests.post(token_url, headers=headers, data=dumps(auth_params))
    r.raise_for_status()
    token = r.json()
    expires_at = timedelta(seconds=token["expires_in"]) + datetime.now()
    token["expires_at"] = expires_at.strftime("%Y-%m-%dT%H:%M:%S")
    del token["expires_in"]
    cache_token(token, host)
    return token

def get_password_token(host, username, password):
    r = requests.post(f"{host}/auth/token",
                      data=dict(username=username, password=password))
    r.raise_for_status()
    token = r.json()
    cache_token(token, host)
    return token


def _get_token(host, username=None, password=None):
    # check for cached token
    file_path = user_cache_dir("amcat4apiclient") + "/" + sha256(host.encode()).hexdigest()
    if os.path.exists(file_path):
        token = secret_read(file_path, host)
    else:
        if username is None or password is None:
            token = get_middlecat_token(host)
        else:
            token = get_password_token(host, username, password)
    return check_token(token, host)["access_token"]

def check_token(token, host):
    if "expires_at" in token:
        if datetime.now() + timedelta(seconds=10) > datetime.strptime(token["expires_at"], "%Y-%m-%dT%H:%M:%S"):
            token = token_refresh(token, host)
    return token


def cache_token(token, host):
    file_path = user_cache_dir("amcat4apiclient") + "/" + sha256(host.encode()).hexdigest()
    secret_write(token, file_path, host)


def secret_write(x, path, host):
    dir_path = os.path.dirname(path)
    # Create the directory if it does not exist
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    fernet = Fernet(make_key(host))
    data = fernet.encrypt(dumps(x).encode())
    with open(path, "wb") as f:
        f.write(data)


def secret_read(path, host):
    with open(path, "rb") as f:
        token_enc = f.read()
    fernet = Fernet(make_key(host))
    return loads(fernet.decrypt(token_enc).decode())


def make_key(key):
    kdf = PBKDF2HMAC(
        algorithm=sha256(),
        length=32,
        salt="supergeheim".encode(),
        iterations=5,
    )
    return urlsafe_b64encode(kdf.derive(key.encode()))


def base64_url_encode(x):

    # Encode x in base64
    x = b64encode(x).decode("utf-8")

    # Remove trailing equals signs
    x = x.rstrip("=")

    # Replace + with - and / with _
    x = x.replace("+", "-").replace("/", "_")

    return x


def pkce_challange():

    # Generate random 32-octet sequence
    verifier = getrandbits(256).to_bytes(32, byteorder="big")

    # Encode the verifier in base64
    verifier = base64_url_encode(verifier)

    # Hash the verifier using the SHA-256 algorithm
    challenge = sha256(verifier.encode("utf-8")).digest()

    # Encode the challenge in base64
    challenge = base64_url_encode(challenge)

    return {
        "verifier": verifier,
        "method": "S256",
        "challenge": challenge
    }
