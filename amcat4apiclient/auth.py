import logging

import requests
import os
from appdirs import user_cache_dir
from base64 import urlsafe_b64encode, b64encode
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

FELINE_RESPONSE = """HTTP/1.1 200 OK
Content-Type: text/html; charset=utf-8

<html><body><pre>Authorization complete, you can close this tab and return to Python.
 ,_     _
 |\\\\_,-~/
 / _  _ |    ,--.
(  @  @ )   / ,-'
 \\  _T_/-._( (
 /         `. \\
|         _  \\ |
 \\ \\ ,  /      |
  || |-_\\__   /
 ((_/`(____,-'
</pre></body></html>
"""


def get_middlecat_token(host, callback_port=65432, refresh="static"):
    """
    Authenticate to an AmCAT instance using a middlecat instance (which is automatically retrieved from the AmCAT instance)
    :param host: The URL to the AmCAT instance (e.g. "http://localhost/api")
    :param callback_port: Port used to receive the token. The only reason to change this is if the port is already in use.
    :param refresh: Either "refresh" or "static" to en-/disable token rotation.
    """
    # we open a socket and browser for for the interactive authentication and wait for the code from middlecat
    # We do this first so we can change the port if needed
    s = socket(AF_INET, SOCK_STREAM)
    while True:
        try:
            s.bind(("127.0.0.1", callback_port))
        except OSError:
            logging.info(f"Port {callback_port} already in use, trying {callback_port-1}")
            callback_port -= 1
        else:
            break
    s.listen()

    middlecat = requests.get(f"{host}/middlecat").json()["middlecat_url"]
    auth_url = f"{middlecat}/authorize"
    token_url = f"{middlecat}/api/token"
    pkce = pkce_challange()

    auth_params = {
        "resource": host,
        "refresh_mode": refresh,
        "session_type": "api_key",
        "code_challenge_method": pkce["method"],
        "code_challenge": pkce["challenge"]
    }

    oauth = OAuth2Session(client_id="amcat4apiclient", redirect_uri=f"http://localhost:{callback_port}/")

    authorization_url, state = oauth.authorization_url(auth_url, **auth_params)

    browse(authorization_url)
    conn, addr = s.accept()

    conn.sendall(FELINE_RESPONSE.encode("ascii"))

    data = conn.recv(1024).decode()
    code = search(r"code=([^&\s]+)", data).group(1)
    conn.close()

    # using the received code, make a request to get the actual token
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    params = {"grant_type": "authorization_code", "code": code, "code_verifier": pkce["verifier"], "state": state}
    r = requests.post(token_url, headers=headers, data=dumps(params))

    r.raise_for_status()
    token = r.json()
    expires_at = timedelta(seconds=token["expires_in"]) + datetime.now()
    token["expires_at"] = expires_at.strftime("%Y-%m-%dT%H:%M:%S")
    del token["expires_in"]
    cache_token(token, host)
    return token


def token_refresh(token, host):
    """
    Usually called by _check_token if token has expired
    :param token: old token
    :param host: The URL to the AmCAT instance (e.g. "http://localhost/api").
    """
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
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    r = requests.post(token_url, headers=headers, data=dumps(auth_params))
    r.raise_for_status()
    token = r.json()
    expires_at = timedelta(seconds=token["expires_in"]) + datetime.now()
    token["expires_at"] = expires_at.strftime("%Y-%m-%dT%H:%M:%S")
    del token["expires_in"]
    cache_token(token, host)
    return token


def get_password_token(host, username, password):
    """
    The old/alternative route to receive a token, i.e. via a username and password.
    :param host: The URL to the AmCAT instance (e.g. "http://localhost/api").
    :param username: username, usually and email address
    :param password: usern password
    """
    r = requests.post(f"{host}/auth/token",
                      data=dict(username=username, password=password))
    r.raise_for_status()
    token = r.json()
    cache_token(token, host)
    return token


def _get_token(host, username=None, password=None, force_refresh=False):
    """
    Returns refreshed token if old token has expired
    :param host: The URL to the AmCAT instance (e.g. "http://localhost/api").
    :param username,password: optionally provide a username and password to trigger get_password_token()
        instead of get_middlecat_token()
    :param force_refresh: when True, overwrites the cached token and creates a new one
    """
    file_path = user_cache_dir("amcat4apiclient") + "/" + sha256(host.encode()).hexdigest()
    if os.path.exists(file_path) and not force_refresh:
        token = secret_read(file_path, host)
    else:
        if username is None or password is None:
            token = get_middlecat_token(host)
        else:
            token = get_password_token(host, username, password)
    return _check_token(token, host)


def _check_token(token, host):
    """
    Returns refreshed token if old token has expired
    :param token: old token
    :param host: The URL to the AmCAT instance (e.g. "http://localhost/api").
    """
    if "expires_at" in token:
        if datetime.now() + timedelta(seconds=10) > datetime.strptime(token["expires_at"], "%Y-%m-%dT%H:%M:%S"):
            token = token_refresh(token, host)
    return token


def cache_token(token, host):
    """
    Caches encrypted token on disk
    :param token: old token
    :param host: The URL to the AmCAT instance (e.g. "http://localhost/api").
    """
    file_path = user_cache_dir("amcat4apiclient") + "/" + sha256(host.encode()).hexdigest()
    dir_path = os.path.dirname(file_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    fernet = Fernet(make_key(host))
    data = fernet.encrypt(dumps(token).encode())
    with open(file_path, "wb") as f:
        f.write(data)


def secret_read(path, host):
    """
    Reads encrypted token from disk
    :param path: path to file, usually in user_cache_dir("amcat4apiclient")
    :param host: The URL to the AmCAT instance (e.g. "http://localhost/api").
    """
    with open(path, "rb") as f:
        token_enc = f.read()
    fernet = Fernet(make_key(host))
    return loads(fernet.decrypt(token_enc).decode())


def make_key(key):
    """
    Helper function to make key for encryption of tokens
    :param key: string that is turned into key.
    """
    kdf = PBKDF2HMAC(
        algorithm=sha256(),
        length=32,
        salt="supergeheim".encode(),
        iterations=5,
    )
    return urlsafe_b64encode(kdf.derive(key.encode()))


def base64_url_encode(x):
    """
    Custom base64 encode for pkce challange nicked from httr2
    https://github.com/r-lib/httr2/blob/main/R/utils.R
    :param x: string to be encoded.
    """
    x = b64encode(x).decode("utf-8")
    # Replace some characters to align output with the javascript version
    x = x.rstrip("=")
    x = x.replace("+", "-").replace("/", "_")
    return x


def pkce_challange():
    """
    Generates PKCE code challange for middlecat requests
    :param x: string to be encoded.
    """
    # Generate random 32-octet sequence
    verifier = getrandbits(256).to_bytes(32, byteorder="big")
    verifier = base64_url_encode(verifier)
    challenge = sha256(verifier.encode("utf-8")).digest()
    challenge = base64_url_encode(challenge)
    return {
        "verifier": verifier,
        "method": "S256",
        "challenge": challenge
    }
