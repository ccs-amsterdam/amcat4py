import requests
from requests_oauthlib import OAuth2Session
from base64 import urlsafe_b64encode
from os import urandom
from hashlib import sha256
import socket
import webbrowser
import re
import base64
import random
import hashlib
import json
from appdirs import user_cache_dir
import pyarrow.feather as feather
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.fernet import Fernet
import os
from datetime import datetime, timedelta



def get_middlecat_token(server, callback_port=65432, refresh = "static"):
    middlecat = requests.get(f"{server}/middlecat").json()["middlecat_url"]
    auth_url = f"{middlecat}/authorize" 
    token_url = f"{middlecat}/api/token"
    pkce = pkce_challange()

    auth_params = {
        "resource": server, 
        "refresh": refresh, 
        "session_type": "api_key",
        "code_challenge_method": pkce["method"],
        "code_challenge": pkce["challenge"]
    }

    oauth = OAuth2Session(client_id="amcat4apiclient", redirect_uri=f"http://localhost:{callback_port}/")

    authorization_url, state = oauth.authorization_url(auth_url, **auth_params)

    # open a socket for callbacks
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", callback_port))
    s.listen()

    # open browser for user
    webbrowser.open(authorization_url)

    # wait for get request with code
    print('Waiting for authorization in browser...')
    conn, addr = s.accept()
    conn.sendall(b'Authentication complete. Please close this page and return to Python.')

    data = conn.recv(1024).decode()
    code = re.search(r"code=([^&\s]+)", data).group(1)
    conn.close()

    # using the received code, make a request to get the actual token
    headers={"Accept": "application/json", "Content-Type": "application/json"}
    params={"grant_type": "authorization_code", "code": code, "code_verifier": pkce["verifier"], "state": state}
    r = requests.post(token_url, headers=headers, data=json.dumps(params))
    
    r.raise_for_status()
    token = r.json()
    expires_at = timedelta(seconds=token["expires_in"]) + datetime.now()
    token["expires_at"] = expires_at.strftime('%Y-%m-%dT%H:%M:%S')
    del token['expires_in']
    return token


def get_password_token(host, username, password):
    r = requests.post(f"{host}auth/token",
                      data=dict(username=username, password=password))
    r.raise_for_status()
    return r.json()


def _get_token(host, username=None, password=None):
    # check for cached token
    file_path = user_cache_dir("amcat4apiclient") + "/" + hashlib.sha256(host.encode()).hexdigest()
    if os.path.exists(file_path):
        token = secret_read(file_path, host)
    else:
        if username is None or password is None:
            token = get_middlecat_token(host)
        else:
            token = get_password_token(username, password)
    return check_token(token)


def cache_token(token, host):
    file_path = user_cache_dir("amcat4apiclient") + "/" + hashlib.sha256(host.encode()).hexdigest()
    secret_write(token, file_path, host)


def secret_write(x, path, host):
    dir_path = os.path.dirname(path)
    # Create the directory if it does not exist
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    fernet = Fernet(make_key(host))
    data = fernet.encrypt(json.dumps(x).encode())
    with open(path, 'wb') as f:
        f.write(data)


def secret_read(path, host):
    with open(path, 'rb') as f:
        token_enc = f.read()
    fernet = Fernet(make_key(host))
    return fernet.decrypt(token_enc)


def make_key(key):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt="supergeheim".encode(),
        iterations=5,
    )
    return base64.urlsafe_b64encode(kdf.derive(key.encode()))


def base64_url_encode(x):

    # Encode x in base64
    x = base64.b64encode(x).decode('utf-8')

    # Remove trailing equals signs
    x = x.rstrip('=')

    # Replace + with - and / with _
    x = x.replace('+', '-').replace('/', '_')

    return x


def pkce_challange():

    # Generate random 32-octet sequence
    verifier = random.getrandbits(256).to_bytes(32, byteorder='big')

    # Encode the verifier in base64
    verifier = base64_url_encode(verifier)

    # Hash the verifier using the SHA-256 algorithm
    challenge = hashlib.sha256(verifier.encode('utf-8')).digest()

    # Encode the challenge in base64
    challenge = base64_url_encode(challenge)

    return {
        "verifier": verifier,
        "method": "S256",
        "challenge": challenge
    }
