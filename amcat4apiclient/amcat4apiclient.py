import logging
from datetime import datetime, date, time
from json import dumps
from typing import List, Iterable, Optional, Union, Dict, Sequence

import requests
from requests import HTTPError
from amcat4apiclient.auth import _get_token, _check_token


def serialize(obj):
    """JSON serializer that accepts datetime & date"""
    if isinstance(obj, date) and not isinstance(obj, datetime):
        obj = datetime.combine(obj, time.min)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return sorted(obj)


class AmcatClient:
    def __init__(self, host, username=None, password=None, force_refresh=False, ignore_tz=True):
        self.host = host
        self.username = username
        self.password = password
        self.ignore_tz = ignore_tz
        # run at init to cache token
        self.token = _get_token(self.host, self.username, self.password, force_refresh)

    @staticmethod
    def _chunks(items: Iterable, chunk_size=100) -> Iterable[List]:
        """ utility method for uploading documents in batches """
        buffer = []
        for item in items:
            buffer.append(item)
            if len(buffer) > chunk_size:
                yield buffer
                buffer = []
        if buffer:
            yield buffer

    def _url(self, url=None, index=None):
        url_parts = [self.host] + (["index", index]
                                   if index else []) + ([url] if url else [])
        return "/".join(url_parts)

    def _request(self, method, url=None, ignore_status=None, headers=None, **kargs):
        if headers is None:
            headers = {}
        self.token = _check_token(self.token, self.host)
        headers['Authorization'] = f"Bearer {self.token['access_token']}"
        r = requests.request(method, url, headers=headers, **kargs)
        if not (ignore_status and r.status_code in ignore_status):
            try:
                r.raise_for_status()
            except HTTPError:
                if r.text:
                    logging.error(f"Response body: {r.text}")
                raise
        return r

    def _get(self, url=None, index=None, params=None, ignore_status=None):
        return self._request("get", url=self._url(url, index), params=params, ignore_status=ignore_status)

    def _post(self, url=None, index=None, json=None, ignore_status=None):
        if json:
            data = dumps(json, default=serialize)
            headers = {'Content-Type': 'application/json'}
        else:
            data = None
            headers = {}

        return self._request("post", url=self._url(url, index), data=data, headers=headers, ignore_status=ignore_status)

    def _put(self, url=None, index=None, json=None, ignore_status=None):
        return self._request("put", url=self._url(url, index), json=json, ignore_status=ignore_status)

    def _delete(self, url=None, index=None, ignore_status=None):
        return self._request("delete", url=self._url(url, index), ignore_status=ignore_status)

    def list_indices(self) -> List[dict]:
        """
        List all indices on this server
        :return: a list of index dicts with keys name and (your) role
        """
        return self._get("index/").json()

    def list_index_users(self, index: str) -> List[dict]:
        """
        List users and their roles in an index
        """
        return self._get(f"index/{index}/users").json()

    def documents(self, index: str, q: Optional[str] = None, *,
                  fields=('date', 'title', 'url'), scroll='2m', per_page=100, **params) -> Iterable[dict]:
        """
        Perform a query on this server, scrolling over the results to get all hits

        :param index: The name of the index
        :param q: An optional query
        :param fields: A list of fields to retrieve (use None for all fields, '_id' for id only)
        :param scroll: type to keep scroll cursor alive
        :param per_page: Number of results per page
        :param params: Any other parameters passed as query arguments
        :return: an iterator over the found documents with the requested (or all) fields
        """
        params['scroll'] = scroll
        params['per_page'] = per_page
        if fields:
            params['fields'] = ",".join(fields)
        if q:
            params['q'] = q
        while True:
            r = self._get("documents", index=index, params=params, ignore_status=[404])
            if r.status_code == 404:
                break
            d = r.json()
            yield from d['results']
            params['scroll_id'] = d['meta']['scroll_id']

    def query(self, index: str, *,
              scroll='2m', per_page=100,
              sort: Union[str, dict, list] = None,
              fields: Sequence[str] = ('date', 'title', 'url'),
              queries: Union[str, list, dict] = None,
              filters: Dict[str, Union[str, list, dict]] = None,
              date_fields: Sequence[str] = ('date',)):
        body = dict(filters=filters, queries=queries, fields=fields, sort=sort,
                    scroll=scroll, per_page=per_page)
        body = {k: v for (k, v) in body.items() if v is not None}
        while True:
            r = self._post("query", index=index, json=body, ignore_status=[404])
            if r.status_code == 404:
                break
            d = r.json()
            for res in d['results']:
                for date_field in date_fields:
                    if res.get(date_field):
                        date = res[date_field][:10] if self.ignore_tz else res[date_field]
                        res[date_field] = datetime.fromisoformat(date)
                yield res
            body['scroll_id'] = d['meta']['scroll_id']

    def create_index(self, index: str, guest_role: Optional[str] = None):
        body = {"name": index}
        if guest_role:
            body['guest_role'] = guest_role
        return self._post("index/", json=body).json()

    def create_user(self, email, password, global_role=None, index_access=None, credentials=None):
        """
        Create a new user (superfluous after refactor_nodb/Jan 6, 2023)
        :param email: Email address of the new user to add
        :param password: new password for the user
        :param global_role: global role of the user ("writer" or "admin")
        :param index_access: index to grant access to for the new user
        :param credentials: The credentials to use. If not given, uses last login information
        """
        body = {
            "email": email,
            "password": password,
            "global_role": global_role,
            "index_access": index_access
        }
        return self._post("users/", json=body).json()

    def add_index_user(self, index: str, email: str, role: str):
        """
        add new user to an index
        :param index: name of the index
        :param email: Email address of the user to add
        :param role: role of the user for this index. One of "admin", "writer", "reader" "metareader".
        """
        body = {
          "email": email,
          "role": role.upper()
        }
        return self._post(f"index/{index}/users", json=body).json()

    def modify_index_user(self, index: str, email: str, role: str):
        """
        modify user role for index
        :param index: name of the index
        :param email: The email of an (existing) user
        :param role: role of the user for this index. One of "admin", "writer", "reader" "metareader".
        """
        body = {"role": role.upper()}
        return self._put(f"index/{index}/users/{email}", json=body).json()

    def check_index(self, ix: str) -> Optional[dict]:
        r = self._get(index=ix, ignore_status=[404])
        if r.status_code == 404:
            return None
        return r.json()

    def delete_index(self, index: str) -> bool:
        r = self._delete(index=index, ignore_status=[404])
        return r.status_code != 404

    def delete_index_user(self, index: str, email: str) -> bool:
        """
        delete user from an index
        :param index: name of the index
        :param email: The email of an (existing) user
        """
        r = self._delete(f"index/{index}/users/{email}", ignore_status=[404])
        return r.status_code != 404

    def upload_documents(self, index: str, articles: Iterable[dict], columns: dict = None,
                         chunk_size=100, show_progress=False) -> None:
        """
        Upload documents to the server. First argument is the name of the index where the new documents should be inserted.
        Second argument is an iterable (e.g., a list) of dictionaries. Each dictionary represents a single document.
        Required keys are: `title`, `text`, `date`, and `url`.
        You can optionally specify the column types with a dictionary.
        By default, the articles are uploaded in chunks of 100 documents. You can adjust this accordingly.
        For larger uploads, you have the option to show a progress bar (make sure tqdm is installed).

        :param index: The name of the index
        :param articles: Documents to upload (a list of dictionaries)
        :param columns: an optional dictionary of field types.
        :param chunk_size: number of documents to upload per batch (default: 100)
        :param show_progress: show a progress bar when uploading documents (default: False)
        """
        body = {}
        if columns:
            body['columns'] = columns

        if show_progress:
            from tqdm import tqdm
            import math
            generator = tqdm(self._chunks(articles, chunk_size=chunk_size),
                             total=math.ceil(len(articles) / chunk_size), unit="chunks")
        else:
            generator = self._chunks(articles, chunk_size=chunk_size)
        for chunk in generator:
            body = {"documents": chunk}
            self._post("documents", index=index, json=body)

    def update_document(self, index: str, doc_id, body):
        self._put(f"documents/{doc_id}", index, json=body)

    def get_document(self, index: str, doc_id):
        return self._get(f"documents/{doc_id}", index).json()

    def delete_document(self, index: str, doc_id):
        self._delete(f"documents/{doc_id}", index)

    def set_fields(self, index: str, body):
        self._post("fields", index, json=body)

    def get_fields(self, index: str):
        return self._get("fields", index).json()
