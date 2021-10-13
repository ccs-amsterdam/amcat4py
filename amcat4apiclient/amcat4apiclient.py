from typing import List, Iterable, Optional

import requests

class AmcatClient:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def list_indices(self) -> List[dict]:
        """
        List all indices on this server
        :return: a list of index dicts with keys name and (your) role
        """
        url = f"{self.host}/index/"
        r = requests.get(url, auth=(self.username, self.password))
        r.raise_for_status()
        return r.json()

    def query(self, index: str, q: Optional[str]= None, *,
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
        url = f"{self.host}/index/{index}/query"
        params['scroll'] = scroll
        params['per_page'] = per_page
        if fields:
            params['fields'] = ",".join(fields)
        if q:
            params['q'] = q
        while True:

            print(url)

            r = requests.get(url, auth=(self.username, self.password), params=params)
            if r.status_code == 404:
                break
            r.raise_for_status()
            d = r.json()
            yield from d['results']
            params['scroll_id'] = d['meta']['scroll_id']


    def upload(self, index: str, documents: list):
        """
        Upload a set of documents to the server

        :param index: The name of the index
        :param documents: A list of dictionaries with at least the keys date, title, text
        :return: response of the POST request to the server
        """
        url = f"{self.host}/index/{index}/documents"
        r = requests.post(url, auth=(self.username, self.password), json=documents)
        return r 
