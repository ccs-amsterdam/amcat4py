# amcat4py

Python client library for the [AmCAT](https://github.com/ccs-amsterdam/amcat4) API. AmCAT (Automatic Content Analysis Toolkit) is a server for document storage, full-text search, and text analysis, developed as part of the [OPTED](https://opted.eu) research project.

## Package structure

```
amcat4py/
├── __init__.py          # Exports AmcatClient
├── amcatclient.py       # Main client class
├── auth.py              # OAuth2/PKCE + API key auth, token caching
├── copy_index.py        # Utility to copy documents between indices
└── __main__.py          # CLI entry point
```

## Development

```bash
uv run python -m amcat4py          # CLI
uv run mypy amcat4py               # type check
uv run ruff check amcat4py         # lint
uv run ruff format amcat4py        # format
```

No automated test suite — manual integration tests in `test.py`, `test2.py`, `test3.py`, `testvector.py`.

## Build & publish

```bash
rm -rf dist && uv build
uv run twine upload dist/*
```

Bump `version` in `pyproject.toml` before publishing. Current version follows semver (`major.minor.patch`).

## AmCAT backend

The backend lives in `~/amcat4`. It is a FastAPI app backed by Elasticsearch. Key things to know:

- API base: `GET /api/config` returns server info including version
- Auth: API key via `X-API-Key` header (≥4.1.0), or OAuth2/PKCE via MiddleCat
- Index = a collection of documents (roughly an Elasticsearch index)
- Roles (ascending): NONE, OBSERVER, METAREADER, READER, WRITER, ADMIN

Key endpoint groups:
- `/api/index` — index CRUD
- `/api/index/{ix}/documents` — document upload/fetch/update/delete
- `/api/{ix}/query` — search + aggregation
- `/api/index/{ix}/fields` — field type definitions
- `/api/index/{ix}/users` — per-index access control
- `/api/users` — server-level user management
- `/api/api_keys` — API key management

## Key conventions in amcatclient.py

- `_request()` / `_get()` / `_post()` / `_put()` / `_delete()` — internal HTTP helpers; always use these, never `requests` directly
- `_url(url, index)` — builds endpoint URLs with optional index prefix
- `query()`, `documents()` return generators (scroll-based pagination)
- `serialize()` handles `datetime`, `date`, `set`, `numpy.float32` in JSON
- `upsert=True` on `update_document()` passes `?upsert=true` to the backend PUT endpoint, creating the document if it doesn't exist
