import logging
from typing import Optional, Sequence, Iterable, List

from amcat4py import AmcatClient


def chunks(items: Iterable, chunk_size=100) -> Iterable[List]:
    buffer = []
    for item in items:
        buffer.append(item)
        if len(buffer) > chunk_size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


def copy_documents(src: AmcatClient, src_index: str, dest: AmcatClient, dest_index: str,
                   ignore_fields: Optional[Sequence[str]] = None):
    if ignore_fields is None:
        ignore_fields = ()
    logging.info(f"Copying from {src.host}/index/{src_index} to {dest.host}/index/{dest_index}")

    # Copy field types
    src_fields = src.get_fields(src_index)
    dest_fields = dest.get_fields(dest_index)
    # TODO: include metadata (but API doesn't like that)
    to_add = {k: v['type'] for (k, v) in src_fields.items() if k not in dest_fields and k not in ignore_fields}
    if to_add:
        logging.info(f"Adding fields {list(to_add.keys())}")
        dest.set_fields(dest_index, to_add)

    # Copy articles
    urls = {a['url'] for a in dest.query(dest_index, fields=["url"])}
    logging.info(f"Found {len(urls)} urls in {dest.host}/index/{dest_index}")
    fields = [f for f in src_fields.keys() if f not in ignore_fields]
    docs = (d for d in src.documents(src_index, scroll="10m", fields=fields) if d['url'] not in urls)
    for page in chunks(docs):
        for doc in page:
            del doc['_id']
        logging.info(f"Uploading {len(page)} documents...")
        dest.upload_documents(dest_index, page)
