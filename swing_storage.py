"""Private report storage. Service-role writes; reads go through authenticated Next server."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from uuid import uuid4

from storage3.exceptions import StorageApiError

BUCKET = "swing-4h"


def missing(error):
    return str(error.status) == "404" or error.code in ("not_found", "NoSuchKey", "NoSuchBucket")


def ensure_bucket(sb):
    try:
        bucket = sb.storage.get_bucket(BUCKET)
    except StorageApiError as error:
        if not missing(error):
            raise
        sb.storage.create_bucket(BUCKET, options={"public": False})
        bucket = sb.storage.get_bucket(BUCKET)
    public = bucket.get("public") if isinstance(bucket, dict) else bucket.public
    if public:
        raise RuntimeError("Swing storage must be private")
    return sb.storage.from_(BUCKET)


def read_json(storage, path):
    try:
        return json.loads(storage.download(path))
    except StorageApiError as error:
        if missing(error):
            return None
        raise


def write_json(storage, path, value, *, replace=False):
    data = json.dumps(value, allow_nan=False, separators=(",", ":")).encode()
    storage.upload(path, data, file_options={"content-type": "application/json",
                                           "cache-control": "0",
                                           "upsert": "true" if replace else "false"})


@contextmanager
def paper_lock(storage):
    # Storage's create-if-absent gives a cross-process lock. Never steal a lock:
    # a crashed owner requires inspection/removal of this single lock object.
    token = uuid4().hex
    path = "paper/write-lock.json"
    try:
        write_json(storage, path, {"owner": token, "acquired_at": datetime.now(timezone.utc).isoformat()})
    except StorageApiError as error:
        if str(error.status) == "409" or error.code in ("Duplicate", "ResourceAlreadyExists"):
            raise RuntimeError("Another swing paper writer owns the lock") from None
        raise
    try:
        yield
    finally:
        saved = read_json(storage, path)
        if saved and saved.get("owner") == token:
            storage.remove([path])
