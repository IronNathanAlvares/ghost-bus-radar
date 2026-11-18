"""The raw archive: a local directory or an S3 bucket behind one small interface.

Locally the archive is a plain directory on the host (outside OneDrive and outside any Docker
volume, so ``docker compose down -v`` cannot delete collected history). In AWS it is an S3 bucket.
Keys are identical in both (``ghostbus_core.archive_layout``), so moving to the cloud is
``aws s3 sync <dir> s3://<bucket>``.

Configure with ``ARCHIVE_URI``: ``file:///archive`` or ``s3://bucket[/prefix]``, plus
``ARCHIVE_S3_ENDPOINT`` for an S3-compatible store such as the local SeaweedFS.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Protocol
from urllib.parse import urlparse

import zstandard

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

#: zstd level for snapshots. Measured in docs/measurements/01-feed-profile.md.
ZSTD_LEVEL = 9


def compress(data: bytes) -> bytes:
    return zstandard.ZstdCompressor(level=ZSTD_LEVEL).compress(data)


def decompress(data: bytes) -> bytes:
    return zstandard.ZstdDecompressor().decompress(data)


class Archive(Protocol):
    uri: str

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None: ...
    def put_file(self, key: str, path: Path, content_type: str) -> None: ...
    def get_bytes(self, key: str) -> bytes | None: ...
    def exists(self, key: str) -> bool: ...
    def list_keys(self, prefix: str) -> Iterator[str]: ...
    def create_if_absent(self, key: str, data: bytes) -> bool:
        """Write ``key`` only if it does not exist. True for exactly one of any racing writers."""
        ...


class LocalArchive:
    """A directory. Writes are atomic (temp file then rename), so readers never see half a file."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.uri = root.as_uri()

    def _path(self, key: str) -> Path:
        if key.startswith("/") or ".." in key.split("/"):
            raise ValueError(f"unsafe key {key!r}")
        return self.root / key

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            Path(tmp).replace(path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def put_file(self, key: str, path: Path, content_type: str) -> None:
        dest = self._path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(f".tmp-{dest.name}")
        shutil.copyfile(path, tmp)
        tmp.replace(dest)

    def get_bytes(self, key: str) -> bytes | None:
        path = self._path(key)
        return path.read_bytes() if path.is_file() else None

    def create_if_absent(self, key: str, data: bytes) -> bool:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as f:  # O_CREAT | O_EXCL
                f.write(data)
        except FileExistsError:
            return False
        return True

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str) -> Iterator[str]:
        base = self._path(prefix.rstrip("/")) if prefix else self.root
        if not base.exists():
            return
        for p in sorted(base.rglob("*")):
            if p.is_file() and not p.name.startswith(".tmp-"):
                key = p.relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    yield key


class S3Archive:
    def __init__(self, bucket: str, prefix: str, client: S3Client) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self.client = client
        self.uri = f"s3://{bucket}/{self.prefix}"

    def _key(self, key: str) -> str:
        return self.prefix + key

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=self._key(key), Body=data, ContentType=content_type
        )

    def put_file(self, key: str, path: Path, content_type: str) -> None:
        self.client.upload_file(
            str(path), self.bucket, self._key(key), ExtraArgs={"ContentType": content_type}
        )

    def get_bytes(self, key: str) -> bytes | None:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        except self.client.exceptions.NoSuchKey:
            return None
        return obj["Body"].read()

    def create_if_absent(self, key: str, data: bytes) -> bool:
        from botocore.exceptions import ClientError

        try:  # S3 conditional write: fails with 412 if the key exists, 409 on a race in flight
            self.client.put_object(
                Bucket=self.bucket, Key=self._key(key), Body=data, IfNoneMatch="*"
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
            }:
                return False
            raise
        return True

    def exists(self, key: str) -> bool:
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self._key(key), MaxKeys=1)
        return any(o.get("Key") == self._key(key) for o in resp.get("Contents", []))

    def list_keys(self, prefix: str) -> Iterator[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            for obj in page.get("Contents", []):
                yield obj["Key"][len(self.prefix) :]


def archive_from_uri(uri: str, s3_endpoint: str | None = None) -> Archive:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        root = Path(parsed.path if os.name != "nt" else parsed.path.lstrip("/"))
        return LocalArchive(root)
    if parsed.scheme == "s3":
        import boto3  # only needed in S3 mode
        from botocore.config import Config

        config = Config(retries={"max_attempts": 5, "mode": "standard"})
        if s3_endpoint:  # S3-compatible stores (SeaweedFS) want path-style addressing
            config = config.merge(Config(s3={"addressing_style": "path"}))
        client = boto3.client("s3", endpoint_url=s3_endpoint or None, config=config)
        return S3Archive(parsed.netloc, parsed.path, client)
    raise ValueError(f"unsupported archive URI {uri!r} (use file:// or s3://)")


def archive_from_env(env: Mapping[str, str]) -> Archive:
    uri = env.get("ARCHIVE_URI")
    if not uri:
        raise ValueError("ARCHIVE_URI is not set (file:///path or s3://bucket/prefix)")
    return archive_from_uri(uri, env.get("ARCHIVE_S3_ENDPOINT"))
