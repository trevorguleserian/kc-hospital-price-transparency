"""
Storage abstraction for lakehouse: local filesystem or GCS.
Backend selected via env STORAGE_BACKEND=local|gcs.
Windows path compatibility: use pathlib and normalize separators.
"""
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

# Optional: pandas for write_parquet; pyarrow for parquet
try:
    import pandas as pd
except ImportError:
    pd = None


def _normalize_path(path: str, base: Optional[Path] = None) -> Path:
    """Normalize path for Windows/local; optionally join to base."""
    p = Path(path).resolve()
    if base is not None:
        p = (base / path).resolve()
    return p


class StorageBackend(ABC):
    """Abstract interface for list_files, read_bytes, write_parquet, exists."""

    @abstractmethod
    def list_files(self, prefix: str) -> List[str]:
        """List object/key paths under prefix (no leading slash)."""

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Read full object as bytes."""

    @abstractmethod
    def write_parquet(self, df: "pd.DataFrame", path: str) -> None:
        """Write DataFrame to path as Parquet (path is key/path without bucket/base)."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Return True if path exists."""


class LocalStorage(StorageBackend):
    """Local filesystem backend; base_dir is project root or data root."""

    def __init__(self, base_dir: str = "."):
        self.base = Path(base_dir).resolve()

    def _full_path(self, path: str) -> Path:
        # Normalize: no leading slash, use OS separator
        path = path.replace("/", os.sep).lstrip(os.sep)
        return self.base / path

    def list_files(self, prefix: str) -> List[str]:
        full = self._full_path(prefix)
        if not full.exists() or not full.is_dir():
            return []
        out: List[str] = []
        for f in full.rglob("*"):
            if f.is_file():
                rel = f.relative_to(self.base)
                out.append(str(rel).replace(os.sep, "/"))
        return sorted(out)

    def read_bytes(self, path: str) -> bytes:
        full = self._full_path(path)
        if not full.is_file():
            raise FileNotFoundError(str(full))
        return full.read_bytes()

    def write_parquet(self, df: "pd.DataFrame", path: str) -> None:
        if pd is None:
            raise RuntimeError("pandas required for write_parquet")
        full = self._full_path(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(full, index=False)

    def exists(self, path: str) -> bool:
        return self._full_path(path).exists()


class GCSStorage(StorageBackend):
    """GCS backend; path = blob path under bucket/prefix."""

    def __init__(self, bucket_name: str, prefix: str = ""):
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/")
        self._client = None

    def _client_get(self):
        from google.cloud import storage
        if self._client is None:
            self._client = storage.Client()
        return self._client

    def _blob_path(self, path: str) -> str:
        path = path.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def list_files(self, prefix: str) -> List[str]:
        full_prefix = self._blob_path(prefix)
        bucket = self._client_get().bucket(self.bucket_name)
        blobs = list(bucket.list_blobs(prefix=full_prefix))
        out = []
        for b in blobs:
            name = b.name
            if self.prefix and name.startswith(self.prefix + "/"):
                name = name[len(self.prefix) + 1:]
            out.append(name)
        return sorted(out)

    def read_bytes(self, path: str) -> bytes:
        blob_path = self._blob_path(path)
        bucket = self._client_get().bucket(self.bucket_name)
        blob = bucket.blob(blob_path)
        return blob.download_as_bytes()

    def write_parquet(self, df: "pd.DataFrame", path: str) -> None:
        if pd is None:
            raise RuntimeError("pandas required for write_parquet")
        import io
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        blob_path = self._blob_path(path)
        bucket = self._client_get().bucket(self.bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_file(buf, content_type="application/octet-stream")

    def exists(self, path: str) -> bool:
        blob_path = self._blob_path(path)
        bucket = self._client_get().bucket(self.bucket_name)
        blob = bucket.blob(blob_path)
        return blob.exists()


def get_storage(
    backend: Optional[str] = None,
    base_dir: Optional[str] = None,
    bucket_name: Optional[str] = None,
    prefix: Optional[str] = None,
) -> StorageBackend:
    """
    Return storage backend from env STORAGE_BACKEND (local|gcs).
    local: LocalStorage(base_dir=base_dir or ".")
    gcs: GCSStorage(bucket_name, prefix) from env GCS_BUCKET, GCS_PREFIX if not passed.
    """
    backend = (backend or os.environ.get("STORAGE_BACKEND", "local")).strip().lower()
    if backend == "gcs":
        bucket = bucket_name or os.environ.get("GCS_BUCKET", "pt_incoming")
        pre = prefix if prefix is not None else os.environ.get("GCS_PREFIX", "pt_landing")
        return GCSStorage(bucket_name=bucket, prefix=pre)
    return LocalStorage(base_dir=base_dir or os.environ.get("LAKE_BASE_DIR", "."))
