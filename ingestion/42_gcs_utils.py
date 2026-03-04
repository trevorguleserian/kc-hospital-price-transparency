"""
GCS helpers for bulk ingestion: upload, delete, and file checksums (Windows-friendly).
"""
import hashlib
import os
from urllib.parse import urlparse

from google.cloud import storage

CHUNK_SIZE_DEFAULT = 8 * 1024 * 1024  # 8 MB


def upload_file_to_gcs(local_path: str, bucket: str, prefix: str, run_id: str) -> str:
    """
    Upload local file to gs://{bucket}/{prefix}/run_id={run_id}/{basename}.
    Returns gcs_uri (e.g. gs://bucket/pt_landing/run_id=20250226120000/file.json).
    """
    local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        raise FileNotFoundError(local_path)
    basename = os.path.basename(local_path)
    prefix = prefix.rstrip("/")
    blob_path = f"{prefix}/run_id={run_id}/{basename}"
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(blob_path)
    blob.upload_from_filename(local_path, content_type="application/octet-stream")
    return f"gs://{bucket}/{blob_path}"


def delete_gcs_uri(gcs_uri: str) -> None:
    """Delete the object at the gs:// URI."""
    parsed = urlparse(gcs_uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")
    bucket_name = parsed.netloc
    blob_path = parsed.path.lstrip("/")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.delete()


def compute_sha256(local_path: str, chunk_size: int = CHUNK_SIZE_DEFAULT) -> str:
    """Compute SHA-256 of file in chunks (Windows-friendly). Returns hex digest."""
    local_path = os.path.abspath(local_path)
    if not os.path.isfile(local_path):
        raise FileNotFoundError(local_path)
    h = hashlib.sha256()
    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def file_size_bytes(local_path: str) -> int:
    """Return file size in bytes."""
    return os.path.getsize(os.path.abspath(local_path))


def list_blob_names(bucket_name: str, prefix: str, project: str | None = None) -> list[str]:
    """
    List blob names under prefix (names are full key paths, e.g. pt_incoming/file.json).
    Normalizes prefix so "pt_incoming" and "pt_incoming/" both work.
    Uses storage.Client(project=project). Does not swallow exceptions.
    """
    if prefix != "" and not prefix.endswith("/"):
        prefix = prefix + "/"
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=prefix) if not blob.name.endswith("/")]


def list_blobs_debug(bucket_name: str, project: str | None = None, limit: int = 50) -> list[str]:
    """
    List first N blob names with no prefix (prefix="") to prove listing works.
    Returns list of blob names.
    """
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(max_results=limit))
    return [b.name for b in blobs if not b.name.endswith("/")]


def copy_blob(bucket_name: str, source_key: str, dest_key: str) -> None:
    """Copy object within bucket from source_key to dest_key."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source = bucket.blob(source_key)
    bucket.copy_blob(source, bucket, dest_key)


def download_blob_to_file(bucket_name: str, key: str, local_path: str) -> None:
    """Download GCS object to local file (Windows-friendly path)."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(key)
    blob.download_to_filename(os.path.abspath(local_path))


def upload_string_to_key(bucket_name: str, key: str, content: str) -> None:
    """Upload string content to GCS key (e.g. error log)."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(key)
    blob.upload_from_string(content, content_type="text/plain; charset=utf-8")
