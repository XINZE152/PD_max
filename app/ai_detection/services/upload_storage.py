from __future__ import annotations

import hashlib
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from PIL import Image, UnidentifiedImageError


MAX_IMAGE_BYTES = 20 * 1024 * 1024
UPLOAD_CHUNK_BYTES = 1024 * 1024
MAX_ORIGINAL_FILENAME_LENGTH = 512

_TASK_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_FORMAT_INFO = {
    "JPEG": (".jpg", "image/jpeg"),
    "PNG": (".png", "image/png"),
    "WEBP": (".webp", "image/webp"),
}


class UploadStorageError(ValueError):
    code = "INVALID_IMAGE"


class ImageTooLargeError(UploadStorageError):
    code = "IMAGE_TOO_LARGE"


class UnsupportedImageTypeError(UploadStorageError):
    code = "UNSUPPORTED_IMAGE_TYPE"


@dataclass(frozen=True)
class UploadArtifact:
    path: Path
    original_filename: str
    content_sha256: str
    size_bytes: int
    media_type: str


def normalize_upload_filename(value: str | None) -> str:
    raw = unicodedata.normalize("NFC", str(value or "").strip())
    name = raw.replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(ch for ch in name if not unicodedata.category(ch).startswith("C")).strip()
    if not name or name in {".", ".."}:
        name = "upload"
    return name[:MAX_ORIGINAL_FILENAME_LENGTH]


def save_original_image(
    stream: BinaryIO,
    *,
    storage_dir: Path,
    task_id: str,
    original_filename: str | None,
    max_bytes: int = MAX_IMAGE_BYTES,
) -> UploadArtifact:
    if not _TASK_ID_RE.fullmatch(task_id):
        raise ValueError("Invalid task id")

    storage_dir.mkdir(parents=True, exist_ok=True)
    temp_path = storage_dir / f".{task_id}.upload.part"
    digest = hashlib.sha256()
    size_bytes = 0

    try:
        with open(temp_path, "wb") as target:
            while True:
                chunk = stream.read(UPLOAD_CHUNK_BYTES)
                if not chunk:
                    break
                size_bytes += len(chunk)
                if size_bytes > max_bytes:
                    raise ImageTooLargeError(f"Image exceeds {max_bytes} bytes")
                digest.update(chunk)
                target.write(chunk)

        try:
            with Image.open(temp_path) as image:
                image_format = str(image.format or "").upper()
                image.verify()
        except (UnidentifiedImageError, OSError, SyntaxError) as exc:
            raise UnsupportedImageTypeError("Unsupported or invalid image") from exc

        format_info = _FORMAT_INFO.get(image_format)
        if format_info is None:
            raise UnsupportedImageTypeError(f"Unsupported image format: {image_format or 'unknown'}")

        extension, media_type = format_info
        final_path = storage_dir / f"{task_id}{extension}"
        os.replace(temp_path, final_path)
        return UploadArtifact(
            path=final_path,
            original_filename=normalize_upload_filename(original_filename),
            content_sha256=digest.hexdigest(),
            size_bytes=size_bytes,
            media_type=media_type,
        )
    except Exception:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise
