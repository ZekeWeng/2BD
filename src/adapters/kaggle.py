from __future__ import annotations

import sys
import zipfile
from contextlib import contextmanager
from pathlib import Path

from ..domain import Target


def _rules_url(target: Target) -> str:
    if target.kind == "competition":
        return f"https://www.kaggle.com/competitions/{target.slug}/rules"
    return f"https://www.kaggle.com/datasets/{target.slug}"


class KaggleSource:
    def __init__(self) -> None:
        self._api = None

    def _ensure_api(self):
        if self._api is not None:
            return self._api
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi
        except ImportError:
            sys.exit("kaggle package missing. Run: uv sync")
        api = KaggleApi()
        try:
            api.authenticate()
        except Exception as exc:
            sys.exit(
                f"Kaggle auth failed: {exc}\n"
                "Set KAGGLE_API_TOKEN (KGAT_… bearer token) in .env or the "
                "environment, or place ~/.kaggle/kaggle.json (chmod 600). "
                "Get a token at https://www.kaggle.com/settings."
            )
        self._api = api
        return api

    @contextmanager
    def _translate_http_errors(self, target: Target):
        """403 → PermissionError with rules URL; other HTTPErrors get the slug."""
        try:
            from requests.exceptions import HTTPError
        except ImportError:
            HTTPError = ()  # type: ignore[assignment]
        try:
            yield
        except HTTPError as exc:  # type: ignore[misc]
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status == 403:
                raise PermissionError(
                    f"403 Forbidden for {target.kind} {target.slug!r}. "
                    f"Accept the rules at {_rules_url(target)} and retry."
                ) from exc
            raise RuntimeError(
                f"Kaggle HTTP {status} on {target.kind} {target.slug!r}: {exc}"
            ) from exc

    def fetch(self, target: Target, dest: Path, *, force: bool = False) -> list[Path]:
        api = self._ensure_api()
        before = {p for p in dest.rglob("*") if p.is_file()}

        with self._translate_http_errors(target):
            if not target.files:
                if target.kind == "competition":
                    api.competition_download_files(
                        target.slug, path=str(dest), force=force, quiet=False)
                else:
                    api.dataset_download_files(
                        target.slug, path=str(dest), force=force,
                        quiet=False, unzip=False)
            else:
                for filename in target.files:
                    if (dest / filename).exists() and not force:
                        continue
                    if target.kind == "competition":
                        api.competition_download_file(
                            target.slug, filename, path=str(dest),
                            force=force, quiet=False)
                    else:
                        api.dataset_download_file(
                            target.slug, filename, path=str(dest),
                            force=force, quiet=False)

        if target.unzip:
            for archive in dest.glob("*.zip"):
                with zipfile.ZipFile(archive) as zf:
                    zf.extractall(dest)
                archive.unlink()

        after = {p for p in dest.rglob("*") if p.is_file()}
        return sorted(after - before) or sorted(after)
