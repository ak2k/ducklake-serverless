"""The engine ↔ adapter boundary.

The generation/marker/CAS/lease/GC machinery moves *bytes*; it never needs to
know whether a generation is a DuckLake catalog, a plain DuckDB database, or an
arbitrary file. A `Payload` teaches the engine the few payload-specific things
it genuinely needs — where a generation's bytes live, whether a prepared file
is safe to publish, and what version tags to pin in the marker — without the
engine importing `duckdb` or understanding the payload's semantics.

Two levels, deliberately separated:

- **Engine storage contract (`Payload`, this module).** Pure bytes-in/bytes-out:
  `validate` (pre-publish hygiene, was `generation.check_hygiene`), `pins` +
  `check_pins` (embed and enforce version tags in the marker body, was
  `session._check_versions`), and `materialize` (turn reconstructed bytes into
  an adapter-openable handle). The generic commit driver depends on exactly
  this. Object keys are the *engine's* concern — it owns one generic layout
  (`payload/<gen>-<uuid>`); the library has never been deployed, so there is no
  `.duckdb` key convention to preserve and the adapter needs no say in keys.

- **Adapter transaction API (not here).** How a caller *mutates* a generation —
  DuckLake's `Lake.transaction()` with SQL recording and rebase-on-conflict
  replay — lives in the adapter, layered on top of the engine driver. A plain
  blob has no such layer; it checks a working copy out and back in wholesale and
  aborts on conflict.

`WorkingCopy` is the read/reconstruct handle: a generation materialized to a
local file. It is what `fsspec` reads and what the future chunk-store hands back
after reassembling packs; for the whole-file transport it is just the downloaded
file.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path


class WorkingCopy(Protocol):
    """A generation materialized to a local, adapter-openable file."""

    @property
    def path(self) -> Path:
        """Filesystem path of the reconstructed generation bytes."""
        ...


class Payload(Protocol):
    """Adapter storage contract the generic engine depends on.

    Implementations carry no lake state — they are strategy objects describing
    one payload *kind*. `BlobPayload` implements the trivial forms;
    `DuckLakeCatalog` implements the rich ones (DuckDB magic-byte hygiene,
    duckdb/ducklake version pinning).
    """

    def validate(self, blob_path: Path) -> None:
        """Reject a prepared file that could corrupt the lake if published.

        Runs before the create-only upload. Raises a `CatalogHygieneError`-family
        domain error (e.g. uncheckpointed WAL sidecar, wrong magic bytes, blown
        size cap). A no-op is legal for an opaque blob.
        """
        ...

    def pins(self) -> Mapping[str, str]:
        """Version tags to embed in this writer's marker body.

        Lets a future writer refuse a silent, fleet-wide format migration.
        Empty for a payload with no format to pin (a plain blob).
        """
        ...

    def check_pins(self, marker_pins: Mapping[str, str]) -> None:
        """Refuse to write when local versions differ from the lake's pins.

        Given the pins recorded in the current head's marker, raise
        `VersionMismatchError` if the local toolchain would migrate the format.
        A no-op for an unpinned payload.
        """
        ...

    def materialize(self, blob_path: Path) -> WorkingCopy:
        """Wrap reconstructed generation bytes as an adapter-openable handle.

        `blob_path` is the engine's local, byte-identical reconstruction of a
        generation (a whole-file download, or reassembled packs). For a plain
        blob this is the file itself; DuckLake returns a handle its connection
        can `ATTACH`.
        """
        ...
