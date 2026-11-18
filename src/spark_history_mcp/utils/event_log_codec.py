"""Decoding of raw Spark event-log files.

Shared by the native-engine tools (which read History Server /logs zips) and
the direct event-log backend (which reads files straight from gs://, s3://,
or local storage). Spark compresses event logs with
``spark.io.compression.codec``; the codec is detected from magic bytes.
"""

import io
import json
import re
import zipfile
import zlib
from typing import Any, Dict, Iterator, List

_COMPRESSION_MAGIC = {
    b"\x1f\x8b": "gzip",
    b"\x28\xb5\x2f\xfd": "zstd",
    b"\x04\x22\x4d\x18": "lz4",
    # Spark's LZ4CompressionCodec (the default) writes net.jpountz
    # LZ4BlockOutputStream, NOT the LZ4 frame format.
    b"LZ4Block": "lz4block",
    b"ZV": "lzf",
    b"\x82SNAPPY\x00": "snappy",
}

# Spark's SnappyCompressionCodec wraps org.xerial.snappy.SnappyOutputStream,
# which frames the stream as: 8-byte magic, two 4-byte version ints, then a
# sequence of [4-byte big-endian length][raw snappy block] chunks. This is
# not the same as the standalone/"Google-framed" Snappy format, so it can't
# be handed to snappy.decompress() as one blob -- each chunk must be sliced
# out and decompressed individually.
_XERIAL_SNAPPY_MAGIC = b"\x82SNAPPY\x00"


def _xerial_snappy_decompress(snappy_module: Any, data: bytes) -> bytes:
    pos = len(_XERIAL_SNAPPY_MAGIC) + 8
    chunks = []
    while pos < len(data):
        chunk_len = int.from_bytes(data[pos : pos + 4], "big")
        pos += 4
        chunks.append(snappy_module.decompress(data[pos : pos + chunk_len]))
        pos += chunk_len
    return b"".join(chunks)


def _lz4block_decompress(lz4_block: Any, data: bytes) -> bytes:
    """net.jpountz LZ4BlockOutputStream: repeated 21-byte headers
    (8 magic, 1 token, LE compressed len, LE decompressed len, checksum)
    each followed by a raw (0x10) or LZ4-compressed (0x20) block. Stops
    quietly at a truncated block (in-progress log)."""
    out, pos = [], 0
    while pos + 21 <= len(data) and data.startswith(b"LZ4Block", pos):
        token = data[pos + 8]
        clen = int.from_bytes(data[pos + 9 : pos + 13], "little")
        dlen = int.from_bytes(data[pos + 13 : pos + 17], "little")
        pos += 21
        if clen == 0 and dlen == 0:  # end-of-stream marker
            break
        block = data[pos : pos + clen]
        if len(block) < clen:
            break
        pos += clen
        method = token & 0xF0
        out.append(
            block
            if method == 0x10
            else lz4_block.decompress(block, uncompressed_size=dlen)
        )
    return b"".join(out)


def _decompress(name: str, data: bytes) -> bytes:
    """Decompress one event-log file's bytes, if compressed.

    Spark event logs can be written plain or compressed with the configured
    ``spark.io.compression.codec`` (lz4 by default); detect by magic bytes
    rather than filename, since Spark doesn't reliably suffix the name.
    """
    for magic, codec in _COMPRESSION_MAGIC.items():
        if not data.startswith(magic):
            continue
        if codec == "gzip":
            # decompressobj returns everything decodable so far, so a
            # truncated in-progress log yields its complete prefix.
            return zlib.decompressobj(wbits=31).decompress(data)
        if codec == "zstd":
            try:
                import zstandard
            except ImportError as error:
                raise RuntimeError(
                    f"Event log file {name!r} is zstd-compressed; install "
                    "the 'zstandard' package to read it"
                ) from error
            # Spark's streaming writer doesn't embed a content size in the
            # frame header (and may emit multiple concatenated frames), so
            # the one-shot decompress() -- which needs an upfront size --
            # fails with "could not determine content size in frame header".
            # A streaming reader across frames handles both cases.
            with zstandard.ZstdDecompressor().stream_reader(
                io.BytesIO(data), read_across_frames=True
            ) as reader:
                return reader.read()
        if codec == "lz4":
            try:
                import lz4.frame
            except ImportError as error:
                raise RuntimeError(
                    f"Event log file {name!r} is lz4-compressed; install "
                    "the 'lz4' package to read it"
                ) from error
            return lz4.frame.LZ4FrameDecompressor().decompress(data)
        if codec == "lz4block":
            try:
                import lz4.block
            except ImportError as error:
                raise RuntimeError(
                    f"Event log file {name!r} is lz4-compressed; install "
                    "the 'lz4' package to read it"
                ) from error
            return _lz4block_decompress(lz4.block, data)
        if codec == "lzf":
            raise RuntimeError(
                f"Event log file {name!r} is lzf-compressed, which is not "
                "supported; use the History Server REST tools instead"
            )
        if codec == "snappy":
            try:
                import snappy  # type: ignore[import-untyped]
            except ImportError as error:
                raise RuntimeError(
                    f"Event log file {name!r} is snappy-compressed; install "
                    "the 'python-snappy' package to read it"
                ) from error
            return _xerial_snappy_decompress(snappy, data)
    if data[:1] not in (b"{", b"\n", b" ", b"") and b"\0" in data[:64]:
        raise RuntimeError(
            f"Event log file {name!r} has an unrecognized binary format "
            f"(first bytes {data[:8]!r})"
        )
    return data


_PART_FILE_RE = re.compile(r"events_(\d+)_")


def _event_log_member_names(names: List[str]) -> List[str]:
    """Order event-log member names for sequential reading.

    Event log v2 (rolling) apps split their events across
    ``events_<n>_<app-id>`` part files inside an ``eventlog_v2_<app-id>/``
    directory, alongside an empty ``appstatus_<app-id>`` marker (skipped) --
    read the numbered parts in order. Non-rolling apps store one flat file
    named after the application id.
    """
    files = [n for n in names if not n.endswith("/")]

    # Without an attempt id, /logs zips every attempt's log together; mixing
    # them collides execution ids. Keep only the latest attempt.
    def attempt_of(n: str) -> str:
        top = n.split("/", 1)[0] if "/" in n else n
        return re.sub(r"\.(lz4|zstd|zst|snappy|gz|lzf|inprogress)$", "", top)

    groups: Dict[str, List[str]] = {}
    for n in files:
        if "appstatus_" in n and "/" not in n:
            continue
        groups.setdefault(attempt_of(n), []).append(n)
    if len(groups) > 1:

        def attempt_key(k: str):
            m = re.search(r"_(\d+)$", k)
            return (int(m.group(1)) if m else -1, k)

        files = groups[max(groups, key=attempt_key)]
    numbered = sorted(
        ((int(m.group(1)), n) for n in files if (m := _PART_FILE_RE.search(n))),
        key=lambda t: t[0],
    )
    if numbered:
        return [n for _, n in numbered]
    return [n for n in files if "appstatus_" not in n]


def iter_event_log_records(zip_bytes: bytes) -> Iterator[Dict[str, Any]]:
    """Yield every JSON event record from a downloaded event-log zip.

    Malformed lines are skipped rather than raising -- a single corrupt line
    (e.g. a truncated in-progress log) shouldn't prevent reading the rest.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in _event_log_member_names(zf.namelist()):
            raw = _decompress(name, zf.read(name))
            for line in raw.split(b"\n"):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except ValueError:  # JSONDecodeError and UnicodeDecodeError
                    continue
