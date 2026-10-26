import json
import sys
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile

import httpx
import ijson.backends.python as ijson
from ijson.common import JSONError

API_URL = "https://api.raia.es/api/censoPublico"
OUTPUT_PATH = Path("data/raw/raia_censo.ndjson")


class CensusDownloadError(RuntimeError):
    """An upstream response cannot be used as a census."""


class BytesIteratorReader:
    def __init__(self, iterator: Iterable[bytes]) -> None:
        self._iterator = iter(iterator)
        self._buffer = bytearray()

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            data = bytes(self._buffer)
            self._buffer.clear()
            return data + b"".join(self._iterator)

        while len(self._buffer) < size:
            try:
                self._buffer.extend(next(self._iterator))
            except StopIteration:
                break

        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data


def stream_censo_to_ndjson(destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    temporary_path = None

    try:
        with httpx.stream(
            "GET",
            API_URL,
            timeout=300,
            headers={"Accept": "application/json"},
        ) as response:
            print(f"Connected. HTTP {response.status_code}")
            response.raise_for_status()

            content_type = response.headers.get("content-type", "").split(";", 1)[0]
            if content_type.strip().lower() != "application/json":
                raise CensusDownloadError("RAIA did not return application/json.")

            byte_stream = BytesIteratorReader(response.iter_bytes())
            events = ijson.parse(byte_stream)
            if next(events, None) != ("", "start_array", None):
                raise CensusDownloadError("RAIA did not return a JSON array.")

            with NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=destination.parent,
                prefix=f".{destination.name}.", suffix=".tmp", delete=False,
            ) as jsonl_file:
                temporary_path = Path(jsonl_file.name)
                for animal in ijson.items(events, "item"):
                    if not isinstance(animal, dict):
                        raise CensusDownloadError("RAIA returned an invalid animal record.")
                    identifier = animal.get("identificador", animal.get("identidicador"))
                    species = animal.get("especie")
                    if (
                        not isinstance(identifier, str) or not identifier
                        or not isinstance(species, str) or not species
                    ):
                        raise CensusDownloadError("RAIA returned an invalid animal record.")
                    jsonl_file.write(json.dumps(animal, ensure_ascii=False))
                    jsonl_file.write("\n")
                    total += 1
                    if total % 1000 == 0:
                        print(f"Downloaded {total} animals")

            if total == 0:
                raise CensusDownloadError("RAIA returned an empty census.")

        temporary_path.replace(destination)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    print(f"Finished. Wrote {total} animals to {destination}")


def main() -> None:
    print(f"Requesting RAIA census from {API_URL}")
    try:
        stream_censo_to_ndjson(OUTPUT_PATH)
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        message = f"RAIA returned HTTP {status}."
        if status in (401, 403):
            message += (
                " Access was denied. Confirm API access requirements with RAIA"
                " (informatica@colegiosveterinarios.es); the published OpenAPI"
                " does not specify authentication."
            )
        elif status >= 500:
            message += " The provider reported a server error."
        fail(message)
    except httpx.HTTPError:
        fail("The RAIA network or TLS request failed.")
    except JSONError:
        fail("RAIA returned invalid or incomplete JSON.")
    except CensusDownloadError as error:
        fail(str(error))


def fail(message: str) -> None:
    print(
        f"{message} No new census was saved; any existing file was preserved.",
        file=sys.stderr,
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
