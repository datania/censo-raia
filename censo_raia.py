import json
from collections.abc import Iterable
from pathlib import Path

import httpx
import ijson.backends.python as ijson

API_URL = "https://api.raia.es/api/censoPublico"
OUTPUT_PATH = Path("data/raw/raia_censo.ndjson")
# The host presents an incomplete certificate chain in this environment.
VERIFY_TLS = False


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

    with destination.open("w", encoding="utf-8") as jsonl_file:
        with httpx.stream(
            "GET",
            API_URL,
            timeout=300,
            headers={"Accept": "application/json"},
            verify=VERIFY_TLS,
        ) as response:
            print(f"Connected. HTTP {response.status_code}")
            response.raise_for_status()

            byte_stream = BytesIteratorReader(response.iter_bytes())
            for animal in ijson.items(byte_stream, "item", multiple_values=True):
                jsonl_file.write(json.dumps(animal, ensure_ascii=False))
                jsonl_file.write("\n")
                total += 1
                if total % 1000 == 0:
                    print(f"Wrote {total} animals")

    print(f"Finished. Wrote {total} animals to {destination}")


def main() -> None:
    print(f"Requesting RAIA census from {API_URL}")
    stream_censo_to_ndjson(OUTPUT_PATH)


if __name__ == "__main__":
    main()
