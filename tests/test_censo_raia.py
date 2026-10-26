import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from ijson.common import JSONError

import censo_raia


ANIMAL = {"identificador": "synthetic-1", "especie": "PERRO", "raza": "Prueba ñ"}


class InterruptedStream(httpx.SyncByteStream):
    def __iter__(self):
        yield b"[" + (json.dumps(ANIMAL).encode() + b",") * 2000
        raise httpx.ReadError("synthetic connection loss")


class CensusDownloadTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.destination = Path(self.directory.name) / "census.ndjson"
        self.previous = b'{"previous":"synthetic census"}\n'
        self.destination.write_bytes(self.previous)

    @contextlib.contextmanager
    def upstream(self, response):
        requests = []

        def handler(request):
            requests.append(request)
            return response

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            with patch.object(censo_raia.httpx, "stream", side_effect=client.stream):
                with contextlib.redirect_stdout(io.StringIO()):
                    yield requests

    def assert_preserved(self):
        self.assertEqual(self.destination.read_bytes(), self.previous)
        self.assertEqual(list(self.destination.parent.iterdir()), [self.destination])

    def test_complete_census_replaces_previous_file(self):
        documented = {"identidicador": "synthetic-2", "especie": "GATO"}
        records = [ANIMAL, documented]
        with self.upstream(httpx.Response(200, json=records)) as requests:
            censo_raia.stream_censo_to_ndjson(self.destination)
        self.assertEqual([json.loads(line) for line in self.destination.read_text().splitlines()], records)
        self.assertEqual(list(self.destination.parent.iterdir()), [self.destination])
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].headers["accept"], "application/json")

    def test_http_errors_preserve_existing_file_without_retry(self):
        for status in (301, 401, 403, 429, 500, 503):
            with self.subTest(status=status):
                with self.upstream(httpx.Response(status, json={"message": "synthetic error"})) as requests:
                    with self.assertRaises(httpx.HTTPStatusError):
                        censo_raia.stream_censo_to_ndjson(self.destination)
                self.assert_preserved()
                self.assertEqual(len(requests), 1)

    def test_empty_or_wrong_shape_census_is_rejected(self):
        for payload in ([], {}, {"item": ANIMAL}, None, [None], ["error"], [{}], [{"message": "error"}]):
            with self.subTest(payload=payload):
                response = httpx.Response(200, content=json.dumps(payload), headers={"content-type": "application/json"})
                with self.upstream(response):
                    with self.assertRaises(censo_raia.CensusDownloadError):
                        censo_raia.stream_censo_to_ndjson(self.destination)
                self.assert_preserved()

    def test_invalid_or_truncated_json_preserves_existing_file(self):
        valid = json.dumps([ANIMAL]).encode()
        for body in (b"", b"[", valid[:-1], valid + b"[]", valid + b"garbage"):
            with self.subTest(body=body):
                response = httpx.Response(200, content=body, headers={"content-type": "application/json"})
                with self.upstream(response):
                    with self.assertRaises(JSONError):
                        censo_raia.stream_censo_to_ndjson(self.destination)
                self.assert_preserved()

    def test_html_success_response_is_rejected(self):
        response = httpx.Response(200, text="<html>synthetic error</html>", headers={"content-type": "text/html"})
        with self.upstream(response):
            with self.assertRaises(censo_raia.CensusDownloadError):
                censo_raia.stream_censo_to_ndjson(self.destination)
        self.assert_preserved()

    def test_interrupted_download_discards_partial_file(self):
        response = httpx.Response(200, stream=InterruptedStream(), headers={"content-type": "application/json"})
        with self.upstream(response):
            with self.assertRaises(httpx.ReadError):
                censo_raia.stream_censo_to_ndjson(self.destination)
        self.assert_preserved()

    def test_failure_does_not_create_output_on_first_download(self):
        self.destination.unlink()
        with self.upstream(httpx.Response(200, json=[])):
            with self.assertRaises(censo_raia.CensusDownloadError):
                censo_raia.stream_censo_to_ndjson(self.destination)
        self.assertEqual(list(self.destination.parent.iterdir()), [])

    def test_replace_failure_preserves_existing_file_and_removes_temporary_file(self):
        with self.upstream(httpx.Response(200, json=[ANIMAL])):
            with patch.object(Path, "replace", side_effect=OSError("synthetic disk failure")):
                with self.assertRaises(OSError):
                    censo_raia.stream_censo_to_ndjson(self.destination)
        self.assert_preserved()

    def test_cli_fails_with_safe_diagnostic_for_provider_errors(self):
        for status in (403, 500):
            with self.subTest(status=status):
                stderr = io.StringIO()
                response = httpx.Response(status, json={"message": "sensitive upstream detail"})
                with self.upstream(response), patch.object(censo_raia, "OUTPUT_PATH", self.destination):
                    with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as error:
                        censo_raia.main()
                self.assertEqual(error.exception.code, 1)
                self.assertIn(f"HTTP {status}", stderr.getvalue())
                self.assertNotIn("sensitive upstream detail", stderr.getvalue())
                self.assert_preserved()

    def test_cli_rejects_invalid_json_without_logging_response_body(self):
        response = httpx.Response(
            200, content=b"sensitive upstream detail",
            headers={"content-type": "application/json"},
        )
        stderr = io.StringIO()
        with self.upstream(response), patch.object(censo_raia, "OUTPUT_PATH", self.destination):
            with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as error:
                censo_raia.main()
        self.assertEqual(error.exception.code, 1)
        self.assertIn("invalid or incomplete JSON", stderr.getvalue())
        self.assertNotIn("sensitive upstream detail", stderr.getvalue())
        self.assert_preserved()

    def test_network_or_tls_failure_exits_without_retry(self):
        stderr = io.StringIO()
        with patch.object(censo_raia.httpx, "stream", side_effect=httpx.ConnectError("synthetic TLS error")) as request:
            with patch.object(censo_raia, "OUTPUT_PATH", self.destination):
                with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(SystemExit) as error:
                        censo_raia.main()
        self.assertEqual(error.exception.code, 1)
        self.assertIn("network or TLS", stderr.getvalue())
        self.assertEqual(request.call_count, 1)
        self.assert_preserved()


if __name__ == "__main__":
    unittest.main()
