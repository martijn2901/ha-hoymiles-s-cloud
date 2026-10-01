"""Regression checks for sparse module data and cloud session expiry."""

import importlib.util
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import time
import types
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1] / "custom_components" / "hoymiles_nimbus"
sys.path.insert(0, str(ROOT))

# Keep these small tests runnable without installing Home Assistant's dependencies.
requests = types.ModuleType("requests")
requests.exceptions = types.SimpleNamespace(RequestException=Exception)
requests.post = Mock()
cachetools = types.ModuleType("cachetools")
cachetools.TTLCache = lambda **kwargs: {}
cachetools.cached = lambda **kwargs: lambda function: function
sys.modules["requests"] = requests
sys.modules["cachetools"] = cachetools

spec = importlib.util.spec_from_file_location("hoymiles_client", ROOT / "hoymiles_client.py")
client_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client_module)

from classes.micro_inverter import Microinverter
from classes.station import Station


class Response:
    def __init__(self, data):
        self.data = data
        self.status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


class RegressionTests(unittest.TestCase):
    def setUp(self):
        self.client = client_module.HoymilesClient("user", "password", "https://example/")
        self.client.token = "old-token"
        self.client._authenticated_at = time.monotonic()
        self.client.get_token = Mock(return_value={"data": {"token": "new-token"}})
        requests.post.reset_mock()
        requests.post.side_effect = None

    def test_expired_token_is_refreshed_before_the_request(self):
        self.client._authenticated_at -= 23 * 3600 + 1
        requests.post.return_value = Response({"data": {"list": []}})

        self.assertEqual(self.client.select_by_page("station"), [])
        self.client.get_token.assert_called_once()
        self.assertEqual(requests.post.call_args.kwargs["headers"]["Authorization"], "new-token")

    def test_empty_data_refreshes_once_and_retries_with_new_token(self):
        requests.post.side_effect = [
            Response({"data": ""}),
            Response({"data": {"list": [{"id": 12}]}}),
        ]

        self.assertEqual(self.client.select_by_page("station"), [{"id": 12}])
        self.client.get_token.assert_called_once()
        self.assertEqual(requests.post.call_count, 2)
        headers = [call.kwargs["headers"] for call in requests.post.call_args_list]
        self.assertEqual([h["Authorization"] for h in headers], ["old-token", "new-token"])

    def test_concurrent_requests_share_one_renewal(self):
        self.client._authenticated_at -= 23 * 3600 + 1
        requests.post.return_value = Response({"data": {"list": []}})

        with ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(lambda _: self.client.select_by_page("station"), range(5)))

        self.assertEqual(results, [[]] * 5)
        self.client.get_token.assert_called_once()

    def test_empty_data_after_retry_does_not_loop(self):
        requests.post.side_effect = [Response({"data": ""}), Response({"data": ""})]

        with self.assertRaises(client_module.HoymilesResponseError):
            self.client.select_by_page("station")
        self.assertEqual(requests.post.call_count, 2)

    def test_new_day_without_samples_does_not_warn(self):
        station = Station(12, "station")
        data = Mock()
        data.get_compact.return_value = [12, "2026-10-01"]

        with patch("classes.station._LOGGER.warning") as warning:
            station.set_data(data)
        warning.assert_not_called()

    def test_short_microinverter_list_is_ignored(self):
        micro = Microinverter(45, "serial")
        micro.set_data([[45, [1]]])


if __name__ == "__main__":
    unittest.main()
