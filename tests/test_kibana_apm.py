"""Тесты клиента Kibana APM и поиска корневой транзакции трейса."""

import os
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.elasticsearch_client import ElasticsearchManager
from src.kibana_apm_client import KibanaApmClient, compact_service, resolve_window


ENV = {
    "KIBANA_BASE_URL": "https://apm.example",
    "APM_USERNAME": "apm",
    "APM_PASSWORD": "secret",
    "APM_TIMEOUT": "5",
}


class FakeResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeHttp:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def get(self, url, params=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self.response


class TestKibanaApmClient(unittest.IsolatedAsyncioTestCase):
    def test_window_rejects_inverted_range(self):
        with self.assertRaises(ValueError):
            resolve_window("2026-09-23T16:00:00Z", "2026-09-23T15:00:00Z")

    def test_service_without_latency_omits_latency_ms(self):
        compacted = compact_service({"serviceName": "math", "throughput": 1})
        self.assertNotIn("latencyMs", compacted)
        self.assertEqual(compacted["throughputPerMinute"], 1)

    async def test_list_services_converts_latency_and_caps(self):
        http = FakeHttp(FakeResponse(200, {"items": [
            {"serviceName": "slow", "latency": 2000, "transactionErrorRate": 0, "throughput": 1, "agentName": "php", "transactionType": "request", "environments": ["prod"]},
            {"serviceName": "busy", "latency": 1000, "transactionErrorRate": 0.1, "throughput": 50, "agentName": "php", "transactionType": "request", "environments": ["prod"]},
            {"serviceName": "mid", "latency": 1500, "transactionErrorRate": 0, "throughput": 10, "agentName": "php", "transactionType": "request", "environments": ["prod"]},
        ]}))
        with patch.dict(os.environ, ENV):
            result = await KibanaApmClient(http=http).list_services(
                start="2026-09-23T16:00:00Z",
                end="2026-09-23T16:15:00Z",
                limit=2,
            )
        self.assertEqual([item["serviceName"] for item in result["services"]], ["busy", "mid"])
        self.assertEqual(result["services"][0]["latencyMs"], 1.0)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["total"], 3)
        self.assertEqual(http.calls[0]["params"]["documentType"], "serviceTransactionMetric")
        self.assertEqual(http.calls[0]["headers"]["kbn-xsrf"], "true")

    async def test_http_error_is_not_an_empty_list(self):
        http = FakeHttp(FakeResponse(503, {"message": "upstream down"}))
        with patch.dict(os.environ, ENV):
            with self.assertRaises(RuntimeError) as caught:
                await KibanaApmClient(http=http).list_services(
                    start="2026-09-23T16:00:00Z",
                    end="2026-09-23T16:15:00Z",
                )
        self.assertIn("HTTP 503", str(caught.exception))
        self.assertIn("upstream down", str(caught.exception))

    async def test_empty_error_groups_stay_empty(self):
        http = FakeHttp(FakeResponse(200, {"errorGroups": []}))
        with patch.dict(os.environ, ENV):
            result = await KibanaApmClient(http=http).list_errors(
                "math",
                start="2026-09-23T16:00:00Z",
                end="2026-09-23T16:15:00Z",
            )
        self.assertEqual(result["errors"], [])
        self.assertFalse(result["truncated"])
        self.assertNotIn("documentType", http.calls[0]["params"])

    async def test_transactions_reject_unknown_aggregation_before_http(self):
        http = FakeHttp(FakeResponse(200, {}))
        with patch.dict(os.environ, ENV):
            with self.assertRaises(ValueError):
                await KibanaApmClient(http=http).list_transactions(
                    "math",
                    latency_aggregation_type="p50",
                    start="2026-09-23T16:00:00Z",
                    end="2026-09-23T16:15:00Z",
                )
        self.assertEqual(http.calls, [])

    async def test_trace_compacts_docs_and_marks_kibana_overflow(self):
        http = FakeHttp(FakeResponse(200, {
            "traceItems": {
                "exceedsMax": True,
                "traceItemCount": 500,
                "traceDocs": [{
                    "processor": {"event": "transaction"},
                    "transaction": {"id": "tx", "name": "GET /", "type": "request", "duration": {"us": 72141}},
                    "service": {"name": "math"},
                    "event": {"outcome": "success"},
                    "labels": {"huge": "drop-me"},
                }],
            }
        }))
        with patch.dict(os.environ, ENV):
            result = await KibanaApmClient(http=http).get_trace(
                "trace",
                "tx",
                start="2026-09-23T16:00:00Z",
                end="2026-09-23T16:15:00Z",
            )
        self.assertTrue(result["exceedsMax"])
        self.assertEqual(result["items"][0]["durationUs"], 72141)
        self.assertNotIn("offsetUs", result["items"][0])
        self.assertNotIn("labels", result["items"][0])
        self.assertEqual(http.calls[0]["params"]["entryTransactionId"], "tx")

    async def test_trace_offset_keeps_duration_order(self):
        started = 1_700_000_000_000_000
        http = FakeHttp(FakeResponse(200, {"traceItems": {"traceDocs": [
            {
                "processor": {"event": "transaction"},
                "timestamp": {"us": started},
                "transaction": {"id": "tx", "name": "PATCH /room", "duration": {"us": 17_000_000}},
            },
            {
                "processor": {"event": "span"},
                "timestamp": {"us": started + 5_000},
                "parent": {"id": "tx"},
                "transaction": {"id": "tx"},
                "span": {"id": "lock-row", "name": "SELECT FROM room FOR UPDATE", "duration": {"us": 16_900_000}},
            },
            {
                "processor": {"event": "span"},
                "timestamp": {"us": started + 100},
                "parent": {"id": "tx"},
                "transaction": {"id": "tx"},
                "span": {"id": "advisory", "name": "advisory lock", "duration": {"us": 500}},
            },
        ]}}))
        with patch.dict(os.environ, ENV):
            result = await KibanaApmClient(http=http).get_trace(
                "trace",
                "tx",
                start="2026-09-23T16:00:00Z",
                end="2026-09-23T16:15:00Z",
            )
        names = [item["name"] for item in result["items"]]
        self.assertEqual(names, ["PATCH /room", "SELECT FROM room FOR UPDATE", "advisory lock"])
        offsets = {item["name"]: item["offsetUs"] for item in result["items"]}
        self.assertEqual(offsets["PATCH /room"], 0)
        self.assertLess(offsets["advisory lock"], offsets["SELECT FROM room FOR UPDATE"])
        self.assertNotIn("timestampUs", result["items"][0])

    async def test_trace_samples_pass_duration_in_microseconds(self):
        http = FakeHttp(FakeResponse(200, {"traceSamples": [
            {"score": 0, "timestamp": "2026-09-24T18:23:08.369Z", "transactionId": "tx", "traceId": "trace"},
        ]}))
        with patch.dict(os.environ, ENV):
            result = await KibanaApmClient(http=http).list_trace_samples(
                "vimbox-core-rooms",
                "POST /server-api/complex/v1/archive/pack-room",
                start="2026-09-24T18:15:00Z",
                end="2026-09-24T18:45:00Z",
                min_duration_ms=20_000,
            )
        params = http.calls[0]["params"]
        self.assertTrue(http.calls[0]["url"].endswith("/transactions/traces/samples"))
        self.assertEqual(params["transactionName"], "POST /server-api/complex/v1/archive/pack-room")
        self.assertEqual(params["sampleRangeFrom"], "20000000")
        self.assertEqual(params["sampleRangeTo"], "86400000000")
        self.assertEqual(result["samples"], [
            {"timestamp": "2026-09-24T18:23:08.369Z", "traceId": "trace", "transactionId": "tx"},
        ])

    async def test_trace_samples_without_duration_skip_range(self):
        http = FakeHttp(FakeResponse(200, {"traceSamples": []}))
        with patch.dict(os.environ, ENV):
            result = await KibanaApmClient(http=http).list_trace_samples(
                "math",
                "GET /",
                start="2026-09-24T18:15:00Z",
                end="2026-09-24T18:45:00Z",
            )
        self.assertNotIn("sampleRangeFrom", http.calls[0]["params"])
        self.assertEqual(result["samples"], [])
        self.assertEqual(result["total"], 0)

    async def test_trace_samples_require_transaction_name_before_http(self):
        http = FakeHttp(FakeResponse(200, {}))
        with patch.dict(os.environ, ENV):
            with self.assertRaises(ValueError):
                await KibanaApmClient(http=http).list_trace_samples("math", " ")
        self.assertEqual(http.calls, [])

    async def test_missing_kibana_url_fails_before_request(self):
        http = FakeHttp(FakeResponse(200, {}))
        env = dict(ENV)
        env["KIBANA_BASE_URL"] = ""
        with patch.dict(os.environ, env):
            with self.assertRaises(RuntimeError) as caught:
                KibanaApmClient(http=http)
        self.assertIn("KIBANA_BASE_URL", str(caught.exception))
        self.assertEqual(http.calls, [])


class TestFindTraceEntry(unittest.IsolatedAsyncioTestCase):
    @patch("src.elasticsearch_client.AsyncElasticsearch")
    async def test_uses_root_transaction_without_second_search(self, _mock_es):
        manager = ElasticsearchManager()
        response = Mock()
        response.body = {"hits": {"hits": [{"_source": {"transaction": {"id": "root"}}}]}}
        manager.client.search = AsyncMock(return_value=response)
        found = await manager.find_trace_entry_transaction("trace", "start", "end")
        self.assertEqual(found, "root")
        self.assertEqual(manager.client.search.await_count, 1)
        query = manager.client.search.await_args.kwargs["body"]["query"]
        self.assertIn("must_not", query["bool"])

    @patch("src.elasticsearch_client.AsyncElasticsearch")
    async def test_falls_back_when_root_is_missing(self, _mock_es):
        manager = ElasticsearchManager()
        empty = Mock()
        empty.body = {"hits": {"hits": []}}
        child = Mock()
        child.body = {"hits": {"hits": [{"_source": {"transaction": {"id": "child"}}}]}}
        manager.client.search = AsyncMock(side_effect=[empty, child])
        found = await manager.find_trace_entry_transaction("trace", "start", "end")
        self.assertEqual(found, "child")
        self.assertEqual(manager.client.search.await_count, 2)

    @patch("src.elasticsearch_client.AsyncElasticsearch")
    async def test_search_error_does_not_look_like_not_found(self, _mock_es):
        manager = ElasticsearchManager()
        manager.client.search = AsyncMock(side_effect=RuntimeError("timeout"))
        with self.assertRaises(RuntimeError) as caught:
            await manager.find_trace_entry_transaction("trace", "start", "end")
        self.assertIn("Ошибка Elasticsearch", str(caught.exception))
        self.assertNotIn("не найдена", str(caught.exception))
        self.assertEqual(manager.client.search.await_count, 1)

    @patch("src.elasticsearch_client.AsyncElasticsearch")
    async def test_no_documents_returns_none(self, _mock_es):
        manager = ElasticsearchManager()
        empty = Mock()
        empty.body = {"hits": {"hits": []}}
        manager.client.search = AsyncMock(return_value=empty)
        found = await manager.find_trace_entry_transaction("trace", "start", "end")
        self.assertIsNone(found)
        self.assertEqual(manager.client.search.await_count, 2)
