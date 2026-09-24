"""Клиент внутреннего API Kibana APM 8.9.

Хост Kibana — не кластер Elasticsearch. Роуты /internal/apm/* отдают
агрегаты UI: сервисы, группы транзакций, группы ошибок, документы трейса.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx

TRACE_ITEM_LIMIT = 80
SERVICES_PATH = "/internal/apm/services"


def resolve_window(
    start: Optional[str],
    end: Optional[str],
    default_minutes: int = 15,
) -> Tuple[str, str]:
    """Окно в ISO-8601 UTC. Пустые границы — последние default_minutes минут."""
    end_dt = _parse_iso(end) if end else datetime.now(timezone.utc)
    start_dt = _parse_iso(start) if start else end_dt - timedelta(minutes=default_minutes)
    if start_dt >= end_dt:
        raise ValueError("start должен быть раньше end")
    return _format_iso(start_dt), _format_iso(end_dt)


def latency_ms(latency_us: Any) -> Optional[float]:
    """Kibana APM отдаёт latency в микросекундах."""
    if latency_us is None:
        return None
    return round(float(latency_us) / 1000, 3)


def compact_service(item: Dict[str, Any]) -> Dict[str, Any]:
    compacted = {
        "serviceName": item.get("serviceName"),
        "transactionType": item.get("transactionType"),
        "environments": item.get("environments") or [],
        "agentName": item.get("agentName"),
        "errorRate": item.get("transactionErrorRate"),
        "throughputPerMinute": item.get("throughput"),
    }
    converted = latency_ms(item.get("latency"))
    if converted is not None:
        compacted["latencyMs"] = converted
    return compacted


def compact_transaction_group(item: Dict[str, Any]) -> Dict[str, Any]:
    compacted = {
        "name": item.get("name"),
        "transactionType": item.get("transactionType"),
        "errorRate": item.get("errorRate"),
        "throughputPerMinute": item.get("throughput"),
        "impact": item.get("impact"),
        "alertsCount": item.get("alertsCount"),
    }
    converted = latency_ms(item.get("latency"))
    if converted is not None:
        compacted["latencyMs"] = converted
    return compacted


def compact_trace_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    event = (doc.get("processor") or {}).get("event")
    if event == "span":
        node = doc.get("span") or {}
    else:
        node = doc.get("transaction") or {}
    duration = node.get("duration") or {}
    compacted = {
        "event": event,
        "id": node.get("id"),
        "parentId": (doc.get("parent") or {}).get("id"),
        "service": (doc.get("service") or {}).get("name"),
        "name": node.get("name"),
        "type": node.get("type"),
        "outcome": (doc.get("event") or {}).get("outcome"),
        "durationUs": duration.get("us"),
    }
    timestamp_us = (doc.get("timestamp") or {}).get("us")
    if timestamp_us is not None:
        compacted["timestampUs"] = timestamp_us
    return compacted


def attach_offset_us(items: List[Dict[str, Any]], entry_transaction_id: str) -> List[Dict[str, Any]]:
    """Смещение старта от входной транзакции. Порядок элементов не меняется."""
    origin = _entry_timestamp_us(items, entry_transaction_id)
    if origin is None:
        stamps = [item["timestampUs"] for item in items if item.get("timestampUs") is not None]
        origin = min(stamps) if stamps else None
    for item in items:
        stamp = item.pop("timestampUs", None)
        if origin is None or stamp is None:
            continue
        item["offsetUs"] = stamp - origin
    return items


def _entry_timestamp_us(items: List[Dict[str, Any]], entry_transaction_id: str) -> Optional[int]:
    for item in items:
        if item.get("event") != "transaction" or item.get("id") != entry_transaction_id:
            continue
        if item.get("timestampUs") is None:
            return None
        return item["timestampUs"]
    return None


def cap_items(items: List[Any], limit: int) -> Tuple[List[Any], bool]:
    bounded = max(1, min(int(limit), 100))
    return items[:bounded], len(items) > bounded


class KibanaApmClient:
    """Тонкая обёртка над /internal/apm Kibana. Ошибки HTTP не превращаются в пустой список."""

    def __init__(self, http: Optional[httpx.AsyncClient] = None):
        base_url = os.getenv("KIBANA_BASE_URL", "").strip().rstrip("/")
        username = os.getenv("APM_USERNAME")
        password = os.getenv("APM_PASSWORD")
        if not base_url:
            raise RuntimeError("KIBANA_BASE_URL не задан")
        if not username or not password:
            raise RuntimeError("APM_USERNAME или APM_PASSWORD не заданы")
        self.base_url = base_url
        self._auth = (username, password)
        self._timeout = float(os.getenv("APM_TIMEOUT", "30"))
        self._http = http

    async def list_services(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
        environment: str = "ENVIRONMENT_ALL",
        kuery: str = "",
        limit: int = 40,
    ) -> Dict[str, Any]:
        start_iso, end_iso = resolve_window(start, end)
        payload = await self._get(SERVICES_PATH, {
            "start": start_iso,
            "end": end_iso,
            "environment": environment,
            "kuery": kuery,
            "documentType": "serviceTransactionMetric",
            "probability": "1",
            "rollupInterval": "1m",
        })
        items = [compact_service(item) for item in payload.get("items") or []]
        items.sort(key=lambda item: item.get("throughputPerMinute") or 0, reverse=True)
        returned, truncated = cap_items(items, limit)
        return {
            "start": start_iso,
            "end": end_iso,
            "environment": environment,
            "total": len(items),
            "truncated": truncated,
            "services": returned,
        }

    async def list_transactions(
        self,
        service_name: str,
        transaction_type: str = "request",
        latency_aggregation_type: str = "avg",
        start: Optional[str] = None,
        end: Optional[str] = None,
        environment: str = "ENVIRONMENT_ALL",
        kuery: str = "",
        limit: int = 30,
    ) -> Dict[str, Any]:
        _require_service_name(service_name)
        if latency_aggregation_type not in ("avg", "p95", "p99"):
            raise ValueError("latency_aggregation_type должен быть avg, p95 или p99")
        start_iso, end_iso = resolve_window(start, end)
        path = f"/internal/apm/services/{quote(service_name, safe='')}/transactions/groups/main_statistics"
        payload = await self._get(path, {
            "start": start_iso,
            "end": end_iso,
            "environment": environment,
            "kuery": kuery,
            "transactionType": transaction_type,
            "latencyAggregationType": latency_aggregation_type,
            "useDurationSummary": "true",
            "documentType": "transactionMetric",
            "rollupInterval": "1m",
        })
        groups = [compact_transaction_group(item) for item in payload.get("transactionGroups") or []]
        returned, truncated = cap_items(groups, limit)
        return {
            "serviceName": service_name,
            "transactionType": transaction_type,
            "latencyAggregationType": latency_aggregation_type,
            "start": start_iso,
            "end": end_iso,
            "environment": environment,
            "total": len(groups),
            "truncated": truncated or bool(payload.get("maxTransactionGroupsExceeded")),
            "transactionOverflowCount": payload.get("transactionOverflowCount"),
            "transactions": returned,
        }

    async def list_errors(
        self,
        service_name: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        environment: str = "ENVIRONMENT_ALL",
        kuery: str = "",
        limit: int = 30,
    ) -> Dict[str, Any]:
        _require_service_name(service_name)
        start_iso, end_iso = resolve_window(start, end)
        path = f"/internal/apm/services/{quote(service_name, safe='')}/errors/groups/main_statistics"
        payload = await self._get(path, {
            "start": start_iso,
            "end": end_iso,
            "environment": environment,
            "kuery": kuery,
        })
        groups = list(payload.get("errorGroups") or [])
        returned, truncated = cap_items(groups, limit)
        return {
            "serviceName": service_name,
            "start": start_iso,
            "end": end_iso,
            "environment": environment,
            "total": len(groups),
            "truncated": truncated,
            "errors": returned,
        }

    async def list_trace_samples(
        self,
        service_name: str,
        transaction_name: str,
        transaction_type: str = "request",
        start: Optional[str] = None,
        end: Optional[str] = None,
        environment: str = "ENVIRONMENT_ALL",
        kuery: str = "",
        min_duration_ms: Optional[float] = None,
        max_duration_ms: Optional[float] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        _require_service_name(service_name)
        if not transaction_name or not str(transaction_name).strip():
            raise ValueError("transaction_name обязателен")
        if min_duration_ms is not None and max_duration_ms is not None and min_duration_ms > max_duration_ms:
            raise ValueError("min_duration_ms не может быть больше max_duration_ms")
        start_iso, end_iso = resolve_window(start, end)
        params = {
            "start": start_iso,
            "end": end_iso,
            "environment": environment,
            "kuery": kuery,
            "transactionName": transaction_name,
            "transactionType": transaction_type,
        }
        # Kibana ждёт диапазон длительности в микросекундах и обе границы сразу.
        if min_duration_ms is not None or max_duration_ms is not None:
            params["sampleRangeFrom"] = str(int((min_duration_ms or 0) * 1000))
            params["sampleRangeTo"] = str(int((max_duration_ms if max_duration_ms is not None else 86_400_000) * 1000))
        path = f"/internal/apm/services/{quote(service_name, safe='')}/transactions/traces/samples"
        payload = await self._get(path, params)
        samples = [
            {
                "timestamp": item.get("timestamp"),
                "traceId": item.get("traceId"),
                "transactionId": item.get("transactionId"),
            }
            for item in payload.get("traceSamples") or []
        ]
        returned, truncated = cap_items(samples, limit)
        return {
            "serviceName": service_name,
            "transactionName": transaction_name,
            "start": start_iso,
            "end": end_iso,
            "total": len(samples),
            "truncated": truncated,
            "samples": returned,
        }

    async def get_trace(
        self,
        trace_id: str,
        entry_transaction_id: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not trace_id or not entry_transaction_id:
            raise ValueError("нужны trace_id и entry_transaction_id")
        start_iso, end_iso = resolve_window(start, end)
        payload = await self._get(f"/internal/apm/traces/{quote(trace_id, safe='')}", {
            "start": start_iso,
            "end": end_iso,
            "entryTransactionId": entry_transaction_id,
        })
        trace_items = payload.get("traceItems") or {}
        docs = [compact_trace_doc(doc) for doc in trace_items.get("traceDocs") or []]
        attach_offset_us(docs, entry_transaction_id)
        returned, truncated = cap_items(docs, TRACE_ITEM_LIMIT)
        return {
            "traceId": trace_id,
            "entryTransactionId": entry_transaction_id,
            "start": start_iso,
            "end": end_iso,
            "exceedsMax": bool(trace_items.get("exceedsMax")),
            "traceItemCount": trace_items.get("traceItemCount"),
            "truncated": truncated,
            "items": returned,
        }

    async def _get(self, path: str, params: Dict[str, str]) -> Dict[str, Any]:
        owns_client = self._http is None
        client = self._http or httpx.AsyncClient(timeout=self._timeout, auth=self._auth)
        try:
            response = await client.get(
                f"{self.base_url}{path}",
                params=params,
                headers={"kbn-xsrf": "true"},
            )
        finally:
            if owns_client:
                await client.aclose()
        if response.status_code >= 400:
            raise RuntimeError(_http_error(response))
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Kibana APM вернул не объект: HTTP {response.status_code}")
        return payload


def _require_service_name(service_name: str) -> None:
    if not service_name or not str(service_name).strip():
        raise ValueError("service_name обязателен")


def _http_error(response: httpx.Response) -> str:
    message = ""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            message = str(payload.get("message") or payload.get("error") or "")
    except Exception:
        message = ""
    if not message:
        message = (response.text or "")[:300]
    return f"Kibana APM HTTP {response.status_code}: {message}"


def _parse_iso(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise ValueError(f"некорректное время: {value}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
