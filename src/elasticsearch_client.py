#!/usr/bin/env python3
"""
Клиент для работы с Elasticsearch
"""

import os
import logging
from typing import Dict, Any, List, Optional
from elasticsearch import AsyncElasticsearch, exceptions as es_exceptions

logger = logging.getLogger(__name__)

class ElasticsearchManager:
    """Менеджер для работы с Elasticsearch"""

    def __init__(self):
        self.client = AsyncElasticsearch(
            hosts=[os.getenv("APM_BASE_URL")],
            basic_auth=(os.getenv("APM_USERNAME"), os.getenv("APM_PASSWORD")),
            request_timeout=int(os.getenv("APM_TIMEOUT", "30"))
        )

    async def list_indexes(self, index_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Список индексов и их описание"""
        result = []

        for idx, data in index_config.items():
            fields_info = {}
            events = data.get("events", [])
            fields = data.get("fields", {})

            # Обрабатываем поля из раздела fields
            for field_name, field_config in fields.items():
                alias = field_config.get("alias")
                # Используем алиас для display_name, если есть, иначе оригинальное имя
                display_name = alias if alias else field_name
                description = field_config.get("description", "")

                # В качестве ключа используем алиас если есть, иначе оригинальное имя
                key_name = alias if alias else field_name

                fields_info[key_name] = {
                    "original_name": field_name,
                    "description": description,
                    "alias": alias,
                    "need_dedupe": field_config.get("need_dedupe", False)
                }

            result.append({
                "name": idx,
                "fields": fields_info,
                "events": events
            })
        return result

    async def query_index(self, index: str, filters: Dict[str, Any], size: int = 100,
                         from_: int = 0, sort: Optional[List[Dict]] = None,
                         index_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Выполняет запрос к индексу Elasticsearch"""
        if index_config and index not in index_config:
            raise ValueError(f"Индекс '{index}' не найден в конфигурации")

        # Преобразуем алиасы обратно в оригинальные имена полей
        if index_config and index in index_config:
            filters = self._resolve_aliases_in_filters(filters, index_config[index])
            if sort:
                sort = self._resolve_aliases_in_sort(sort, index_config[index])

        query_body = self._build_query(filters, size, from_, sort)

        logger.info(f"Elasticsearch query to index '{index}': {query_body}")
        try:
            result = await self.client.search(index=index, body=query_body)
            return result.body
        except es_exceptions.AuthenticationException:
            raise RuntimeError("Ошибка аутентификации Elasticsearch")
        except es_exceptions.ConnectionError:
            raise RuntimeError("Ошибка подключения к Elasticsearch")
        except Exception as e:
            raise RuntimeError(f"Ошибка Elasticsearch: {e}")

    async def find_trace_entry_transaction(
        self, trace_id: str, start: str, end: str
    ) -> Optional[str]:
        """Корневая transaction.id трейса в traces-apm*. None — документов нет."""
        if not trace_id:
            raise ValueError("trace_id обязателен")
        window = {"range": {"@timestamp": {"gte": start, "lte": end}}}
        trace_filter = [
            {"term": {"trace.id": trace_id}},
            {"term": {"processor.event": "transaction"}},
            window,
        ]
        root_hits = await self._search_trace_transactions({
            "bool": {
                "filter": trace_filter,
                "must_not": [{"exists": {"field": "parent.id"}}],
            }
        })
        if root_hits:
            return _transaction_id(root_hits[0])
        any_hits = await self._search_trace_transactions({"bool": {"filter": trace_filter}})
        if not any_hits:
            return None
        return _transaction_id(any_hits[0])

    async def _search_trace_transactions(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        body = {
            "size": 1,
            "_source": ["transaction.id"],
            "sort": [{"@timestamp": "asc"}],
            "query": query,
        }
        try:
            result = await self.client.search(index="traces-apm*", body=body)
        except es_exceptions.AuthenticationException:
            raise RuntimeError("Ошибка аутентификации Elasticsearch")
        except es_exceptions.ConnectionError:
            raise RuntimeError("Ошибка подключения к Elasticsearch")
        except Exception as e:
            raise RuntimeError(f"Ошибка Elasticsearch: {e}")
        hits = result.body.get("hits", {}).get("hits", [])
        return hits

    def _build_query(self, filters: Dict[str, Any], size: int, from_: int,
                    sort: Optional[List[Dict]]) -> Dict[str, Any]:
        """Строит тело запроса к Elasticsearch"""
        # Список поддерживаемых типов запросов Elasticsearch
        es_query_types = ["bool", "match", "match_phrase", "term", "terms", "range", "wildcard", "regexp", "fuzzy", "prefix", "exists"]

        if any(query_type in filters for query_type in es_query_types):
            query_body = {
                "query": filters,
                "size": size,
                "from": from_
            }
        else:
            query_body = {
                "query": {
                    "bool": {
                        "must": []
                    }
                },
                "size": size,
                "from": from_
            }
            for key, value in filters.items():
                if isinstance(value, dict) and ("gte" in value or "lte" in value):
                    query_body["query"]["bool"]["must"].append({
                        "range": {key: value}
                    })
                else:
                    query_body["query"]["bool"]["must"].append({
                        "term": {key: value}
                    })

        if sort:
            query_body["sort"] = sort

        return query_body

    def _resolve_aliases_in_filters(self, filters: Dict[str, Any], index_config: Dict[str, Any]) -> Dict[str, Any]:
        """Преобразует алиасы полей в оригинальные имена в фильтрах"""
        fields_config = index_config.get("fields", {})
        alias_to_original = {}

        # Создаём маппинг алиас -> оригинальное имя
        for original_name, field_config in fields_config.items():
            alias = field_config.get("alias")
            if alias:
                alias_to_original[alias] = original_name

        return self._replace_field_names_recursive(filters, alias_to_original)

    def _resolve_aliases_in_sort(self, sort: List[Dict], index_config: Dict[str, Any]) -> List[Dict]:
        """Преобразует алиасы полей в оригинальные имена в сортировке"""
        fields_config = index_config.get("fields", {})
        alias_to_original = {}

        # Создаём маппинг алиас -> оригинальное имя
        for original_name, field_config in fields_config.items():
            alias = field_config.get("alias")
            if alias:
                alias_to_original[alias] = original_name

        new_sort = []
        for sort_item in sort:
            new_sort.append(self._replace_field_names_recursive(sort_item, alias_to_original))

        return new_sort

    def _replace_field_names_recursive(self, obj: Any, alias_mapping: Dict[str, str]) -> Any:
        """Рекурсивно заменяет имена полей в объекте"""
        if isinstance(obj, dict):
            new_obj = {}
            for key, value in obj.items():
                # Заменяем ключ если он есть в маппинге
                new_key = alias_mapping.get(key, key)
                new_obj[new_key] = self._replace_field_names_recursive(value, alias_mapping)
            return new_obj
        elif isinstance(obj, list):
            return [self._replace_field_names_recursive(item, alias_mapping) for item in obj]
        else:
            return obj


def _transaction_id(hit: Dict[str, Any]) -> str:
    transaction = (hit.get("_source") or {}).get("transaction") or {}
    transaction_id = transaction.get("id")
    if not transaction_id:
        raise RuntimeError("В документе трейса нет transaction.id")
    return transaction_id