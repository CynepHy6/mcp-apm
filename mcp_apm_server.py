#!/usr/bin/env python3
"""
MCP сервер для безопасной работы с Elasticsearch (APM)
"""

import os
import logging
import json
import asyncio
from typing import List
from datetime import datetime, timedelta
from dotenv import load_dotenv
from mcp.server import Server
from mcp.types import Tool, TextContent
import mcp.server.stdio

from src.config_utils import load_index_config
from src.elasticsearch_client import ElasticsearchManager
from src.data_processing import process_elasticsearch_data
from src.kibana_apm_client import KibanaApmClient, resolve_window
from src.plotting import PlotManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcp-apm-server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Загружаем конфигурацию
INDEX_CONFIG = load_index_config()

# Инициализируем менеджеры
es_manager = ElasticsearchManager()
plot_manager = PlotManager()
server = Server("apm-server")

def get_data_retention_info():
    """Получить информацию о доступном периоде данных"""
    now = datetime.now()
    retention_days = 20
    oldest_available = now - timedelta(days=retention_days)
    return f"ВАЖНО: логи хранятся не более {retention_days} дней. Данные доступны с {oldest_available.strftime('%Y-%m-%d')} по {now.strftime('%Y-%m-%d')}. Поиск данных старше этого периода не даст результатов"

@server.list_tools()
async def list_tools() -> list[Tool]:
    """Список доступных инструментов MCP"""

    tools = [
        Tool(
            name="list_indexes",
            description="Получить список доступных индексов и их описание",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="get_data_retention_info",
            description="Получить информацию о доступном периоде данных в Elasticsearch. ВАЖНО: логи хранятся не более 20 дней. Если наобходимо получить данные по дате, то сначала надо получить информацию о доступном периоде данных",
            inputSchema={"type": "object", "properties": {}}
        ),
        Tool(
            name="query_index",
            description="Выполнить запрос к индексу Elasticsearch с поддержкой стандартных Elasticsearch запросов",
            inputSchema={
                "type": "object",
                "properties": {
                    "index": {"type": "string", "description": "Имя индекса"},
                    "filters": {
                        "type": "object",
                        "description": "Фильтры запроса в формате Elasticsearch. Поддерживаются: match_phrase (для поиска фраз), match (для поиска слов), term/terms (точное совпадение), range (диапазоны), wildcard (с *, но лучше использовать .keyword поля), bool (комбинированные запросы), exists, prefix, fuzzy, regexp. Примеры: {\"match_phrase\": {\"message\": \"User has been notified\"}}, {\"range\": {\"@timestamp\": {\"gte\": \"now-1d\"}}}, {\"bool\": {\"must\": [{\"match_phrase\": {\"message\": \"error\"}}, {\"range\": {\"@timestamp\": {\"gte\": \"now-3h\"}}}]}}"
                    },
                    "size": {"type": "integer", "description": "Размер выборки", "default": 100},
                    "from_": {"type": "integer", "description": "Смещение", "default": 0},
                    "sort": {"type": "array", "items": {"type": "object"}, "description": "Сортировка"}
                },
                "required": ["index", "filters"]
            }
        ),
        Tool(
            name="list_apm_services",
            description="Сервисы Kibana APM за окно времени: latencyMs, errorRate, throughputPerMinute. Это не query_index и не index.yaml. latency в Kibana хранится в микросекундах, в ответе уже миллисекунды. throughputPerMinute — как в UI APM, транзакций в минуту.",
            inputSchema={
                "type": "object",
                "properties": {
                    "start": {"type": "string", "description": "Начало окна, ISO-8601. Пусто — 15 минут до end"},
                    "end": {"type": "string", "description": "Конец окна, ISO-8601. Пусто — сейчас"},
                    "environment": {"type": "string", "description": "ENVIRONMENT_ALL, ENVIRONMENT_NOT_DEFINED или имя окружения", "default": "ENVIRONMENT_ALL"},
                    "kuery": {"type": "string", "description": "KQL-фильтр Kibana", "default": ""},
                    "limit": {"type": "integer", "description": "Сколько сервисов вернуть, 1..100", "default": 40}
                }
            }
        ),
        Tool(
            name="list_apm_transactions",
            description="Группы транзакций одного сервиса Kibana APM: имя, latencyMs, errorRate, throughputPerMinute, impact. Пустой список — за окно групп нет, это не ошибка HTTP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_name": {"type": "string", "description": "Имя сервиса, как в APM"},
                    "transaction_type": {"type": "string", "description": "Тип транзакции", "default": "request"},
                    "latency_aggregation_type": {"type": "string", "enum": ["avg", "p95", "p99"], "default": "avg"},
                    "start": {"type": "string", "description": "Начало окна, ISO-8601"},
                    "end": {"type": "string", "description": "Конец окна, ISO-8601"},
                    "environment": {"type": "string", "default": "ENVIRONMENT_ALL"},
                    "kuery": {"type": "string", "default": ""},
                    "limit": {"type": "integer", "default": 30}
                },
                "required": ["service_name"]
            }
        ),
        Tool(
            name="list_apm_errors",
            description="Группы ошибок сервиса Kibana APM: groupId, name, occurrences, culprit, type. Пустой список — ошибок за окно нет.",
            inputSchema={
                "type": "object",
                "properties": {
                    "service_name": {"type": "string", "description": "Имя сервиса, как в APM"},
                    "start": {"type": "string", "description": "Начало окна, ISO-8601"},
                    "end": {"type": "string", "description": "Конец окна, ISO-8601"},
                    "environment": {"type": "string", "default": "ENVIRONMENT_ALL"},
                    "kuery": {"type": "string", "default": ""},
                    "limit": {"type": "integer", "default": 30}
                },
                "required": ["service_name"]
            }
        ),
        Tool(
            name="get_apm_trace",
            description="Водопад одного трейса из Kibana APM: транзакции и спаны без сырого документа. offsetUs — микросекунды от старта входной транзакции; меньше значит раньше. Список по-прежнему не хронологический: его порядок — по длительности. Если entry_transaction_id не передан, корневая транзакция ищется в traces-apm*. Окно start/end должно накрывать трейс. exceedsMax или truncated — водопад обрезан, это не полный трейс.",
            inputSchema={
                "type": "object",
                "properties": {
                    "trace_id": {"type": "string", "description": "trace.id"},
                    "entry_transaction_id": {"type": "string", "description": "transaction.id входа. Пусто — найти корневую транзакцию в Elasticsearch"},
                    "start": {"type": "string", "description": "Начало окна, ISO-8601"},
                    "end": {"type": "string", "description": "Конец окна, ISO-8601"}
                },
                "required": ["trace_id"]
            }
        )
    ]

    if plot_manager.is_available():
        tools.append(
            Tool(
                name="create_plot",
                description="Создать график по данным из Elasticsearch",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "index": {"type": "string", "description": "Имя индекса"},
                        "filters": {"type": "object", "description": "Фильтры запроса"},
                        "plot_type": {
                            "type": "string",
                            "enum": ["line", "scatter", "bar", "mos_timeline", "metrics_comparison"],
                            "description": "Тип графика"
                        },
                        "x_field": {"type": "string", "description": "Поле для оси X (обычно @timestamp)"},
                        "y_field": {"type": "string", "description": "Поле для оси Y"},
                        "group_by": {"type": "string", "description": "Поле для группировки (например, userId)", "default": None},
                        "title": {"type": "string", "description": "Заголовок графика", "default": "График"},
                        "size": {"type": "integer", "description": "Размер выборки", "default": 100}
                    },
                    "required": ["index", "filters", "plot_type", "x_field", "y_field"]
                }
            )
        )

    return tools

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Обработчик вызовов инструментов MCP"""
    try:
        if name == "list_indexes":
            result = await es_manager.list_indexes(INDEX_CONFIG)
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]

        elif name == "get_data_retention_info":
            return [TextContent(type="text", text=get_data_retention_info())]

        elif name == "query_index":
            index = arguments.get("index")
            filters = arguments.get("filters", {})
            size = arguments.get("size", 100)
            from_ = arguments.get("from_", 0)
            sort = arguments.get("sort")

            result = await es_manager.query_index(
                index, filters, size, from_, sort, INDEX_CONFIG
            )

            # Обработка данных: дедупликация + алиасы
            result = process_elasticsearch_data(result, index, INDEX_CONFIG)

            logger.info(f"Query result size: {len(str(result))} characters")
            if isinstance(result, dict) and 'hits' in result:
                logger.info(f"Hits count: {len(result['hits']['hits'])}")

            # Добавляем информацию о ретенции в начало ответа
            retention_info = get_data_retention_info()
            result_text = f"{retention_info}\n\n" + json.dumps(result, ensure_ascii=False, indent=2, default=str)
            return [TextContent(type="text", text=result_text)]

        elif name == "create_plot":
            if not plot_manager.is_available():
                return [TextContent(type="text", text="Ошибка: matplotlib/pandas не установлены")]

            index = arguments.get("index")
            filters = arguments.get("filters", {})
            plot_type = arguments.get("plot_type")
            x_field = arguments.get("x_field")
            y_field = arguments.get("y_field")
            group_by = arguments.get("group_by")
            title = arguments.get("title", "График")
            size = arguments.get("size", 100)

            # Получаем данные из Elasticsearch
            result = await es_manager.query_index(
                index, filters, size, 0, [{"@timestamp": {"order": "asc"}}], INDEX_CONFIG
            )

            # Обработка данных: дедупликация + алиасы
            result = process_elasticsearch_data(result, index, INDEX_CONFIG)

            # Создаем график
            plot_result = await plot_manager.create_plot_from_data(
                result, plot_type, x_field, y_field, group_by, title
            )

            # Добавляем информацию о ретенции в начало ответа
            retention_info = get_data_retention_info()
            final_result = f"{retention_info}\n\n{plot_result}"
            return [TextContent(type="text", text=final_result)]

        elif name == "list_apm_services":
            args = arguments or {}
            result = await KibanaApmClient().list_services(
                start=args.get("start"),
                end=args.get("end"),
                environment=args.get("environment") or "ENVIRONMENT_ALL",
                kuery=args.get("kuery") or "",
                limit=args.get("limit") or 40,
            )
            return [_json_content(result)]

        elif name == "list_apm_transactions":
            args = arguments or {}
            result = await KibanaApmClient().list_transactions(
                service_name=args.get("service_name"),
                transaction_type=args.get("transaction_type") or "request",
                latency_aggregation_type=args.get("latency_aggregation_type") or "avg",
                start=args.get("start"),
                end=args.get("end"),
                environment=args.get("environment") or "ENVIRONMENT_ALL",
                kuery=args.get("kuery") or "",
                limit=args.get("limit") or 30,
            )
            return [_json_content(result)]

        elif name == "list_apm_errors":
            args = arguments or {}
            result = await KibanaApmClient().list_errors(
                service_name=args.get("service_name"),
                start=args.get("start"),
                end=args.get("end"),
                environment=args.get("environment") or "ENVIRONMENT_ALL",
                kuery=args.get("kuery") or "",
                limit=args.get("limit") or 30,
            )
            return [_json_content(result)]

        elif name == "get_apm_trace":
            args = arguments or {}
            trace_id = args.get("trace_id")
            entry_transaction_id = args.get("entry_transaction_id")
            start = args.get("start")
            end = args.get("end")
            if not entry_transaction_id:
                start, end = resolve_window(start, end)
                entry_transaction_id = await es_manager.find_trace_entry_transaction(trace_id, start, end)
                if not entry_transaction_id:
                    raise RuntimeError(
                        "Корневая транзакция трейса в traces-apm* не найдена. "
                        "Расширьте окно start/end или передайте entry_transaction_id"
                    )
            result = await KibanaApmClient().get_trace(
                trace_id=trace_id,
                entry_transaction_id=entry_transaction_id,
                start=start,
                end=end,
            )
            return [_json_content(result)]

        else:
            return [TextContent(type="text", text=f"Неизвестный инструмент: {name}")]

    except Exception as e:
        logger.error(f"Ошибка выполнения инструмента {name}: {e}")
        return [TextContent(type="text", text=f"Ошибка: {str(e)}")]

def _json_content(payload: dict) -> TextContent:
    return TextContent(
        type="text",
        text=json.dumps(payload, ensure_ascii=False, indent=2, default=str),
    )


def show_help():
    """Показывает справку по использованию"""
    print("MCP сервер для работы с Elasticsearch (APM) - РЕФАКТОРЕННАЯ ВЕРСИЯ\n")
    print("Использование:")
    print("  python mcp_apm_server_refactored.py     - Запуск MCP сервера")
    print("  python mcp_apm_server_refactored.py --help - Показать справку\n")
    print("Доступные инструменты MCP:")
    print("  • list_indexes   - Список индексов и их описание")
    print("  • get_data_retention_info - Получить информацию о доступном периоде данных")
    print("  • query_index    - Выполнить запрос к индексу Elasticsearch")
    print("  • list_apm_services - Сервисы Kibana APM")
    print("  • list_apm_transactions - Группы транзакций сервиса")
    print("  • list_apm_errors - Группы ошибок сервиса")
    print("  • get_apm_trace  - Водопад одного трейса")
    if plot_manager.is_available():
        print("  • create_plot    - Создать график по данным из Elasticsearch")
    else:
        print("  • create_plot    - НЕДОСТУПНО (нет matplotlib/pandas)")

    print("\nИнформация о данных:")
    retention_info = get_data_retention_info()
    print(f"  {retention_info}")

    print("\nСтруктура модулей:")
    print("  • src/config_utils.py      - утилиты конфигурации")
    print("  • src/elasticsearch_client.py - клиент Elasticsearch")
    print("  • src/kibana_apm_client.py - клиент Kibana APM")
    print("  • src/data_processing.py   - обработка данных")
    print("  • src/plotting.py          - создание графиков")

async def main():
    """Главная функция запуска сервера"""
    import sys
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ['--help', '-h']:
            show_help()
            return
        else:
            print(f"Неизвестный аргумент: {arg}")
            print("Используйте --help для справки")
            return

    logger.info("Запуск рефакторенного MCP сервера для работы с Elasticsearch (APM)")
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())