# MCP APM Server

Сервер MCP (Model Context Protocol) для безопасного доступа к APM логам Skyeng Platform через Elasticsearch API с продвинутой обработкой данных.

## 🚀 Возможности

### 📊 Анализ данных
- 🔍 **Поиск логов** по произвольным фильтрам с автоматической очисткой данных
- 📋 **Схема полей** и событий индексов (из index.yaml)
- 🎯 **Алиасы полей** - упрощенные названия для сложных путей
- 📦 **Парсинг массивов** - извлечение данных из массивов с синтаксисом `field[].subfield`
- 🧹 **Очистка данных** - компактные ответы только с нужными полями


## 🛠 MCP Инструменты

### 1. `list_indexes` - Список индексов
Получает список доступных индексов и их схему полей с алиасами.

**Пример ответа:**
```json
[
  {
    "name": "logs_videocall",
    "fields": {
      "issueReason": {
        "original_name": "details.issues[].reason",
        "description": "Причина проблемы",
        "alias": "issueReason",
        "need_dedupe": true
      },
      "mos": {
        "original_name": "details.summary.publisher.publisherMos.mos",
        "description": "МОС",
        "alias": "mos"
      }
    },
    "events": [
      {"tech-summary-minute": "отчет по МОС за минуту"},
      {"webrtcIssue": "проблемы с WebRTC, аудио, видео, скриншеринг"}
    ]
  }
]
```

### 2. `list_apm_services`, `list_apm_transactions`, `list_apm_errors`, `list_apm_trace_samples`, `get_apm_trace`

Сводка Kibana APM (`KIBANA_BASE_URL`, внутренние роуты `/internal/apm/*`). Это не поиск по `index.yaml`.

`APM_BASE_URL` — адрес Elasticsearch (`:9200`). `https://apm.skyeng.link` туда подставлять нельзя: это Kibana.

- `list_apm_services` — сервисы за окно: `latencyMs`, `errorRate`, `throughputPerMinute`
- `list_apm_transactions` — группы транзакций сервиса
- `list_apm_errors` — группы ошибок сервиса. Пустой список значит, что ошибок нет
- `list_apm_trace_samples` — `traceId` и `transactionId` сэмплов одной группы транзакций, по ним открывается `get_apm_trace`. `min_duration_ms` / `max_duration_ms` оставляют только медленные запросы. Сэмплируется не каждый запрос, пустой список не значит, что транзакций не было
- `get_apm_trace` — водопад трейса. `offsetUs` — старт спана в микросекундах от входной транзакции, порядок элементов при этом не хронологический. Без `entry_transaction_id` корневая транзакция ищется в `traces-apm*`. `truncated` или `exceedsMax` — водопад неполный

`query_index` по-прежнему принимает только индексы из `index.yaml`.

### 3. `query_index` - Поиск данных
Выполняет поиск с автоматической обработкой и очисткой данных.

**Параметры:**
- `index` - имя индекса
- `filters` - фильтры запроса (term/range/bool)
- `size` - размер выборки (по умолчанию 100)
- `from_` - смещение для пагинации
- `sort` - сортировка

**Пример запроса:**
```json
{
  "index": "logs_videocall",
  "filters": {
    "bool": {
      "must": [
        {"term": {"event": "webrtcIssue"}},
        {"term": {"appSessionId": "hofexozakone"}}
      ]
    }
  },
  "size": 10,
  "sort": [{"@timestamp": {"order": "desc"}}]
}
```

**Пример очищенного ответа:**
```json
{
  "hits": {
    "hits": [
      {
        "_source": {
          "@timestamp": "2025-06-12T10:48:50.793950442Z",
          "userId": 5614788,
          "userRole": "admin",
          "event": "webrtcIssue",
          "appSessionId": "hofexozakone",
          "issueReason": "inbound-network-quality"
        }
      }
    ]
  }
}
```


### 4. `get_data_retention_info` - Информация о данных
Показывает доступный период данных (логи хранятся 20 дней).

## 🎯 Продвинутые возможности

### Парсинг массивов
Система поддерживает извлечение данных из массивов:

```yaml
# index.yaml
"details.issues[].reason":     # Извлекает reason из всех элементов массива issues
  alias: issueReason
  need_dedupe: true

"details.issues[0].reason":    # Извлекает reason из первого элемента
  alias: firstIssueReason
```

### Алиасы и очистка данных
- **До обработки**: ~2000+ символов с десятками полей
- **После обработки**: ~200 символов только с нужными полями
- **Сокращение размера**: в 10+ раз

### Автоматическая дедупликация
```json
// Исходные данные
"details.issues": [
  {"reason": "network-issue"},
  {"reason": "server-issue"},
  {"reason": "network-issue"}
]

// Результат
"issueReason": "network-issue, server-issue"
```

## 📈 Примеры использования

### Анализ проблем WebRTC
```python
# Поиск проблем в конкретной комнате
query_index(
  index="logs_videocall",
  filters={"bool": {"must": [
    {"term": {"event": "webrtcIssue"}},
    {"term": {"appSessionId": "room123"}}
  ]}},
  size=50
)

# Результат: компактные данные с алиасом issueReason
```

### Анализ качества связи (МОС)
```python
# Поиск данных МОС
query_index(
  index="logs_videocall",
  filters={"term": {"event": "tech-summary-minute"}},
  size=100
)

# Результат: данные с алиасами mos, avgJitter, rtt, packetsLoss
```


## 🚀 Быстрая установка

```bash
# Клонируем репозиторий
git clone https://github.com/CynepHy6/mcp-apm.git mcp-apm
cd mcp-apm

# Автоматическая установка
python3 setup.py    # Linux/macOS
python setup.py     # Windows
```

## ⚙️ Конфигурация

### Переменные окружения (.env)
```bash
APM_BASE_URL=http://elasticsearch-host:9200
KIBANA_BASE_URL=https://apm.skyeng.link
APM_USERNAME=your_username
APM_PASSWORD=your_password
APM_TIMEOUT=30
```

### Конфигурация полей (index.yaml)
```yaml
logs_videocall:
  events:
    - tech-summary-minute: отчет по МОС за минуту
    - webrtcIssue: проблемы с WebRTC
  fields:
    "@timestamp":
      description: "Время события"
    "details.issues[].reason":
      description: "Причина проблемы"
      alias: issueReason
      need_dedupe: true
    "details.summary.publisher.publisherMos.mos":
      description: "МОС"
      alias: mos
```

## 🧪 Тестирование

```bash
# Запуск всех тестов
python -m pytest tests/ -v

# Тесты парсинга массивов
python -m pytest tests/test_data_processing.py -k "array" -v

# Тесты алиасов
python -m pytest tests/test_data_processing.py::TestDataProcessing::test_array_aliases -v
```

## 🔧 Устранение неполадок

- **401** — ошибка аутентификации: проверьте креды в .env
- **404** — индекс не найден: проверьте название в index.yaml и правильный адрес **APM_BASE_URL**
- **503** — ошибка соединения: проверьте APM_BASE_URL
- **Пустые алиасы** — проверьте пути полей в index.yaml
- **Графики не создаются** — убедитесь что matplotlib установлен

## 📋 Требования

- Python 3.8+
- Доступ к APM Skyeng Platform

## 📄 Лицензия

Внутренний инструмент Skyeng Platform