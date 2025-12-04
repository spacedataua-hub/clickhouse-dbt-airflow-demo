import requests
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import pytz
from clickhouse_driver import Client

# ==============================
# Настройка логирования
# ==============================
# Логирование помогает отслеживать процесс выполнения программы:
# - DEBUG: подробные сообщения для отладки
# - INFO: общая информация о ходе работы
# - WARNING: предупреждения
# - ERROR/CRITICAL: ошибки
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ==============================
# Вспомогательная функция: получить вчерашнюю дату
# ==============================
# Affise API требует параметры date_from и date_to.
# Мы берём вчерашнюю дату в формате YYYY-MM-DD, чтобы выгружать данные за прошлый день.
def get_yesterday():
    return (datetime.now(pytz.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

# ==============================
# Функция: запрос данных из API Affise
# ==============================
# Здесь мы:
# 1. Формируем URL и заголовки (API-Key).
# 2. Указываем параметры (диапазон дат, постраничная выборка).
# 3. Делаем GET-запросы и собираем все страницы в список.
def get_affise_data():
    url = "http://.affise.com/3.0/stats/serverpostbacks"
    headers = {"API-Key": "f9c39f53777"}
    params = {
        "date_from": get_yesterday(),
        "date_to": get_yesterday(),
        "page": 1,
        "limit": 500,
        "supplier[]": ["65fc16870c7938"]
    }

    all_data = []  # общий список для всех постбеков

    while True:
        logger.debug(f"Запрос к {url} с параметрами {params}")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            postbacks = response.json().get("postbacks", [])
            if not postbacks:
                logger.debug("Нет данных в 'postbacks'. Завершаем цикл.")
                break
            all_data.extend(postbacks)
            logger.debug(f"Получено {len(postbacks)} записей.")

            # Если меньше лимита (500), значит это последняя страница
            if len(postbacks) < 500:
                break
            params["page"] += 1
        else:
            logger.error(f"Ошибка при запросе: {response.status_code}, {response.text}")
            raise Exception(f"Error fetching data: {response.status_code} {response.text}")

    return all_data

# ==============================
# Функция: создать таблицу в ClickHouse (если её нет)
# ==============================
# Здесь мы описываем схему таблицы:
# - Колонки соответствуют полям из API Affise
# - ENGINE = MergeTree — стандартный движок для хранения
# - ORDER BY id — упорядочивание по ключу
def ensure_table_exists(client):
    client.execute('''
        CREATE TABLE IF NOT EXISTS affise_postbacks (
            id String,
            get String,
            post String,
            server String,
            supplier String,
            date String,
            response String,
            track String,
            ba_date Date
        ) ENGINE = MergeTree()
        ORDER BY id
    ''')

# ==============================
# Функция: запись данных в ClickHouse
# ==============================
# 1. Инициализируем клиент ClickHouse.
# 2. Проверяем/создаём таблицу.
# 3. Преобразуем данные API в список кортежей.
# 4. Вставляем данные батчом через INSERT.
def write_to_clickhouse(data):
    client = Client(host='localhost', port=9000, user='default', password='', database='default')
    ensure_table_exists(client)

    if not data:
        logger.warning("Нет данных для записи в ClickHouse.")
        return

    # Преобразуем JSON-ответ в список кортежей для вставки
    processed_data = [
        (
            item.get("_id", ""),
            item.get("get", ""),
            json.dumps(item.get("_post", {})),
            item.get("server", ""),
            json.dumps(item.get("supplier", {})),
            json.dumps(item.get("date", {})),
            json.dumps(item.get("response", {})),
            json.dumps(item.get("track", {})),
            get_yesterday()
        )
        for item in data
    ]

    # Проверка: если список пустой, не пишем
    df = pd.DataFrame(processed_data)
    if df.empty:
        logger.warning("DataFrame пустой, пропускаем загрузку.")
        return

    # Вставка данных в ClickHouse
    client.execute("INSERT INTO affise_postbacks VALUES", processed_data)
    logger.info(f"Вставлено {len(processed_data)} строк в ClickHouse.")

# ==============================
# Главная функция: извлечение и запись
# ==============================
# 1. Получаем данные из Affise API.
# 2. Если данные есть — пишем в ClickHouse.
# 3. Если нет — логируем предупреждение.
def fetch_and_store_data():
    data = get_affise_data()
    if data:
        write_to_clickhouse(data)
    else:
        logger.warning("Нет данных от Affise API.")
    return "Success"
