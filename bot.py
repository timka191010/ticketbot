import asyncio
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List
import warnings

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

# Игнорируем предупреждения SQLite
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ================= НАСТРОЙКИ =================
# ⚠️ ВСТАВЬ СВОИ НОВЫЕ КЛЮЧИ (получи их заново!):
TELEGRAM_BOT_TOKEN = "8611024215:AAEDivgf-iQlJjnTkTUUtv0J6z9SgRuY3CE"
AVIASALES_API_TOKEN = "4587b79386fe645570e662bfc28aaf95"
ADMIN_CHAT_ID = 7271900005  # ЗАМЕНИ НА СВОЙ ID (узнай у @userinfobot)

# Маршрут
ORIGIN_IATA = "MOW"      # Москва
DESTINATION_IATA = "DPS" # Бали

# ДАТЫ (фиксированные)
DEPARTURE_DATE = "2026-08-01"   # Туда
RETURN_DATE = "2026-08-23"      # Обратно

# Ссылка на поиск на Aviasales
SEARCH_URL = f"https://www.aviasales.ru/search/{ORIGIN_IATA}{DESTINATION_IATA}1?departure_date={DEPARTURE_DATE}&return_date={RETURN_DATE}"
# =============================================

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# База данных для хранения последней цены
def init_db():
    conn = sqlite3.connect('flight_prices.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_history (
            route TEXT PRIMARY KEY,
            last_price INTEGER,
            last_update TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_last_price(route: str) -> Optional[int]:
    conn = sqlite3.connect('flight_prices.db')
    cursor = conn.cursor()
    cursor.execute('SELECT last_price FROM price_history WHERE route = ?', (route,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def update_price(route: str, price: int):
    conn = sqlite3.connect('flight_prices.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO price_history (route, last_price, last_update)
        VALUES (?, ?, ?)
        ON CONFLICT(route) DO UPDATE SET
            last_price = excluded.last_price,
            last_update = excluded.last_update
    ''', (route, price, datetime.now()))
    conn.commit()
    conn.close()

# Запрос цен на билеты туда-обратно (ТОЛЬКО НАСТОЯЩИЕ ПРЯМЫЕ РЕЙСЫ)
async def fetch_round_trip_prices(origin: str, destination: str, depart_date: str, return_date: str) -> List[Dict[str, Any]]:
    """
    Ищет билеты туда-обратно. Возвращает ТОЛЬКО прямые рейсы (без пересадок).
    Единственный гарантированно прямой перевозчик Москва → Бали — Аэрофлот (SU)
    """
    url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
    params = {
        "origin": origin,
        "destination": destination,
        "departure_at": depart_date,
        "return_at": return_date,
        "one_way": "false",
        "direct": "true",           # Пытаемся запросить прямые
        "sorting": "price",
        "currency": "rub",
        "limit": 30,                # Увеличиваем лимит для фильтрации
        "token": AVIASALES_API_TOKEN
    }
    
    # Авиакомпании, которые летают ПРЯМЫМИ рейсами Москва → Бали
    DIRECT_AIRLINES = ["SU"]  # Аэрофлот — единственный регулярный прямой перевозчик
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    all_tickets = data.get('data', [])
                    
                    if not all_tickets:
                        print("  Билеты не найдены")
                        return []
                    
                    # Жёсткая фильтрация: оставляем ТОЛЬКО настоящие прямые рейсы
                    direct_tickets = []
                    for ticket in all_tickets:
                        transfers = ticket.get('transfers')
                        airline = ticket.get('airline', '')
                        price = ticket.get('price', '?')
                        
                        # Условия для прямого рейса:
                        # 1. transfers == 0 (нет пересадок)
                        # 2. ИЛИ авиакомпания из списка прямых (Аэрофлот)
                        is_direct = (transfers == 0) or (airline in DIRECT_AIRLINES)
                        
                        if is_direct:
                            direct_tickets.append(ticket)
                            print(f"  ✅ ПРЯМОЙ рейс: {airline}, цена {price} ₽, пересадки: {transfers}")
                        else:
                            print(f"  ❌ Отфильтрован (с пересадками): {airline}, цена {price} ₽, пересадки: {transfers}")
                    
                    if len(direct_tickets) == 0:
                        print(f"  ⚠️ Найдено {len(all_tickets)} билетов, но все с пересадками. Прямых рейсов нет.")
                    else:
                        print(f"  ✅ Итого прямых билетов: {len(direct_tickets)} из {len(all_tickets)}")
                    
                    return direct_tickets
                else:
                    print(f"Ошибка API: HTTP {response.status}")
                    return []
        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            return []

# Форматирование сообщения
def format_price_alert(ticket: Dict[str, Any], is_new_low: bool = False) -> str:
    price = ticket.get('price', 'Цена неизвестна')
    airline = ticket.get('airline', 'Неизвестная авиакомпания')
    link = ticket.get('link', '')
    
    # Расшифровка кодов авиакомпаний
    airline_names = {
        "SU": "Аэрофлот",
        "FV": "Россия",
        "EK": "Emirates",
        "QR": "Qatar Airways",
        "TK": "Turkish Airlines",
    }
    airline_name = airline_names.get(airline, airline)
    
    depart_date_obj = datetime.strptime(DEPARTURE_DATE, "%Y-%m-%d")
    return_date_obj = datetime.strptime(RETURN_DATE, "%Y-%m-%d")
    depart_formatted = depart_date_obj.strftime("%d %B %Y").replace("August", "августа")
    return_formatted = return_date_obj.strftime("%d %B %Y").replace("August", "августа")
    
    full_url = f"https://www.aviasales.ru{link}" if link else SEARCH_URL
    
    if is_new_low:
        header = "🎉 НОВЫЙ МИНИМУМ ЦЕНЫ! 🎉"
        trend = "🔻 ЦЕНА СНИЗИЛАСЬ"
    else:
        header = "✈️ ЦЕНА ИЗМЕНИЛАСЬ"
        trend = "📈 ЦЕНА ВЫРОСЛА"
    
    message = f"""
{header}

{trend}

📍 **Маршрут:** Москва → Бали → Москва
📅 **Туда:** {depart_formatted}
📅 **Обратно:** {return_formatted}
✈️ **Тип рейса:** ПРЯМОЙ (без пересадок)
🛩️ **Авиакомпания:** {airline_name} ({airline})
💸 **Цена (туда-обратно):** {price} ₽
🔗 **[Купить билет на Aviasales]({full_url})**

⏰ _Время проверки: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}_
    """
    return message.strip()

# Проверка цен
async def check_prices_and_notify():
    route_key = f"{ORIGIN_IATA}_{DESTINATION_IATA}_{DEPARTURE_DATE}_{RETURN_DATE}_direct"
    last_price = get_last_price(route_key)
    
    print(f"[{datetime.now()}] Проверка цен на ПРЯМЫЕ рейсы (только Аэрофлот)...")
    
    tickets = await fetch_round_trip_prices(ORIGIN_IATA, DESTINATION_IATA, DEPARTURE_DATE, RETURN_DATE)
    
    if not tickets:
        print("Прямые билеты не найдены")
        return
    
    cheapest_ticket = tickets[0]
    current_price = cheapest_ticket.get('price')
    
    if current_price is None:
        print("Не удалось получить цену")
        return
    
    print(f"Текущая минимальная цена (прямой рейс): {current_price} ₽")
    print(f"Было: {last_price if last_price else 'нет данных'}")
    
    if last_price is None:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=True)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"✅ Отправлено начальное уведомление: {current_price} ₽")
    elif current_price < last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=True)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"🎉 СНИЖЕНИЕ: {last_price} → {current_price}")
    elif current_price > last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"⚠️ ПОВЫШЕНИЕ: {last_price} → {current_price}")
    else:
        print("Цена не изменилась")

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        f"✈️ Бот запущен!\n\n"
        f"📍 Москва → Бали → Москва\n"
        f"📅 Туда: 1 августа 2026\n"
        f"📅 Обратно: 23 августа 2026\n"
        f"✈️ **Только прямые рейсы (без пересадок)**\n"
        f"🛩️ Отслеживается: Аэрофлот (SU)\n\n"
        f"🔄 Проверка цен: каждый час\n"
        f"📌 /price — узнать текущую цену на прямой рейс"
    )

# Команда /price
@dp.message(Command("price"))
async def cmd_price(message: Message):
    await message.answer("🔍 Ищу прямые рейсы (только Аэрофлот, без пересадок)...")
    tickets = await fetch_round_trip_prices(ORIGIN_IATA, DESTINATION_IATA, DEPARTURE_DATE, RETURN_DATE)
    if tickets:
        msg = format_price_alert(tickets[0], is_new_low=False)
        await message.answer(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
    else:
        await message.answer("❌ Прямые билеты (Аэрофлот) не найдены на эти даты.\n\nВозможно, на указанные даты нет прямых рейсов. Попробуй изменить даты.")

# Периодическая проверка (каждый час)
async def scheduled_monitoring():
    while True:
        await check_prices_and_notify()
        await asyncio.sleep(3600)

# Запуск
async def main():
    init_db()
    print("🚀 Бот запущен! Мониторинг ТОЛЬКО ПРЯМЫХ РЕЙСОВ (Аэрофлот)")
    print(f"📅 Даты: {DEPARTURE_DATE} → {RETURN_DATE}")
    asyncio.create_task(scheduled_monitoring())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())        CREATE TABLE IF NOT EXISTS price_history (
            route TEXT PRIMARY KEY,
            last_price INTEGER,
            last_update TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_last_price(route: str) -> Optional[int]:
    conn = sqlite3.connect('flight_prices.db')
    cursor = conn.cursor()
    cursor.execute('SELECT last_price FROM price_history WHERE route = ?', (route,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def update_price(route: str, price: int):
    conn = sqlite3.connect('flight_prices.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO price_history (route, last_price, last_update)
        VALUES (?, ?, ?)
        ON CONFLICT(route) DO UPDATE SET
            last_price = excluded.last_price,
            last_update = excluded.last_update
    ''', (route, price, datetime.now(MSK_TZ)))  # ← московское время в БД
    conn.commit()
    conn.close()

async def fetch_round_trip_prices(origin: str, destination: str, depart_date: str, return_date: str) -> List[Dict[str, Any]]:
    url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
    params = {
        "origin": origin,
        "destination": destination,
        "departure_at": depart_date,
        "return_at": return_date,
        "one_way": "false",
        "direct": "false",
        "sorting": "price",
        "currency": "rub",
        "limit": 10,
        "token": AVIASALES_API_TOKEN
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
                else:
                    print(f"Ошибка API: HTTP {response.status}")
                    return []
        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            return []

def get_stops_info(ticket: Dict[str, Any]) -> str:
    """
    Определяет, прямой рейс или с пересадками.
    В ответе API есть поле 'stops' (количество пересадок)
    """
    stops = ticket.get('stops', 999)  # если нет данных — считаем с пересадками
    if stops == 0:
        return "ПРЯМОЙ (без пересадок)"
    elif stops == 1:
        return "1 пересадка"
    else:
        return f"{stops} пересадки"

def format_price_alert(ticket: Dict[str, Any], is_new_low: bool = False) -> str:
    price = ticket.get('price', 'Цена неизвестна')
    airline = ticket.get('airline', 'Неизвестная авиакомпания')
    link = ticket.get('link', '')
    stops_info = get_stops_info(ticket)
    
    # Даты в читаемом формате
    depart_date_obj = datetime.strptime(DEPARTURE_DATE, "%Y-%m-%d")
    return_date_obj = datetime.strptime(RETURN_DATE, "%Y-%m-%d")
    depart_formatted = depart_date_obj.strftime("%d %B %Y").replace("August", "августа")
    return_formatted = return_date_obj.strftime("%d %B %Y").replace("August", "августа")
    
    # Московское время прямо сейчас
    now_msk = datetime.now(MSK_TZ)
    time_str = now_msk.strftime("%H:%M:%S")
    date_str = now_msk.strftime("%d.%m.%Y")
    
    full_url = f"https://www.aviasales.ru{link}" if link else SEARCH_URL
    
    # Выбор заголовка
    if is_new_low:
        header = "🎉 НОВЫЙ МИНИМУМ ЦЕНЫ! 🎉"
    else:
        header = "✈️ ЦЕНА ВЫРОСЛА"
    
    # Формируем сообщение в точности как на скриншоте
    message = f"""
{header}

📍 Маршрут: Москва → Бали → Москва
📅 Туда: {depart_formatted}
📅 Обратно: {return_formatted}
🛫 Тип рейса: {stops_info}
💰 Цена (туда-обратно): {price} ₽
🛩️ Авиакомпания: {airline}
🔗 Купить билет на Aviasales

⏰ Время проверки: {time_str}
{date_str}

---
🔍 [Авиасейлс]({full_url})
Дешёвые авиабилеты онлайн, цены. Поиск билетов на самолёт и сравнение цен
    """
    return message.strip()

async def check_prices_and_notify():
    route_key = f"{ORIGIN_IATA}_{DESTINATION_IATA}_{DEPARTURE_DATE}_{RETURN_DATE}"
    last_price = get_last_price(route_key)
    
    now_msk = datetime.now(MSK_TZ)
    print(f"[{now_msk}] Проверка цен: Москва → Бали ({DEPARTURE_DATE}) → Москва ({RETURN_DATE})")
    
    tickets = await fetch_round_trip_prices(ORIGIN_IATA, DESTINATION_IATA, DEPARTURE_DATE, RETURN_DATE)
    
    if not tickets:
        print("Билеты не найдены или ошибка API")
        return
    
    cheapest_ticket = tickets[0]
    current_price = cheapest_ticket.get('price')
    
    if current_price is None:
        print("Не удалось получить цену")
        return
    
    print(f"Текущая цена: {current_price} ₽ (было: {last_price if last_price else 'нет данных'})")
    
    if last_price is None:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=True)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print("Отправлено начальное уведомление")
    elif current_price < last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=True)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"🎉 СНИЖЕНИЕ: {last_price} → {current_price}")
    elif current_price > last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"⚠️ ПОВЫШЕНИЕ: {last_price} → {current_price}")
    else:
        print("Цена не изменилась")

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        f"✈️ Сканер билетов Бали\n\n"
        f"📍 Москва → Бали → Москва\n"
        f"📅 1 августа — 23 августа 2026\n"
        f"✅ Отслеживаю изменения цен (каждый час)\n\n"
        f"📌 /price — текущая цена"
    )

@dp.message(Command("price"))
async def cmd_price(message: Message):
    await message.answer("🔍 Ищу билеты...")
    tickets = await fetch_round_trip_prices(ORIGIN_IATA, DESTINATION_IATA, DEPARTURE_DATE, RETURN_DATE)
    if tickets:
        msg = format_price_alert(tickets[0], is_new_low=False)
        await message.answer(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
    else:
        await message.answer("❌ Билеты не найдены")

async def scheduled_monitoring():
    while True:
        await check_prices_and_notify()
        await asyncio.sleep(3600)

async def main():
    init_db()
    print(f"🚀 Бот запущен. Часовой пояс: {MSK_TZ}")
    asyncio.create_task(scheduled_monitoring())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
