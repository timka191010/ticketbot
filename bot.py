import asyncio
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List
import pytz  # для работы с часовыми поясами

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

# ================= НАСТРОЙКИ =================
TELEGRAM_BOT_TOKEN = "8611024215:AAEDivgf-iQlJjnTkTUUtv0J6z9SgRuY3CE"
AVIASALES_API_TOKEN = "4587b79386fe645570e662bfc28aaf95"
ADMIN_CHAT_ID = 7271900005

# Маршрут и даты
ORIGIN_IATA = "MOW"
DESTINATION_IATA = "DPS"
DEPARTURE_DATE = "2026-08-01"
RETURN_DATE = "2026-08-23"

# Московский часовой пояс (MSK, UTC+3)
MSK_TZ = pytz.timezone('Europe/Moscow')

SEARCH_URL = f"https://www.aviasales.ru/search/{ORIGIN_IATA}{DESTINATION_IATA}1?departure_date={DEPARTURE_DATE}&return_date={RETURN_DATE}"
# =============================================

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

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
