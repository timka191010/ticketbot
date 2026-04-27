import asyncio
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

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
SEARCH_URL = f"https://www.aviasales.ru/search/{ORIGIN_IATA}{DESTINATION_IATA}1?departure_date={DEPARTURE_DATE}&return_date={RETURN_DATE}&direct=1"
# =============================================

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# База данных
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

# Запрос цен (только прямые рейсы)
async def fetch_round_trip_prices(origin: str, destination: str, depart_date: str, return_date: str) -> List[Dict[str, Any]]:
    url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
    params = {
        "origin": origin,
        "destination": destination,
        "departure_at": depart_date,
        "return_at": return_date,
        "one_way": "false",
        "direct": "true",
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
                    tickets = data.get('data', [])
                    
                    direct_tickets = []
                    for ticket in tickets:
                        if ticket.get('transfers') == 0 or ticket.get('segments') is None:
                            direct_tickets.append(ticket)
                    
                    print(f"  Найдено прямых билетов: {len(direct_tickets)} из {len(tickets)}")
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
💸 **Цена (туда-обратно):** {price} ₽
🛩️ **Авиакомпания:** {airline}
🔗 **[Купить билет на Aviasales]({full_url})**

⏰ _Время проверки: {datetime.now().strftime('%H:%M:%S %d.%m.%Y')}_
    """
    return message.strip()

# Проверка цен
async def check_prices_and_notify():
    route_key = f"{ORIGIN_IATA}_{DESTINATION_IATA}_{DEPARTURE_DATE}_{RETURN_DATE}_direct"
    last_price = get_last_price(route_key)
    
    print(f"[{datetime.now()}] Проверка цен на ПРЯМЫЕ рейсы...")
    
    tickets = await fetch_round_trip_prices(ORIGIN_IATA, DESTINATION_IATA, DEPARTURE_DATE, RETURN_DATE)
    
    if not tickets:
        print("Прямые билеты не найдены")
        return
    
    cheapest_ticket = tickets[0]
    current_price = cheapest_ticket.get('price')
    
    if current_price is None:
        print("Не удалось получить цену")
        return
    
    print(f"Текущая цена: {current_price} ₽ (было: {last_price})")
    
    if last_price is None:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=True)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"✅ Отправлено начальное уведомление")
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
        f"✈️ **Только прямые рейсы**\n\n"
        f"🔄 Проверка: каждый час\n"
        f"📌 /price — узнать текущую цену"
    )

# Команда /price
@dp.message(Command("price"))
async def cmd_price(message: Message):
    await message.answer("🔍 Ищу прямые рейсы...")
    tickets = await fetch_round_trip_prices(ORIGIN_IATA, DESTINATION_IATA, DEPARTURE_DATE, RETURN_DATE)
    if tickets:
        msg = format_price_alert(tickets[0], is_new_low=False)
        await message.answer(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
    else:
        await message.answer("❌ Прямые билеты не найдены")

# Периодическая проверка
async def scheduled_monitoring():
    while True:
        await check_prices_and_notify()
        await asyncio.sleep(3600)

# ТЕСТОВАЯ ФУНКЦИЯ (для проверки уведомлений)
async def test_notifications():
    """Отправляет тестовые сообщения о снижении и повышении цены"""
    print("\n🧪 ТЕСТОВЫЙ РЕЖИМ\n")
    
    # Тестовое снижение
    test_ticket_low = {"price": 75000, "airline": "SU (Аэрофлот)", "link": ""}
    msg_low = format_price_alert(test_ticket_low, is_new_low=True)
    print("📨 Отправка тестового сообщения о СНИЖЕНИИ...")
    await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg_low, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
    
    await asyncio.sleep(2)
    
    # Тестовое повышение
    test_ticket_high = {"price": 85000, "airline": "SU (Аэрофлот)", "link": ""}
    msg_high = format_price_alert(test_ticket_high, is_new_low=False)
    print("📨 Отправка тестового сообщения о ПОВЫШЕНИИ...")
    await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg_high, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
    
    print("\n✅ Тест завершён! Проверь Telegram.")

# Запуск
async def main():
    init_db()
    print("🚀 Бот запущен! Мониторинг ТОЛЬКО ПРЯМЫХ РЕЙСОВ")
    asyncio.create_task(scheduled_monitoring())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())