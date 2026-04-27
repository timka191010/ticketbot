import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
import warnings
import json

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

# Игнорируем предупреждения SQLite
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ================= НАСТРОЙКИ =================
TELEGRAM_BOT_TOKEN = "8611024215:AAFyLo-uj3MFkvHPPbUzGmE5WvRK4RqdSo4"
AVIASALES_API_TOKEN = "4587b79386fe645570e662bfc28aaf95"
ADMIN_CHAT_ID = -5111899468  # ID группы

# Московское время (UTC+3)
MSK_TZ = timezone(timedelta(hours=3))

# Маршрут 1: Москва → Бали (только прямой Аэрофлот)
ROUTE_BALI = {
    "name": "Москва → Бали → Москва ✈️",
    "origin": "MOW",
    "destination": "DPS",
    "depart_date": "2026-08-01",
    "return_date": "2026-08-23",
    "direct_only": True,
    "direct_airlines": ["SU"],  # Только Аэрофлот
    "search_url": "https://www.aviasales.ru/search/MOWDPS1?departure_date=2026-08-01&return_date=2026-08-23"
}

# Маршрут 2: Москва → Сочи (с багажом, любые рейсы)
ROUTE_SOCHI = {
    "name": "Москва → Сочи → Москва 🏖️🧳",
    "origin": "MOW",
    "destination": "AER",
    "depart_date": "2026-07-17",
    "return_date": "2026-07-26",
    "direct_only": False,
    "direct_airlines": [],
    "search_url": "https://www.aviasales.ru/search/MOWAER1?departure_date=2026-07-17&return_date=2026-07-26",
    "with_luggage": True
}

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
    ''', (route, price, datetime.now(MSK_TZ)))
    conn.commit()
    conn.close()

# Функция для проверки, является ли билет прямым (через отдельный API запрос)
async def check_if_direct(origin: str, destination: str, depart_date: str, return_date: str, airline_code: str) -> bool:
    """
    Проверяет, существует ли прямой рейс у конкретной авиакомпании.
    Используется для верификации, что билет действительно без пересадок.
    """
    url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
    params = {
        "origin": origin,
        "destination": destination,
        "departure_at": depart_date,
        "return_at": return_date,
        "one_way": "false",
        "direct": "true",  # Запрашиваем только прямые
        "sorting": "price",
        "currency": "rub",
        "limit": 50,
        "token": AVIASALES_API_TOKEN
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    tickets = data.get('data', [])
                    
                    # Проверяем, есть ли билеты этой авиакомпании с параметром transfers=0
                    for ticket in tickets:
                        if ticket.get('airline') == airline_code and ticket.get('transfers') == 0:
                            return True
                    return False
                else:
                    return False
        except Exception as e:
            print(f"  Ошибка проверки прямого рейса: {e}")
            return False

# Запрос цен на билеты
async def fetch_ticket_prices(route_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Ищет билеты для маршрута с фильтрацией прямых рейсов для Бали
    """
    url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
    params = {
        "origin": route_config["origin"],
        "destination": route_config["destination"],
        "departure_at": route_config["depart_date"],
        "return_at": route_config["return_date"],
        "one_way": "false",
        "sorting": "price",
        "currency": "rub",
        "limit": 50,
        "token": AVIASALES_API_TOKEN
    }
    
    # Для Сочи добавляем параметр багажа
    if route_config.get("with_luggage"):
        params["baggage"] = "1"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    all_tickets = data.get('data', [])
                    
                    if not all_tickets:
                        print("  Билеты не найдены")
                        return []
                    
                    # Фильтрация билетов
                    filtered_tickets = []
                    
                    for ticket in all_tickets:
                        airline = ticket.get('airline', '')
                        transfers = ticket.get('transfers')
                        price = ticket.get('price', '?')
                        
                        # Для Бали: только прямые рейсы Аэрофлота (transfers = 0)
                        if route_config["direct_only"]:
                            # Должен быть Аэрофлот И пересадок 0
                            if airline in route_config["direct_airlines"] and transfers == 0:
                                filtered_tickets.append(ticket)
                                print(f"  ✅ {route_config['name']} - ПРЯМОЙ: {airline}, {price} ₽")
                            else:
                                print(f"  ❌ {route_config['name']} - Отфильтрован (с пересадками): {airline}, пересадки: {transfers}, {price} ₽")
                        else:
                            # Для Сочи: все билеты
                            filtered_tickets.append(ticket)
                            print(f"  ✅ {route_config['name']} - {airline}, {price} ₽, пересадки: {transfers}")
                    
                    return filtered_tickets
                else:
                    print(f"Ошибка API: HTTP {response.status}")
                    return []
        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            return []

# Форматирование сообщения
def format_price_alert(ticket: Dict[str, Any], route_config: Dict[str, Any], is_new_low: bool = False, is_manual_check: bool = False) -> str:
    price = ticket.get('price', 'Цена неизвестна')
    airline = ticket.get('airline', 'Неизвестная авиакомпания')
    link = ticket.get('link', '')
    
    airline_names = {
        "SU": "Аэрофлот",
        "FV": "Россия",
        "S7": "S7 Airlines",
        "UT": "ЮТэйр",
        "DP": "Победа",
        "EK": "Emirates",
        "QR": "Qatar Airways",
        "TK": "Turkish Airlines",
    }
    airline_name = airline_names.get(airline, airline)
    
    depart_date_obj = datetime.strptime(route_config["depart_date"], "%Y-%m-%d")
    return_date_obj = datetime.strptime(route_config["return_date"], "%Y-%m-%d")
    depart_formatted = depart_date_obj.strftime("%d %B %Y").replace("July", "июля").replace("August", "августа")
    return_formatted = return_date_obj.strftime("%d %B %Y").replace("July", "июля").replace("August", "августа")
    
    full_url = f"https://www.aviasales.ru{link}" if link else route_config["search_url"]
    
    if is_manual_check:
        header = f"🔍 {route_config['name']}"
        trend = ""
    elif is_new_low:
        header = f"🎉 НОВЫЙ МИНИМУМ ЦЕНЫ! 🎉"
        trend = f"🔻 {route_config['name']} — ЦЕНА СНИЗИЛАСЬ"
    else:
        header = f"✈️ ЦЕНА ИЗМЕНИЛАСЬ"
        trend = f"📈 {route_config['name']} — ЦЕНА ВЫРОСЛА"
    
    direct_text = " ✈️ ПРЯМОЙ РЕЙС" if route_config.get("direct_only") else ""
    luggage_text = " 🧳 С БАГАЖОМ" if route_config.get("with_luggage") else ""
    
    message = f"""
{header}

{trend}

📍 **Маршрут:** {route_config['name']}{direct_text}{luggage_text}
📅 **Туда:** {depart_formatted}
📅 **Обратно:** {return_formatted}
🛩️ **Авиакомпания:** {airline_name} ({airline})
💸 **Цена (туда-обратно):** {price} ₽
🔗 **[Купить билет на Aviasales]({full_url})**

⏰ _Время проверки: {datetime.now(MSK_TZ).strftime('%H:%M:%S %d.%m.%Y')}_
    """
    return message.strip()

# Проверка цен для маршрута
async def check_route_prices(route_config: Dict[str, Any], is_manual: bool = False, target_chat_id: int = None):
    route_key = f"{route_config['origin']}_{route_config['destination']}_{route_config['depart_date']}_{route_config['return_date']}_{route_config.get('direct_only', False)}"
    last_price = get_last_price(route_key)
    
    print(f"[{datetime.now(MSK_TZ)}] Проверка: {route_config['name']}")
    
    tickets = await fetch_ticket_prices(route_config)
    
    chat_id = target_chat_id if target_chat_id else ADMIN_CHAT_ID
    
    if not tickets:
        print(f"  {route_config['name']} - билеты не найдены")
        if is_manual:
            await bot.send_message(
                chat_id=chat_id,
                text=f"❌ {route_config['name']} — билеты не найдены на указанные даты.\n\n" + (
                    "Возможно, на эти даты нет прямых рейсов Аэрофлота." if route_config.get("direct_only") else ""
                ),
                parse_mode=ParseMode.MARKDOWN
            )
        return
    
    cheapest_ticket = tickets[0]
    current_price = cheapest_ticket.get('price')
    
    if current_price is None:
        print("  Не удалось получить цену")
        return
    
    print(f"  {route_config['name']} - цена: {current_price} ₽ (было: {last_price if last_price else 'нет'})")
    
    if is_manual:
        msg = format_price_alert(cheapest_ticket, route_config, is_new_low=False, is_manual_check=True)
        await bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  ✅ Отправлен ручной отчёт")
        return
    
    # Автоматическая проверка
    if last_price is None:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, route_config, is_new_low=True, is_manual_check=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  ✅ Отправлено начальное уведомление")
    elif current_price < last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, route_config, is_new_low=True, is_manual_check=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  🎉 СНИЖЕНИЕ: {last_price} → {current_price}")
    elif current_price > last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, route_config, is_new_low=False, is_manual_check=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  ⚠️ ПОВЫШЕНИЕ: {last_price} → {current_price}")
    else:
        print("  Цена не изменилась")

# Список маршрутов
ROUTES = [ROUTE_BALI, ROUTE_SOCHI]

# Проверка всех маршрутов
async def check_all_prices(is_manual: bool = False, target_chat_id: int = None):
    for route in ROUTES:
        await check_route_prices(route, is_manual, target_chat_id)
        await asyncio.sleep(2)

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.chat.type in ["group", "supergroup"]:
        await message.answer(
            f"✈️ **Привет! Я бот для отслеживания цен на авиабилеты.**\n\n"
            f"📍 **Отслеживаемые маршруты:**\n"
            f"• ✈️ Москва → Бали → Москва (1-23 августа 2026)\n"
            f"  ✅ **ТОЛЬКО ПРЯМЫЕ РЕЙСЫ АЭРОФЛОТА**\n"
            f"• 🏖️ Москва → Сочи → Москва с БАГАЖОМ (17-26 июля 2026)\n\n"
            f"**📌 Команды:** /price /check /start\n\n"
            f"**❓ Вопросы** — @timka191010",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.answer("❌ У вас нет доступа.")
            return
        
        await message.answer(
            f"✈️ **Бот запущен!**\n\n"
            f"📍 **Маршруты:**\n"
            f"• Бали: 1-23 августа 2026, ТОЛЬКО ПРЯМЫЕ Аэрофлот\n"
            f"• Сочи: 17-26 июля 2026, С БАГАЖОМ\n\n"
            f"**📌 Команды:** /price /check /start\n\n"
            f"🔄 Проверка: каждый час",
            parse_mode=ParseMode.MARKDOWN
        )

# Команда /price
@dp.message(Command("price"))
async def cmd_price(message: Message):
    if message.chat.type in ["group", "supergroup"]:
        await message.answer("🔍 Ищу цены...\n\n• Бали (прямые Аэрофлот)\n• Сочи (с багажом)")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)
    else:
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.answer("❌ У вас нет доступа.")
            return
        await message.answer("🔍 Ищу цены...")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)

# Команда /check
@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.chat.type in ["group", "supergroup"]:
        await message.answer("🔄 Принудительная проверка...")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)
    else:
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.answer("❌ У вас нет доступа.")
            return
        await message.answer("🔄 Принудительная проверка...")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)

# Периодическая проверка
async def scheduled_monitoring():
    while True:
        await check_all_prices(is_manual=False)
        await asyncio.sleep(3600)

# Запуск
async def main():
    init_db()
    print("🚀 Бот запущен!")
    for route in ROUTES:
        print(f"  📍 {route['name']}: {route['depart_date']} → {route['return_date']}")
        if route.get('direct_only'):
            print(f"     ✈️ ТОЛЬКО ПРЯМЫЕ РЕЙСЫ ({', '.join(route['direct_airlines'])})")
    await asyncio.gather(scheduled_monitoring(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
