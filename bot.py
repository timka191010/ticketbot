import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
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
TELEGRAM_BOT_TOKEN = "8611024215:AAFyLo-uj3MFkvHPPbUzGmE5WvRK4RqdSo4"
AVIASALES_API_TOKEN = "4587b79386fe645570e662bfc28aaf95"
ADMIN_CHAT_ID = -5111899468  # ID группы (отрицательное число)

# Маршрут 1: Москва → Бали
ORIGIN_IATA_BALI = "MOW"
DESTINATION_IATA_BALI = "DPS"
DEPARTURE_DATE_BALI = "2026-08-01"
RETURN_DATE_BALI = "2026-08-23"

# Маршрут 2: Москва → Сочи (с багажом)
ORIGIN_IATA_SOCHI = "MOW"
DESTINATION_IATA_SOCHI = "AER"  # Адлер/Сочи
DEPARTURE_DATE_SOCHI = "2026-07-17"
RETURN_DATE_SOCHI = "2026-07-26"

# Московское время (UTC+3)
MSK_TZ = timezone(timedelta(hours=3))

# Ссылки на поиск на Aviasales
SEARCH_URL_BALI = f"https://www.aviasales.ru/search/{ORIGIN_IATA_BALI}{DESTINATION_IATA_BALI}1?departure_date={DEPARTURE_DATE_BALI}&return_date={RETURN_DATE_BALI}"
SEARCH_URL_SOCHI = f"https://www.aviasales.ru/search/{ORIGIN_IATA_SOCHI}{DESTINATION_IATA_SOCHI}1?departure_date={DEPARTURE_DATE_SOCHI}&return_date={RETURN_DATE_SOCHI}"
# =============================================

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# База данных для хранения последней цены (расширена для двух маршрутов)
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

# Запрос цен на билеты туда-обратно с возможностью указать багаж
async def fetch_round_trip_prices(origin: str, destination: str, depart_date: str, return_date: str, with_luggage: bool = False) -> List[Dict[str, Any]]:
    """
    Ищет билеты туда-обратно.
    with_luggage=True - ищем билеты с багажом (через параметр baggage)
    """
    url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"
    params = {
        "origin": origin,
        "destination": destination,
        "departure_at": depart_date,
        "return_at": return_date,
        "one_way": "false",
        "sorting": "price",
        "currency": "rub",
        "limit": 30,
        "token": AVIASALES_API_TOKEN
    }
    
    # Для Сочи - добавляем параметр багажа (если API поддерживает)
    if with_luggage:
        params["baggage"] = "1"  # 1 означает "с багажом"
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    tickets = data.get('data', [])
                    
                    if not tickets:
                        print("  Билеты не найдены")
                        return []
                    
                    print(f"  Найдено билетов: {len(tickets)}")
                    return tickets
                else:
                    print(f"Ошибка API: HTTP {response.status}")
                    return []
        except Exception as e:
            print(f"Ошибка при запросе: {e}")
            return []

# Форматирование сообщения (универсальное)
def format_price_alert(ticket: Dict[str, Any], route_name: str, depart_date: str, return_date: str, search_url: str, is_new_low: bool = False, is_manual_check: bool = False, with_luggage: bool = False) -> str:
    price = ticket.get('price', 'Цена неизвестна')
    airline = ticket.get('airline', 'Неизвестная авиакомпания')
    link = ticket.get('link', '')
    
    # Расшифровка кодов авиакомпаний
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
    
    depart_date_obj = datetime.strptime(depart_date, "%Y-%m-%d")
    return_date_obj = datetime.strptime(return_date, "%Y-%m-%d")
    depart_formatted = depart_date_obj.strftime("%d %B %Y").replace("July", "июля").replace("August", "августа")
    return_formatted = return_date_obj.strftime("%d %B %Y").replace("July", "июля").replace("August", "августа")
    
    full_url = f"https://www.aviasales.ru{link}" if link else search_url
    
    luggage_text = " 🧳 С БАГАЖОМ" if with_luggage else ""
    
    if is_manual_check:
        header = f"🔍 {route_name}{luggage_text}"
        trend = ""
    elif is_new_low:
        header = f"🎉 НОВЫЙ МИНИМУМ ЦЕНЫ! 🎉"
        trend = f"🔻 {route_name}{luggage_text} — ЦЕНА СНИЗИЛАСЬ"
    else:
        header = f"✈️ ЦЕНА ИЗМЕНИЛАСЬ"
        trend = f"📈 {route_name}{luggage_text} — ЦЕНА ВЫРОСЛА"
    
    message = f"""
{header}

{trend}

📍 **Маршрут:** {route_name}
📅 **Туда:** {depart_formatted}
📅 **Обратно:** {return_formatted}
{"🧳 **Багаж:** включён" if with_luggage else ""}
🛩️ **Авиакомпания:** {airline_name} ({airline})
💸 **Цена (туда-обратно):** {price} ₽
🔗 **[Купить билет на Aviasales]({full_url})**

⏰ _Время проверки: {datetime.now(MSK_TZ).strftime('%H:%M:%S %d.%m.%Y')}_
    """
    return message.strip()

# Проверка цен для маршрута
async def check_route_prices(route_config: Dict[str, Any], is_manual: bool = False, target_chat_id: int = None):
    route_key = route_config["key"]
    last_price = get_last_price(route_key)
    
    print(f"[{datetime.now(MSK_TZ)}] Проверка цен: {route_config['name']}")
    
    tickets = await fetch_round_trip_prices(
        route_config["origin"],
        route_config["destination"],
        route_config["depart_date"],
        route_config["return_date"],
        with_luggage=route_config.get("with_luggage", False)
    )
    
    chat_id = target_chat_id if target_chat_id else ADMIN_CHAT_ID
    
    if not tickets:
        print(f"  {route_config['name']} - билеты не найдены")
        if is_manual:
            await bot.send_message(
                chat_id=chat_id,
                text=f"❌ {route_config['name']} — билеты не найдены на указанные даты.",
                parse_mode=ParseMode.MARKDOWN
            )
        return
    
    cheapest_ticket = tickets[0]
    current_price = cheapest_ticket.get('price')
    
    if current_price is None:
        print("  Не удалось получить цену")
        return
    
    print(f"  {route_config['name']} - текущая цена: {current_price} ₽")
    print(f"  Было: {last_price if last_price else 'нет данных'}")
    
    # Если ручная проверка - всегда отправляем текущую цену
    if is_manual:
        msg = format_price_alert(
            cheapest_ticket, 
            route_config["name"],
            route_config["depart_date"],
            route_config["return_date"],
            route_config["search_url"],
            is_new_low=False,
            is_manual_check=True,
            with_luggage=route_config.get("with_luggage", False)
        )
        await bot.send_message(chat_id=chat_id, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  ✅ Отправлен ручной отчёт: {current_price} ₽")
        return
    
    # Автоматическая проверка (по расписанию)
    if last_price is None:
        update_price(route_key, current_price)
        msg = format_price_alert(
            cheapest_ticket, 
            route_config["name"],
            route_config["depart_date"],
            route_config["return_date"],
            route_config["search_url"],
            is_new_low=True,
            is_manual_check=False,
            with_luggage=route_config.get("with_luggage", False)
        )
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  ✅ Отправлено начальное уведомление: {current_price} ₽")
    elif current_price < last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(
            cheapest_ticket, 
            route_config["name"],
            route_config["depart_date"],
            route_config["return_date"],
            route_config["search_url"],
            is_new_low=True,
            is_manual_check=False,
            with_luggage=route_config.get("with_luggage", False)
        )
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  🎉 СНИЖЕНИЕ: {last_price} → {current_price}")
    elif current_price > last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(
            cheapest_ticket, 
            route_config["name"],
            route_config["depart_date"],
            route_config["return_date"],
            route_config["search_url"],
            is_new_low=False,
            is_manual_check=False,
            with_luggage=route_config.get("with_luggage", False)
        )
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"  ⚠️ ПОВЫШЕНИЕ: {last_price} → {current_price}")
    else:
        print("  Цена не изменилась")

# Список маршрутов для отслеживания
ROUTES = [
    {
        "key": "MOW_DPS_2026-08-01_2026-08-23_direct",
        "name": "Москва → Бали → Москва ✈️",
        "origin": "MOW",
        "destination": "DPS",
        "depart_date": "2026-08-01",
        "return_date": "2026-08-23",
        "search_url": SEARCH_URL_BALI,
        "with_luggage": False,
        "direct_only": True
    },
    {
        "key": "MOW_AER_2026-07-17_2026-07-26_luggage",
        "name": "Москва → Сочи → Москва 🏖️",
        "origin": "MOW",
        "destination": "AER",
        "depart_date": "2026-07-17",
        "return_date": "2026-07-26",
        "search_url": SEARCH_URL_SOCHI,
        "with_luggage": True,
        "direct_only": False
    }
]

# Проверка всех маршрутов
async def check_all_prices(is_manual: bool = False, target_chat_id: int = None):
    for route in ROUTES:
        await check_route_prices(route, is_manual, target_chat_id)
        await asyncio.sleep(2)  # Задержка между запросами к API

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.chat.type in ["group", "supergroup"]:
        await message.answer(
            f"✈️ **Привет! Я бот для отслеживания цен на авиабилеты.**\n\n"
            f"📍 **Отслеживаемые маршруты:**\n"
            f"• Москва → Бали → Москва (1-23 августа 2026) ✈️\n"
            f"• Москва → Сочи → Москва с БАГАЖОМ (17-26 июля 2026) 🏖️🧳\n\n"
            f"**📌 Команды бота:**\n"
            f"/price — узнать текущие цены на билеты\n"
            f"/check — принудительная проверка цен прямо сейчас\n"
            f"/start — показать это сообщение\n\n"
            f"**🔄 Как работает бот:**\n"
            f"• Каждый час автоматически проверяет цены\n"
            f"• При **снижении** цены — присылает уведомление 🎉\n"
            f"• При **повышении** цены — присылает уведомление 📈\n\n"
            f"**❓ Вопросы или предложения** — пиши разработчику @timka191010",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.answer("❌ У вас нет доступа к этому боту.")
            return
        
        await message.answer(
            f"✈️ **Бот запущен!**\n\n"
            f"📍 **Отслеживаемые маршруты:**\n"
            f"• Москва → Бали → Москва ✈️\n"
            f"  📅 1 августа 2026 → 23 августа 2026\n"
            f"  ✈️ Прямые рейсы (Аэрофлот)\n\n"
            f"• Москва → Сочи → Москва 🏖️🧳\n"
            f"  📅 17 июля 2026 → 26 июля 2026\n"
            f"  🧳 С БАГАЖОМ!\n\n"
            f"**📌 Команды:**\n"
            f"/price — узнать текущие цены\n"
            f"/check — принудительная проверка\n"
            f"/start — это сообщение\n\n"
            f"**🔄 Проверка цен:** каждый час\n\n"
            f"**❓ Вопросы или предложения** — пиши @timka191010",
            parse_mode=ParseMode.MARKDOWN
        )

# Команда /price
@dp.message(Command("price"))
async def cmd_price(message: Message):
    if message.chat.type in ["group", "supergroup"]:
        await message.answer("🔍 Ищу актуальные цены на оба направления...\n\n• Москва → Бали\n• Москва → Сочи (с багажом)\n\nЭто может занять до 30 секунд.")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)
    else:
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.answer("❌ У вас нет доступа к этому боту.")
            return
        
        await message.answer("🔍 Ищу актуальные цены на оба направления...\n\n• Москва → Бали\n• Москва → Сочи (с багажом)")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)

# Команда /check (принудительная проверка)
@dp.message(Command("check"))
async def cmd_check(message: Message):
    if message.chat.type in ["group", "supergroup"]:
        await message.answer("🔄 Принудительная проверка цен...\n\nБот проверяет оба направления прямо сейчас.")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)
    else:
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.answer("❌ У вас нет доступа к этому боту.")
            return
        
        await message.answer("🔄 Принудительная проверка цен...")
        await check_all_prices(is_manual=True, target_chat_id=message.chat.id)

# Периодическая проверка (каждый час)
async def scheduled_monitoring():
    while True:
        await check_all_prices(is_manual=False)
        await asyncio.sleep(3600)  # 1 час

# Запуск
async def main():
    init_db()
    print("🚀 Бот запущен! Мониторинг маршрутов:")
    for route in ROUTES:
        print(f"  📍 {route['name']}: {route['depart_date']} → {route['return_date']}")
    print(f"🕐 Московское время: {datetime.now(MSK_TZ).strftime('%H:%M:%S')}")
    print(f"📌 Доступны команды: /start, /price, /check")
    asyncio.create_task(scheduled_monitoring())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
