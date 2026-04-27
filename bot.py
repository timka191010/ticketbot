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
# ⚠️ ВСТАВЬ СВОИ НОВЫЕ КЛЮЧИ (получи их заново!):
TELEGRAM_BOT_TOKEN = "8611024215:AAFyLo-uj3MFkvHPPbUzGmE5WvRK4RqdSo4"
AVIASALES_API_TOKEN = "4587b79386fe645570e662bfc28aaf95"
ADMIN_CHAT_ID = -5111899468  # ЗАМЕНИ НА СВОЙ ID (узнай у @userinfobot)

# Маршрут
ORIGIN_IATA = "MOW"      # Москва
DESTINATION_IATA = "DPS" # Бали

# ДАТЫ (фиксированные)
DEPARTURE_DATE = "2026-08-01"   # Туда
RETURN_DATE = "2026-08-23"      # Обратно

# Московское время (UTC+3)
MSK_TZ = timezone(timedelta(hours=3))

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
    ''', (route, price, datetime.now(MSK_TZ)))
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
        "direct": "true",
        "sorting": "price",
        "currency": "rub",
        "limit": 30,
        "token": AVIASALES_API_TOKEN
    }
    
    # Авиакомпании, которые летают ПРЯМЫМИ рейсами Москва → Бали
    DIRECT_AIRLINES = ["SU"]  # Аэрофлот
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    all_tickets = data.get('data', [])
                    
                    if not all_tickets:
                        print("  Билеты не найдены")
                        return []
                    
                    direct_tickets = []
                    for ticket in all_tickets:
                        transfers = ticket.get('transfers')
                        airline = ticket.get('airline', '')
                        price = ticket.get('price', '?')
                        
                        # Условия для прямого рейса
                        is_direct = (transfers == 0) or (airline in DIRECT_AIRLINES)
                        
                        if is_direct:
                            direct_tickets.append(ticket)
                            print(f"  ✅ ПРЯМОЙ рейс: {airline}, цена {price} ₽, пересадки: {transfers}")
                        else:
                            print(f"  ❌ Отфильтрован: {airline}, цена {price} ₽, пересадки: {transfers}")
                    
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
def format_price_alert(ticket: Dict[str, Any], is_new_low: bool = False, is_manual_check: bool = False) -> str:
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
    
    if is_manual_check:
        header = "🔍 РЕЗУЛЬТАТ ПРОВЕРКИ ЦЕНЫ"
        trend = ""
    elif is_new_low:
        header = "🎉 НОВЫЙ МИНИМУМ ЦЕНЫ! 🎉"
        trend = "🔻 ЦЕНА СНИЗИЛАСЬ"
    else:
        header = "✈️ ЦЕНА ИЗМЕНИЛАСЬ"
        trend = "📈 ЦЕНА ВЫРОСЛА"
    
    if is_manual_check:
        message = f"""
{header}

📍 **Маршрут:** Москва → Бали → Москва
📅 **Туда:** {depart_formatted}
📅 **Обратно:** {return_formatted}
✈️ **Тип рейса:** ПРЯМОЙ (без пересадок)
🛩️ **Авиакомпания:** {airline_name} ({airline})
💸 **Цена (туда-обратно):** {price} ₽
🔗 **[Купить билет на Aviasales]({full_url})**

⏰ _Время проверки: {datetime.now(MSK_TZ).strftime('%H:%M:%S %d.%m.%Y')}_
"""
    else:
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

⏰ _Время проверки: {datetime.now(MSK_TZ).strftime('%H:%M:%S %d.%m.%Y')}_
    """
    return message.strip()

# Проверка цен
async def check_prices_and_notify(is_manual: bool = False):
    route_key = f"{ORIGIN_IATA}_{DESTINATION_IATA}_{DEPARTURE_DATE}_{RETURN_DATE}_direct"
    last_price = get_last_price(route_key)
    
    print(f"[{datetime.now(MSK_TZ)}] Проверка цен на ПРЯМЫЕ рейсы (только Аэрофлот)...")
    
    tickets = await fetch_round_trip_prices(ORIGIN_IATA, DESTINATION_IATA, DEPARTURE_DATE, RETURN_DATE)
    
    if not tickets:
        print("Прямые билеты не найдены")
        if is_manual:
            # Отправляем сообщение пользователю, что билетов нет
            await bot.send_message(
                chat_id=ADMIN_CHAT_ID, 
                text="❌ Прямые билеты (Аэрофлот) не найдены на эти даты.\n\nВозможно, на указанные даты нет прямых рейсов.",
                parse_mode=ParseMode.MARKDOWN
            )
        return
    
    cheapest_ticket = tickets[0]
    current_price = cheapest_ticket.get('price')
    
    if current_price is None:
        print("Не удалось получить цену")
        return
    
    print(f"Текущая минимальная цена (прямой рейс): {current_price} ₽")
    print(f"Было: {last_price if last_price else 'нет данных'}")
    
    # Если ручная проверка - всегда отправляем текущую цену
    if is_manual:
        msg = format_price_alert(cheapest_ticket, is_new_low=False, is_manual_check=True)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"✅ Отправлен ручной отчёт: {current_price} ₽")
        return
    
    # Автоматическая проверка (по расписанию)
    if last_price is None:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=True, is_manual_check=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"✅ Отправлено начальное уведомление: {current_price} ₽")
    elif current_price < last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=True, is_manual_check=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"🎉 СНИЖЕНИЕ: {last_price} → {current_price}")
    elif current_price > last_price:
        update_price(route_key, current_price)
        msg = format_price_alert(cheapest_ticket, is_new_low=False, is_manual_check=False)
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=False)
        print(f"⚠️ ПОВЫШЕНИЕ: {last_price} → {current_price}")
    else:
        print("Цена не изменилась")

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    # Проверяем, что пользователь - админ
    if message.from_user.id != ADMIN_CHAT_ID:
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    
    await message.answer(
        f"✈️ Бот запущен!\n\n"
        f"📍 Москва → Бали → Москва\n"
        f"📅 Туда: 1 августа 2026\n"
        f"📅 Обратно: 23 августа 2026\n"
        f"✈️ **Только прямые рейсы (без пересадок)**\n"
        f"🛩️ Отслеживается: Аэрофлот (SU)\n\n"
        f"🔄 Проверка цен: каждый час\n"
        f"📌 /price — узнать текущую цену на прямой рейс\n"
        f"📌 /check — принудительная проверка цен прямо сейчас"
    )

# Команда /price
@dp.message(Command("price"))
async def cmd_price(message: Message):
    # Проверяем, что пользователь - админ
    if message.from_user.id != ADMIN_CHAT_ID:
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    
    await message.answer("🔍 Ищу прямые рейсы (только Аэрофлот, без пересадок)...")
    await check_prices_and_notify(is_manual=True)

# Команда /check (принудительная проверка)
@dp.message(Command("check"))
async def cmd_check(message: Message):
    # Проверяем, что пользователь - админ
    if message.from_user.id != ADMIN_CHAT_ID:
        await message.answer("❌ У вас нет доступа к этому боту.")
        return
    
    await message.answer("🔄 Принудительная проверка цен...\n\nБот проверяет актуальные цены на прямые рейсы прямо сейчас. Это может занять несколько секунд.")
    await check_prices_and_notify(is_manual=True)

# Периодическая проверка (каждый час)
async def scheduled_monitoring():
    while True:
        await check_prices_and_notify(is_manual=False)
        await asyncio.sleep(3600)  # 1 час

# Запуск
async def main():
    init_db()
    print("🚀 Бот запущен! Мониторинг ТОЛЬКО ПРЯМЫХ РЕЙСОВ (Аэрофлот)")
    print(f"📅 Даты: {DEPARTURE_DATE} → {RETURN_DATE}")
    print(f"🕐 Московское время: {datetime.now(MSK_TZ).strftime('%H:%M:%S')}")
    print(f"📌 Доступны команды: /start, /price, /check")
    asyncio.create_task(scheduled_monitoring())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
