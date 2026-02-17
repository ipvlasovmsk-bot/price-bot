HEAD
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, asyncio, logging
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, Document, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("aiogram").setLevel(logging.WARNING)

# НАСТРОЙКИ 
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")

if not BOT_TOKEN:
    raise ValueError("❌ Не установлен BOT_TOKEN. Укажите: export BOT_TOKEN='ваш_токен'")
if not ADMIN_IDS_STR:
    raise ValueError("❌ Не установлены ADMIN_IDS. Укажите через запятую: export ADMIN_IDS='123,456'")

try:
    ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip().isdigit()]
    if not ADMIN_IDS:
        raise ValueError("Список пуст")
except Exception as e:
    raise ValueError(f"❌ Ошибка в ADMIN_IDS: {e}. Формат: '123,456,789'")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
DATA_FILE = "data.json"

# ПРОВЕРКА АДМИНА
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# УВЕДОМЛЕНИЯ АДМИНАМ О НОВЫХ ПОДПИСЧИКАХ
async def notify_admins_about_new_subscriber(user, is_reactivation: bool = False):
    """Отправляет уведомление всем админам о новом подписчике или реактивации"""
    action = "🔄 Реактивация подписки!" if is_reactivation else "🎉 Новый подписчик!"
    text = (
        f"🔔 <b>{action}</b>\n\n"
        f"👤 Имя: {user.first_name or ''} {user.last_name or ''}\n"
    )
    if user.username:
        text += f"🔗 Username: @{user.username}\n"
    text += (
        f"🆔 ID: <code>{user.id}</code>\n"
        f"⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=text,
                parse_mode="HTML"
            )
            await asyncio.sleep(0.05)  # Защита от рейт-лимитов
        except TelegramForbiddenError:
            logger.warning(f"Админ {admin_id} заблокировал бота — уведомление пропущено")
        except Exception as e:
            logger.warning(f"Ошибка отправки уведомления админу {admin_id}: {str(e)[:50]}")

# БАЗА ДАННЫХ
class DB:
    def __init__(self):
        self.data = self._load()
    
    def _load(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки данных: {e}")
        return {
            "subscribers": {}, "price_lists": {}, "campaigns": {},
            "campaign_stats": {}, "next_price_id": 1, "next_campaign_id": 1
        }
    
    def _save(self):
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")
    
    async def add_sub(self, uid, u, fn, ln):
        uid_str = str(uid)
        now = datetime.now().isoformat()
        
        # Определяем тип события для уведомления
        is_new = uid_str not in self.data["subscribers"]
        is_reactivation = False
        
        if not is_new:
            existing = self.data["subscribers"][uid_str]
            is_reactivation = not existing.get("is_active", False)
        
        # Сохраняем/обновляем данные
        self.data["subscribers"][uid_str] = {
            "username": u or "",
            "first_name": fn or "",
            "last_name": ln or "",
            "subscribed_at": now if is_new else existing.get("subscribed_at", now),
            "is_active": True,
            "unsubscribed_at": None
        }
        self._save()
        
        # Возвращаем тип события для уведомления
        if is_new:
            return "new"
        elif is_reactivation:
            return "reactivated"
        else:
            return "existing"
    
    async def rem_sub(self, uid):
        uid_str = str(uid)
        if uid_str in self.data["subscribers"]:
            self.data["subscribers"][uid_str]["is_active"] = False
            self.data["subscribers"][uid_str]["unsubscribed_at"] = datetime.now().isoformat()
            self._save()
    
    async def get_active(self):
        return [int(uid) for uid, d in self.data["subscribers"].items() if d.get("is_active", False)]
    
    async def add_price(self, fid, fn, ub):
        pid = self.data["next_price_id"]
        self.data["price_lists"][str(pid)] = {
            "id": pid, "file_id": fid, "file_name": fn,
            "uploaded_at": datetime.now().isoformat(), "uploaded_by": ub
        }
        self.data["next_price_id"] += 1
        self._save()
        return pid
    
    async def get_prices(self):
        return sorted(
            [v for v in self.data["price_lists"].values()],
            key=lambda x: x["uploaded_at"], reverse=True
        )
    
    async def create_campaign(self, plid, sb):
        cid = self.data["next_campaign_id"]
        self.data["campaigns"][str(cid)] = {
            "id": cid, "price_list_id": plid,
            "scheduled_at": datetime.now().isoformat(), "sent_at": None,
            "sent_by": sb, "total_sent": 0, "total_failed": 0, "total_opened": 0
        }
        self.data["next_campaign_id"] += 1
        self._save()
        return cid
    
    async def upd_campaign(self, cid, sent, failed):
        cid_str = str(cid)
        if cid_str in self.data["campaigns"]:
            self.data["campaigns"][cid_str]["sent_at"] = datetime.now().isoformat()
            self.data["campaigns"][cid_str]["total_sent"] = sent
            self.data["campaigns"][cid_str]["total_failed"] = failed
            self._save()
    
    async def inc_opened(self, cid):
        cid_str = str(cid)
        if cid_str in self.data["campaigns"]:
            self.data["campaigns"][cid_str]["total_opened"] = self.data["campaigns"][cid_str].get("total_opened", 0) + 1
            self._save()
    
    async def track_open(self, cid, uid, method="button"):
        cid_str, uid_str = str(cid), str(uid)
        if cid_str not in self.data["campaign_stats"]:
            self.data["campaign_stats"][cid_str] = {}
        if uid_str not in self.data["campaign_stats"][cid_str]:
            self.data["campaign_stats"][cid_str][uid_str] = {
                "sent_at": datetime.now().isoformat(), "delivered": False,
                "error_message": None, "opened_at": None, "opened_method": None
            }
        if not self.data["campaign_stats"][cid_str][uid_str].get("opened_at"):
            self.data["campaign_stats"][cid_str][uid_str]["opened_at"] = datetime.now().isoformat()
            self.data["campaign_stats"][cid_str][uid_str]["opened_method"] = method
            self._save()
            await self.inc_opened(cid)
    
    async def get_stats(self, cid):
        cid_str = str(cid)
        stats = self.data["campaign_stats"].get(cid_str, {})
        total = len(stats)
        delivered = sum(1 for v in stats.values() if v.get("delivered"))
        opened = sum(1 for v in stats.values() if v.get("opened_at"))
        failed = sum(1 for v in stats.values() if v.get("error_message"))
        return {
            "total": total, "delivered": delivered, "opened": opened,
            "failed": failed, "open_rate": round(opened / delivered * 100, 2) if delivered > 0 else 0
        }
    
    async def get_overall(self):
        subs = {"total": len(self.data["subscribers"]), "active": len(await self.get_active())}
        camps = list(self.data["campaigns"].values())
        total_sent = sum(c.get("total_sent", 0) for c in camps)
        total_opened = sum(c.get("total_opened", 0) for c in camps)
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        new_subs = sum(1 for s in self.data["subscribers"].values() if s.get("subscribed_at", "") > week_ago)
        unsub = sum(1 for s in self.data["subscribers"].values() if s.get("unsubscribed_at", "") and s["unsubscribed_at"] > week_ago)
        avg_rate = round(total_opened / total_sent * 100, 2) if total_sent > 0 else 0
        return {
            "subscribers": subs, "total_campaigns": len(camps),
            "total_sent": total_sent, "total_opened": total_opened,
            "avg_open_rate": avg_rate, "new_subs_week": new_subs, "unsub_week": unsub
        }

db = DB()

class AS(StatesGroup):
    waiting_for_price_file = State()
    waiting_for_campaign_choice = State()
    waiting_for_campaign_caption = State()

# ОСНОВНЫЕ ХЕНДЛЕРЫ
@router.message(Command("start"))
async def start(m: Message):
    u = m.from_user
    status = await db.add_sub(u.id, u.username, u.first_name, u.last_name)
    
    # Отправляем уведомление админам только при новой подписке или реактивации
    if status in ("new", "reactivated"):
        asyncio.create_task(
            notify_admins_about_new_subscriber(u, is_reactivation=(status == "reactivated"))
        )
    
    await m.answer(
        "👋 Добро пожаловать!\n\n"
        "Вы подписаны на получение прайс-листов.\n"
        "Чтобы отписаться, нажмите /stop"
    )

@router.message(Command("stop"))
async def stop(m: Message):
    await db.rem_sub(m.from_user.id)
    await m.answer(
        "❌ Вы отписались от рассылки прайс-листов.\n"
        "Чтобы подписаться снова, напишите /start"
    )

@router.callback_query(F.data.startswith("track_open:"))
async def track_open(cb: CallbackQuery):
    _, cid, uid = cb.data.split(":")
    if int(uid) != cb.from_user.id:
        await cb.answer("❌ Это не ваша кнопка!", show_alert=True)
        return
    await db.track_open(int(cid), int(uid))
    await cb.answer("✅ Спасибо! Прайс-лист получен.", show_alert=False)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except:
        pass

@router.callback_query(F.data.startswith("unsubscribe:"))
async def unsub(cb: CallbackQuery):
    _, uid = cb.data.split(":")
    if int(uid) != cb.from_user.id:
        await cb.answer("❌ Это не ваша кнопка!", show_alert=True)
        return
    await db.rem_sub(cb.from_user.id)
    await cb.answer("❌ Вы отписались от рассылки.", show_alert=True)
    try:
        await cb.message.delete()
    except:
        pass

@router.message(Command("admin"))
async def admin(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("🚫 Доступ запрещён")
        return
    await m.answer(
        "👑 <b>Панель администратора</b>\n\n"
        "Команды:\n"
        "/upload — загрузить прайс-лист\n"
        "/send — отправить рассылку\n"
        "/stats — статистика\n"
        "/campaigns — история рассылок",
        parse_mode="HTML"
    )

@router.message(Command("upload"))
async def upload(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    await m.answer("📤 Отправьте файл прайс-листа (PDF, XLSX, JPG, PNG)")
    await state.set_state(AS.waiting_for_price_file)

@router.message(AS.waiting_for_price_file, F.document)
async def save_file(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    doc = m.document
    if os.path.splitext(doc.file_name)[1].lower() not in ['.pdf', '.xlsx', '.xls', '.jpg', '.jpeg', '.png']:
        await m.answer("❌ Неподдерживаемый формат. Разрешены: PDF, XLSX, XLS, JPG, PNG")
        return
    pid = await db.add_price(doc.file_id, doc.file_name, m.from_user.id)
    await state.clear()
    await m.answer(f"✅ Прайс-лист «{doc.file_name}» сохранён под номером #{pid}")

@router.message(Command("send"))
async def send_start(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    prices = await db.get_prices()
    if not prices:
        await m.answer("📭 Нет загруженных прайс-листов. Сначала /upload")
        return
    text = "📋 Выберите прайс-лист:\n\n"
    for p in prices[:10]:
        uploaded = datetime.fromisoformat(p["uploaded_at"]).strftime("%d.%m.%Y")
        text += f"{p['id']}. {p['file_name']} ({uploaded})\n"
    text += "\nОтправьте номер:"
    await m.answer(text)
    await state.set_state(AS.waiting_for_campaign_choice)
    await state.update_data(prices={p["id"]: p for p in prices})

@router.message(AS.waiting_for_campaign_choice, F.text)
async def send_choice(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    try:
        pid = int(m.text)
    except:
        await m.answer("❌ Неверный номер. Попробуйте снова.")
        return
    data = await state.get_data()
    prices = data.get("prices", {})
    if pid not in prices:
        await m.answer("❌ Прайс-лист не найден.")
        return
    await state.update_data(selected_price=prices[pid])
    await m.answer(f"📄 Вы выбрали: {prices[pid]['file_name']}\n\nНапишите текст рассылки (или «-» для пропуска):")
    await state.set_state(AS.waiting_for_campaign_caption)

@router.message(AS.waiting_for_campaign_caption, F.text)
async def send_final(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    data = await state.get_data()
    price = data.get("selected_price")
    if not price:
        await m.answer("❌ Ошибка выбора прайс-листа")
        return
    caption = m.text if m.text != "-" else "📄 Ваш прайс-лист"
    cid = await db.create_campaign(price["id"], m.from_user.id)
    await m.answer(f"🚀 Начинаю рассылку {price['file_name']}...\nЭто займёт несколько минут.")
    asyncio.create_task(send_task(cid, price["file_id"], caption, m))
    await state.clear()

async def send_task(cid, fid, caption, admin_msg):
    subs = await db.get_active()
    if not subs:
        await admin_msg.answer("📭 Нет активных подписчиков")
        return
    total, sent, failed = len(subs), 0, 0
    logger.info(f"🚀 Рассылка {total} подписчикам...")
    for i in range(0, total, 25):
        batch = subs[i:i+25]
        tasks = [send_user(uid, cid, fid, caption) for uid in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if r is True: sent += 1
            else: failed += 1
        if (i + 25) % 100 == 0 or i + 25 >= total:
            progress = min((i + 25) / total * 100, 100)
            await admin_msg.answer(f"📨 {progress:.0f}% ({sent+failed}/{total})\n✅ {sent} ❌ {failed}")
        if i + 25 < total: await asyncio.sleep(1.5)
    await db.upd_campaign(cid, sent, failed)
    await admin_msg.answer(
        f"✅ Рассылка завершена!\n🆔 ID: {cid}\n📤 Отправлено: {sent}\n❌ Ошибок: {failed}\n📊 /campaigns для деталей"
    )

async def send_user(uid, cid, fid, caption):
    try:
        if str(cid) not in db.data["campaign_stats"]:
            db.data["campaign_stats"][str(cid)] = {}
        db.data["campaign_stats"][str(cid)][str(uid)] = {
            "sent_at": datetime.now().isoformat(), "delivered": False,
            "error_message": None, "opened_at": None, "opened_method": None
        }
        db._save()
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Получил прайс", callback_data=f"track_open:{cid}:{uid}")],
            [InlineKeyboardButton(text="❌ Отписаться", callback_data=f"unsubscribe:{uid}")]
        ])
        await bot.send_document(uid, fid, caption=caption, reply_markup=kb)
        db.data["campaign_stats"][str(cid)][str(uid)]["delivered"] = True
        db._save()
        return True
    except TelegramForbiddenError:
        await db.rem_sub(uid)
        db.data["campaign_stats"][str(cid)][str(uid)]["error_message"] = "user_blocked"
        db._save()
        return False
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        return await send_user(uid, cid, fid, caption)
    except Exception as e:
        db.data["campaign_stats"][str(cid)][str(uid)]["error_message"] = str(e)[:100]
        db._save()
        return False

@router.message(Command("stats"))
async def stats(m: Message):
    if not is_admin(m.from_user.id): return
    s = await db.get_overall()
    await m.answer(
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего: {s['subscribers']['total']}\n✅ Активных: {s['subscribers']['active']}\n\n"
        f"📬 Рассылок: {s['total_campaigns']}\n📤 Отправлено: {s['total_sent']}\n"
        f"👁️ Открыто: {s['total_opened']}\n📈 Открытий: {s['avg_open_rate']}%\n\n"
        f"🆕 Новых за неделю: {s['new_subs_week']}\n❌ Отписалось: {s['unsub_week']}",
        parse_mode="HTML"
    )

@router.message(Command("campaigns"))
async def campaigns(m: Message):
    if not is_admin(m.from_user.id): return
    camps = sorted(
        [{"id": int(k), **v} for k, v in db.data["campaigns"].items()],
        key=lambda x: x.get("sent_at") or x.get("scheduled_at"), reverse=True
    )[:5]
    if not camps:
        await m.answer("📭 Нет рассылок")
        return
    text = "📋 <b>История рассылок</b>\n\n"
    for c in camps:
        sent_at = c.get("sent_at") or c.get("scheduled_at")
        try:
            sent_at = datetime.fromisoformat(sent_at).strftime("%d.%m %H:%M")
        except:
            sent_at = "—"
        st = await db.get_stats(c["id"])
        text += (
            f"🆔 {c['id']} | {c.get('file_name', '—')}\n"
            f"🕐 {sent_at}\n"
            f"📤 {st['delivered']} 👁️ {st['opened']} ({st['open_rate']}%)\n"
            f"❌ {st['failed']}\n────────────\n"
        )
    await m.answer(text, parse_mode="HTML")

async def main():
    logger.info("="*50)
    logger.info(f"🚀 Бот запущен! Админы: {ADMIN_IDS}")
    logger.info(f"💾 Данные: {DATA_FILE}")
    logger.info("="*50)
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
=======
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, asyncio, logging
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, CallbackQuery, Document, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("aiogram").setLevel(logging.WARNING)

# НАСТРОЙКИ
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_IDS_STR = os.environ.get("ADMIN_IDS", "")

if not BOT_TOKEN:
    raise ValueError("❌ Не установлен BOT_TOKEN. Укажите: export BOT_TOKEN='ваш_токен'")
if not ADMIN_IDS_STR:
    raise ValueError("❌ Не установлены ADMIN_IDS. Укажите через запятую: export ADMIN_IDS='123,456'")

try:
    ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip().isdigit()]
    if not ADMIN_IDS:
        raise ValueError("Список пуст")
except Exception as e:
    raise ValueError(f"❌ Ошибка в ADMIN_IDS: {e}. Формат: '123,456,789'")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
DATA_FILE = "data.json"

# ПРОВЕРКА АДМИНА 
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# УВЕДОМЛЕНИЯ АДМИНАМ О НОВЫХ ПОДПИСЧИКАХ
async def notify_admins_about_new_subscriber(user, is_reactivation: bool = False):
    """Отправляет уведомление всем админам о новом подписчике или реактивации"""
    action = "🔄 Реактивация подписки!" if is_reactivation else "🎉 Новый подписчик!"
    text = (
        f"🔔 <b>{action}</b>\n\n"
        f"👤 Имя: {user.first_name or ''} {user.last_name or ''}\n"
    )
    if user.username:
        text += f"🔗 Username: @{user.username}\n"
    text += (
        f"🆔 ID: <code>{user.id}</code>\n"
        f"⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
    )
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=text,
                parse_mode="HTML"
            )
            await asyncio.sleep(0.05)  # Защита от рейт-лимитов
        except TelegramForbiddenError:
            logger.warning(f"Админ {admin_id} заблокировал бота — уведомление пропущено")
        except Exception as e:
            logger.warning(f"Ошибка отправки уведомления админу {admin_id}: {str(e)[:50]}")

# БАЗА ДАННЫХ
class DB:
    def __init__(self):
        self.data = self._load()
    
    def _load(self):
        try:
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки данных: {e}")
        return {
            "subscribers": {}, "price_lists": {}, "campaigns": {},
            "campaign_stats": {}, "next_price_id": 1, "next_campaign_id": 1
        }
    
    def _save(self):
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")
    
    async def add_sub(self, uid, u, fn, ln):
        uid_str = str(uid)
        now = datetime.now().isoformat()
        
        # Определяем тип события для уведомления
        is_new = uid_str not in self.data["subscribers"]
        is_reactivation = False
        
        if not is_new:
            existing = self.data["subscribers"][uid_str]
            is_reactivation = not existing.get("is_active", False)
        
        # Сохраняем/обновляем данные
        self.data["subscribers"][uid_str] = {
            "username": u or "",
            "first_name": fn or "",
            "last_name": ln or "",
            "subscribed_at": now if is_new else existing.get("subscribed_at", now),
            "is_active": True,
            "unsubscribed_at": None
        }
        self._save()
        
        # Возвращаем тип события для уведомления
        if is_new:
            return "new"
        elif is_reactivation:
            return "reactivated"
        else:
            return "existing"
    
    async def rem_sub(self, uid):
        uid_str = str(uid)
        if uid_str in self.data["subscribers"]:
            self.data["subscribers"][uid_str]["is_active"] = False
            self.data["subscribers"][uid_str]["unsubscribed_at"] = datetime.now().isoformat()
            self._save()
    
    async def get_active(self):
        return [int(uid) for uid, d in self.data["subscribers"].items() if d.get("is_active", False)]
    
    async def add_price(self, fid, fn, ub):
        pid = self.data["next_price_id"]
        self.data["price_lists"][str(pid)] = {
            "id": pid, "file_id": fid, "file_name": fn,
            "uploaded_at": datetime.now().isoformat(), "uploaded_by": ub
        }
        self.data["next_price_id"] += 1
        self._save()
        return pid
    
    async def get_prices(self):
        return sorted(
            [v for v in self.data["price_lists"].values()],
            key=lambda x: x["uploaded_at"], reverse=True
        )
    
    async def create_campaign(self, plid, sb):
        cid = self.data["next_campaign_id"]
        self.data["campaigns"][str(cid)] = {
            "id": cid, "price_list_id": plid,
            "scheduled_at": datetime.now().isoformat(), "sent_at": None,
            "sent_by": sb, "total_sent": 0, "total_failed": 0, "total_opened": 0
        }
        self.data["next_campaign_id"] += 1
        self._save()
        return cid
    
    async def upd_campaign(self, cid, sent, failed):
        cid_str = str(cid)
        if cid_str in self.data["campaigns"]:
            self.data["campaigns"][cid_str]["sent_at"] = datetime.now().isoformat()
            self.data["campaigns"][cid_str]["total_sent"] = sent
            self.data["campaigns"][cid_str]["total_failed"] = failed
            self._save()
    
    async def inc_opened(self, cid):
        cid_str = str(cid)
        if cid_str in self.data["campaigns"]:
            self.data["campaigns"][cid_str]["total_opened"] = self.data["campaigns"][cid_str].get("total_opened", 0) + 1
            self._save()
    
    async def track_open(self, cid, uid, method="button"):
        cid_str, uid_str = str(cid), str(uid)
        if cid_str not in self.data["campaign_stats"]:
            self.data["campaign_stats"][cid_str] = {}
        if uid_str not in self.data["campaign_stats"][cid_str]:
            self.data["campaign_stats"][cid_str][uid_str] = {
                "sent_at": datetime.now().isoformat(), "delivered": False,
                "error_message": None, "opened_at": None, "opened_method": None
            }
        if not self.data["campaign_stats"][cid_str][uid_str].get("opened_at"):
            self.data["campaign_stats"][cid_str][uid_str]["opened_at"] = datetime.now().isoformat()
            self.data["campaign_stats"][cid_str][uid_str]["opened_method"] = method
            self._save()
            await self.inc_opened(cid)
    
    async def get_stats(self, cid):
        cid_str = str(cid)
        stats = self.data["campaign_stats"].get(cid_str, {})
        total = len(stats)
        delivered = sum(1 for v in stats.values() if v.get("delivered"))
        opened = sum(1 for v in stats.values() if v.get("opened_at"))
        failed = sum(1 for v in stats.values() if v.get("error_message"))
        return {
            "total": total, "delivered": delivered, "opened": opened,
            "failed": failed, "open_rate": round(opened / delivered * 100, 2) if delivered > 0 else 0
        }
    
    async def get_overall(self):
        subs = {"total": len(self.data["subscribers"]), "active": len(await self.get_active())}
        camps = list(self.data["campaigns"].values())
        total_sent = sum(c.get("total_sent", 0) for c in camps)
        total_opened = sum(c.get("total_opened", 0) for c in camps)
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()
        new_subs = sum(1 for s in self.data["subscribers"].values() if s.get("subscribed_at", "") > week_ago)
        unsub = sum(1 for s in self.data["subscribers"].values() if s.get("unsubscribed_at", "") and s["unsubscribed_at"] > week_ago)
        avg_rate = round(total_opened / total_sent * 100, 2) if total_sent > 0 else 0
        return {
            "subscribers": subs, "total_campaigns": len(camps),
            "total_sent": total_sent, "total_opened": total_opened,
            "avg_open_rate": avg_rate, "new_subs_week": new_subs, "unsub_week": unsub
        }

db = DB()

class AS(StatesGroup):
    waiting_for_price_file = State()
    waiting_for_campaign_choice = State()
    waiting_for_campaign_caption = State()

# ОСНОВНЫЕ ХЕНДЛЕРЫ
@router.message(Command("start"))
async def start(m: Message):
    u = m.from_user
    status = await db.add_sub(u.id, u.username, u.first_name, u.last_name)
    
    # Отправляем уведомление админам только при новой подписке или реактивации
    if status in ("new", "reactivated"):
        asyncio.create_task(
            notify_admins_about_new_subscriber(u, is_reactivation=(status == "reactivated"))
        )
    
    await m.answer(
        "👋 Добро пожаловать!\n\n"
        "Вы подписаны на получение прайс-листов.\n"
        "Чтобы отписаться, нажмите /stop"
    )

@router.message(Command("stop"))
async def stop(m: Message):
    await db.rem_sub(m.from_user.id)
    await m.answer(
        "❌ Вы отписались от рассылки прайс-листов.\n"
        "Чтобы подписаться снова, напишите /start"
    )

@router.callback_query(F.data.startswith("track_open:"))
async def track_open(cb: CallbackQuery):
    _, cid, uid = cb.data.split(":")
    if int(uid) != cb.from_user.id:
        await cb.answer("❌ Это не ваша кнопка!", show_alert=True)
        return
    await db.track_open(int(cid), int(uid))
    await cb.answer("✅ Спасибо! Прайс-лист получен.", show_alert=False)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except:
        pass

@router.callback_query(F.data.startswith("unsubscribe:"))
async def unsub(cb: CallbackQuery):
    _, uid = cb.data.split(":")
    if int(uid) != cb.from_user.id:
        await cb.answer("❌ Это не ваша кнопка!", show_alert=True)
        return
    await db.rem_sub(cb.from_user.id)
    await cb.answer("❌ Вы отписались от рассылки.", show_alert=True)
    try:
        await cb.message.delete()
    except:
        pass

@router.message(Command("admin"))
async def admin(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("🚫 Доступ запрещён")
        return
    await m.answer(
        "👑 <b>Панель администратора</b>\n\n"
        "Команды:\n"
        "/upload — загрузить прайс-лист\n"
        "/send — отправить рассылку\n"
        "/stats — статистика\n"
        "/campaigns — история рассылок",
        parse_mode="HTML"
    )

@router.message(Command("upload"))
async def upload(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    await m.answer("📤 Отправьте файл прайс-листа (PDF, XLSX, JPG, PNG)")
    await state.set_state(AS.waiting_for_price_file)

@router.message(AS.waiting_for_price_file, F.document)
async def save_file(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    doc = m.document
    if os.path.splitext(doc.file_name)[1].lower() not in ['.pdf', '.xlsx', '.xls', '.jpg', '.jpeg', '.png']:
        await m.answer("❌ Неподдерживаемый формат. Разрешены: PDF, XLSX, XLS, JPG, PNG")
        return
    pid = await db.add_price(doc.file_id, doc.file_name, m.from_user.id)
    await state.clear()
    await m.answer(f"✅ Прайс-лист «{doc.file_name}» сохранён под номером #{pid}")

@router.message(Command("send"))
async def send_start(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    prices = await db.get_prices()
    if not prices:
        await m.answer("📭 Нет загруженных прайс-листов. Сначала /upload")
        return
    text = "📋 Выберите прайс-лист:\n\n"
    for p in prices[:10]:
        uploaded = datetime.fromisoformat(p["uploaded_at"]).strftime("%d.%m.%Y")
        text += f"{p['id']}. {p['file_name']} ({uploaded})\n"
    text += "\nОтправьте номер:"
    await m.answer(text)
    await state.set_state(AS.waiting_for_campaign_choice)
    await state.update_data(prices={p["id"]: p for p in prices})

@router.message(AS.waiting_for_campaign_choice, F.text)
async def send_choice(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    try:
        pid = int(m.text)
    except:
        await m.answer("❌ Неверный номер. Попробуйте снова.")
        return
    data = await state.get_data()
    prices = data.get("prices", {})
    if pid not in prices:
        await m.answer("❌ Прайс-лист не найден.")
        return
    await state.update_data(selected_price=prices[pid])
    await m.answer(f"📄 Вы выбрали: {prices[pid]['file_name']}\n\nНапишите текст рассылки (или «-» для пропуска):")
    await state.set_state(AS.waiting_for_campaign_caption)

@router.message(AS.waiting_for_campaign_caption, F.text)
async def send_final(m: Message, state: FSMContext):
    if not is_admin(m.from_user.id): return
    data = await state.get_data()
    price = data.get("selected_price")
    if not price:
        await m.answer("❌ Ошибка выбора прайс-листа")
        return
    caption = m.text if m.text != "-" else "📄 Ваш прайс-лист"
    cid = await db.create_campaign(price["id"], m.from_user.id)
    await m.answer(f"🚀 Начинаю рассылку {price['file_name']}...\nЭто займёт несколько минут.")
    asyncio.create_task(send_task(cid, price["file_id"], caption, m))
    await state.clear()

async def send_task(cid, fid, caption, admin_msg):
    subs = await db.get_active()
    if not subs:
        await admin_msg.answer("📭 Нет активных подписчиков")
        return
    total, sent, failed = len(subs), 0, 0
    logger.info(f"🚀 Рассылка {total} подписчикам...")
    for i in range(0, total, 25):
        batch = subs[i:i+25]
        tasks = [send_user(uid, cid, fid, caption) for uid in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if r is True: sent += 1
            else: failed += 1
        if (i + 25) % 100 == 0 or i + 25 >= total:
            progress = min((i + 25) / total * 100, 100)
            await admin_msg.answer(f"📨 {progress:.0f}% ({sent+failed}/{total})\n✅ {sent} ❌ {failed}")
        if i + 25 < total: await asyncio.sleep(1.5)
    await db.upd_campaign(cid, sent, failed)
    await admin_msg.answer(
        f"✅ Рассылка завершена!\n🆔 ID: {cid}\n📤 Отправлено: {sent}\n❌ Ошибок: {failed}\n📊 /campaigns для деталей"
    )

async def send_user(uid, cid, fid, caption):
    try:
        if str(cid) not in db.data["campaign_stats"]:
            db.data["campaign_stats"][str(cid)] = {}
        db.data["campaign_stats"][str(cid)][str(uid)] = {
            "sent_at": datetime.now().isoformat(), "delivered": False,
            "error_message": None, "opened_at": None, "opened_method": None
        }
        db._save()
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Получил прайс", callback_data=f"track_open:{cid}:{uid}")],
            [InlineKeyboardButton(text="❌ Отписаться", callback_data=f"unsubscribe:{uid}")]
        ])
        await bot.send_document(uid, fid, caption=caption, reply_markup=kb)
        db.data["campaign_stats"][str(cid)][str(uid)]["delivered"] = True
        db._save()
        return True
    except TelegramForbiddenError:
        await db.rem_sub(uid)
        db.data["campaign_stats"][str(cid)][str(uid)]["error_message"] = "user_blocked"
        db._save()
        return False
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        return await send_user(uid, cid, fid, caption)
    except Exception as e:
        db.data["campaign_stats"][str(cid)][str(uid)]["error_message"] = str(e)[:100]
        db._save()
        return False

@router.message(Command("stats"))
async def stats(m: Message):
    if not is_admin(m.from_user.id): return
    s = await db.get_overall()
    await m.answer(
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего: {s['subscribers']['total']}\n✅ Активных: {s['subscribers']['active']}\n\n"
        f"📬 Рассылок: {s['total_campaigns']}\n📤 Отправлено: {s['total_sent']}\n"
        f"👁️ Открыто: {s['total_opened']}\n📈 Открытий: {s['avg_open_rate']}%\n\n"
        f"🆕 Новых за неделю: {s['new_subs_week']}\n❌ Отписалось: {s['unsub_week']}",
        parse_mode="HTML"
    )

@router.message(Command("campaigns"))
async def campaigns(m: Message):
    if not is_admin(m.from_user.id): return
    camps = sorted(
        [{"id": int(k), **v} for k, v in db.data["campaigns"].items()],
        key=lambda x: x.get("sent_at") or x.get("scheduled_at"), reverse=True
    )[:5]
    if not camps:
        await m.answer("📭 Нет рассылок")
        return
    text = "📋 <b>История рассылок</b>\n\n"
    for c in camps:
        sent_at = c.get("sent_at") or c.get("scheduled_at")
        try:
            sent_at = datetime.fromisoformat(sent_at).strftime("%d.%m %H:%M")
        except:
            sent_at = "—"
        st = await db.get_stats(c["id"])
        text += (
            f"🆔 {c['id']} | {c.get('file_name', '—')}\n"
            f"🕐 {sent_at}\n"
            f"📤 {st['delivered']} 👁️ {st['opened']} ({st['open_rate']}%)\n"
            f"❌ {st['failed']}\n────────────\n"
        )
    await m.answer(text, parse_mode="HTML")

async def main():
    logger.info("="*50)
    logger.info(f"🚀 Бот запущен! Админы: {ADMIN_IDS}")
    logger.info(f"💾 Данные: {DATA_FILE}")
    logger.info("="*50)
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":

    asyncio.run(main())