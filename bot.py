import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("lecture-bot")

# ---------- config (all from environment variables) ----------
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
DB_NAME = os.getenv("DB_NAME", "lecture_bot")
DB_CHANNEL_ID = int(os.environ["DB_CHANNEL_ID"])  # private channel, e.g. -1001234567890
ADMIN_IDS = [int(x) for x in os.environ["ADMIN_IDS"].replace(" ", "").split(",") if x]
AUTO_DELETE_HOURS = float(os.getenv("AUTO_DELETE_HOURS", "3"))
PROTECT_CONTENT = os.getenv("PROTECT_CONTENT", "false").lower() == "true"

if not ADMIN_IDS:
    raise SystemExit("ADMIN_IDS must contain at least one Telegram user id")

# Python 3.14 no longer creates an event loop implicitly, so create one up front.
# Motor and run_polling() both pick up this same loop.
asyncio.set_event_loop(asyncio.new_event_loop())

db = AsyncIOMotorClient(MONGO_URI)[DB_NAME]
admin_only = filters.User(user_id=ADMIN_IDS)

COURSE, CHAPTER, FIRST_ID, LAST_ID = range(4)

DEFAULT_NOTICE_MESSAGE = (
    "⏳ These lectures will be deleted in {hours}.\n\n"
    "💎 For permanent access, message {contact}"
)
DEFAULT_DELETE_MESSAGE = (
    "🗑 Your lectures were deleted after {hours}.\n\n"
    "▶️ To get them again, send /start\n"
    "💎 For permanent access, message {contact}"
)


async def get_message(key, default):
    doc = await db.settings.find_one({"_id": key})
    text = doc["text"] if doc else default
    cdoc = await db.settings.find_one({"_id": "contact"})
    if cdoc and cdoc.get("value"):
        text = text.replace("{contact}", cdoc["value"])
    else:  # contact not set yet: drop any line that needs it
        text = "\n".join(line for line in text.split("\n") if "{contact}" not in line)
    return text.replace("{hours}", pretty_hours()).strip()


async def get_delete_message():
    return await get_message("delete_message", DEFAULT_DELETE_MESSAGE)


async def get_notice_message():
    return await get_message("notice_message", DEFAULT_NOTICE_MESSAGE)


def utcnow():
    return datetime.now(timezone.utc)


def chunks(seq, n=100):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def pretty_hours():
    h = AUTO_DELETE_HOURS
    return f"{int(h)} hour(s)" if h == int(h) else f"{h} hours"


# ---------- force subscribe ----------
BROADCAST_WORKERS = 20
JOIN_TEXT = "🔒 Please join the channel(s) below to use this bot, then tap “I've joined”."


async def get_fsub():
    doc = await db.settings.find_one({"_id": "force_sub"})
    return doc["channels"] if doc else []


async def missing_channels(bot, user_id):
    channels = await get_fsub()

    async def check(ch):
        try:
            m = await bot.get_chat_member(ch["chat_id"], user_id)
            return m.status in ("member", "administrator", "creator") or (
                m.status == "restricted" and m.is_member
            )
        except TelegramError as e:
            log.warning("Force-sub check failed for %s: %s", ch["chat_id"], e)
            return True  # never lock users out because of a bot misconfiguration

    results = await asyncio.gather(*(check(c) for c in channels))
    return [c for c, ok in zip(channels, results) if not ok]


def join_markup(missing):
    rows = [[InlineKeyboardButton(f"📢 Join {c['title']}", url=c["link"])] for c in missing]
    rows.append([InlineKeyboardButton("✅ I've joined", callback_data="chk:")])
    return InlineKeyboardMarkup(rows)


# ---------- user side ----------
async def courses_markup():
    courses = await db.courses.find().sort("name_lower", 1).to_list(200)
    if not courses:
        return None
    buttons = [InlineKeyboardButton(c["name"], callback_data=f"c:{c['_id']}") for c in courses]
    rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
    return InlineKeyboardMarkup(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db.users.update_one(
        {"_id": user.id},
        {"$set": {"name": user.full_name, "blocked": False}, "$setOnInsert": {"joined": utcnow()}},
        upsert=True,
    )
    if user.id in ADMIN_IDS:
        await update.message.reply_text(
            "👑 Admin mode\n"
            "/addbatch – add a course / chapter with lectures\n"
            "/broadcast – reply to a message to send it to all users\n"
            "/manage – delete chapters or courses\n"
            "/stats – bot statistics\n"
            "/setnoticemsg – message shown with the lectures (before deletion)\n"
            "/setdeletemsg – message shown after lectures are deleted\n"
            "/setcontact @username – contact for permanent access\n"
            "/fsub – list force-subscribe channels\n"
            "/addfsub <channel id or @username> – require a channel\n"
            "/delfsub <channel id> – remove a channel\n\n"
            "Users will see the course list below:"
        )
    if user.id not in ADMIN_IDS:
        missing = await missing_channels(context.bot, user.id)
        if missing:
            await update.message.reply_text(JOIN_TEXT, reply_markup=join_markup(missing))
            return
    markup = await courses_markup()
    if not markup:
        await update.message.reply_text(
            "No courses added yet." + (" Use /addbatch to add the first one." if user.id in ADMIN_IDS else "")
        )
        return
    await update.message.reply_text("📚 Choose a course:", reply_markup=markup)


async def send_lectures(context, chat_id, chapter):
    ids = list(range(chapter["first_id"], chapter["last_id"] + 1))
    sent = []
    for part in chunks(ids):
        for attempt in range(3):
            try:
                res = await context.bot.copy_messages(
                    chat_id, chapter["channel_id"], part, protect_content=PROTECT_CONTENT
                )
                sent += [m.message_id for m in res]
                break
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
        await asyncio.sleep(0.3)
    return sent


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    action, _, value = q.data.partition(":")

    if update.effective_user.id not in ADMIN_IDS:
        missing = await missing_channels(context.bot, update.effective_user.id)
        if missing:
            if action == "chk":
                await q.answer("❌ You haven't joined all channels yet.", show_alert=True)
            else:
                await q.answer()
                await q.edit_message_text(JOIN_TEXT, reply_markup=join_markup(missing))
            return
    await q.answer()

    if action == "chk":
        markup = await courses_markup()
        await q.edit_message_text("📚 Choose a course:" if markup else "No courses added yet.", reply_markup=markup)
        return

    if action == "home":
        markup = await courses_markup()
        await q.edit_message_text("📚 Choose a course:", reply_markup=markup)
        return

    try:
        oid = ObjectId(value)
    except InvalidId:
        return

    if action == "c":
        course = await db.courses.find_one({"_id": oid})
        chapters = await db.chapters.find({"course_id": oid}).sort("created_at", 1).to_list(300)
        if not course or not chapters:
            await q.edit_message_text(
                "No chapters yet.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="home:")]]),
            )
            return
        rows = [[InlineKeyboardButton(c["name"], callback_data=f"ch:{c['_id']}")] for c in chapters]
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="home:")])
        await q.edit_message_text(f"📖 {course['name']} – choose a chapter:", reply_markup=InlineKeyboardMarkup(rows))

    elif action == "ch":
        chapter = await db.chapters.find_one({"_id": oid})
        if not chapter:
            return
        chat_id = q.message.chat_id
        status = await context.bot.send_message(chat_id, f"📤 Sending “{chapter['name']}”…")
        sent = await send_lectures(context, chat_id, chapter)
        await status.delete()
        if not sent:
            await context.bot.send_message(chat_id, "No lectures found in this chapter.")
            return
        notice = await context.bot.send_message(chat_id, await get_notice_message())
        # Stored in MongoDB so the deletion survives Heroku dyno restarts.
        await db.scheduled_deletes.insert_one(
            {
                "chat_id": chat_id,
                "message_ids": sent + [notice.message_id],
                "delete_at": utcnow() + timedelta(hours=AUTO_DELETE_HOURS),
            }
        )
        course = await db.courses.find_one({"_id": chapter["course_id"]})
        await db.downloads.insert_one(
            {
                "user_id": update.effective_user.id,
                "chapter_id": chapter["_id"],
                "label": f"{course['name'] if course else '?'} › {chapter['name']}",
                "at": utcnow(),
            }
        )


# ---------- auto-delete job ----------
async def cleanup(context: ContextTypes.DEFAULT_TYPE):
    while True:
        doc = await db.scheduled_deletes.find_one_and_delete({"delete_at": {"$lte": utcnow()}})
        if not doc:
            break
        for part in chunks(doc["message_ids"]):
            try:
                await context.bot.delete_messages(doc["chat_id"], part)
            except TelegramError as e:
                log.warning("Delete failed for chat %s: %s", doc["chat_id"], e)
        try:
            await context.bot.send_message(
                doc["chat_id"],
                await get_delete_message(),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("📚 Open courses", callback_data="home:")]]
                ),
            )
        except TelegramError as e:
            log.warning("Could not send delete notice to %s: %s", doc["chat_id"], e)


# ---------- admin: add batch ----------
async def addbatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Course name? (e.g. RD, TNM, Acid, YSY)\n/cancel to stop.")
    return COURSE


async def got_course(update, context):
    context.user_data["course"] = update.message.text.strip()
    await update.message.reply_text("Chapter name?")
    return CHAPTER


async def got_chapter(update, context):
    context.user_data["chapter"] = update.message.text.strip()
    await update.message.reply_text("First message ID in the private channel?")
    return FIRST_ID


async def got_first(update, context):
    if not update.message.text.strip().isdigit():
        await update.message.reply_text("Send a number, e.g. 101")
        return FIRST_ID
    context.user_data["first"] = int(update.message.text)
    await update.message.reply_text("Last message ID?")
    return LAST_ID


async def got_last(update, context):
    text = update.message.text.strip()
    first = context.user_data["first"]
    if not text.isdigit() or int(text) < first:
        await update.message.reply_text(f"Send a number that is {first} or higher.")
        return LAST_ID
    last = int(text)
    name = context.user_data["course"]
    course = await db.courses.find_one_and_update(
        {"name_lower": name.lower()},
        {"$setOnInsert": {"name": name, "name_lower": name.lower()}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    await db.chapters.insert_one(
        {
            "course_id": course["_id"],
            "name": context.user_data["chapter"],
            "channel_id": DB_CHANNEL_ID,
            "first_id": first,
            "last_id": last,
            "created_at": utcnow(),
        }
    )
    await update.message.reply_text(
        f"✅ Saved: {course['name']} › {context.user_data['chapter']}\n"
        f"Messages {first} → {last} ({last - first + 1} IDs)"
    )
    context.user_data.clear()
    return ConversationHandler.END


async def cancel(update, context):
    context.user_data.clear()
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# ---------- admin: broadcast / stats ----------
async def do_broadcast(bot, src_chat, src_msg, admin_chat):
    ids = [u["_id"] async for u in db.users.find({"blocked": {"$ne": True}}, {"_id": 1})]
    total = len(ids)
    queue = asyncio.Queue()
    for uid in ids:
        queue.put_nowait(uid)
    counts = {"ok": 0, "fail": 0, "blocked": 0}
    blocked_ids = []
    status = await bot.send_message(admin_chat, f"📣 Broadcasting to {total} users…")

    async def worker():
        while True:
            try:
                uid = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            for _ in range(3):
                try:
                    await bot.copy_message(uid, src_chat, src_msg)
                    counts["ok"] += 1
                    break
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except Forbidden:
                    blocked_ids.append(uid)
                    counts["blocked"] += 1
                    break
                except TelegramError:
                    counts["fail"] += 1
                    break
            else:
                counts["fail"] += 1
            await asyncio.sleep(0.7)  # ~25 msgs/sec across 20 workers, under Telegram's limit

    async def progress():
        while True:
            await asyncio.sleep(5)
            try:
                await status.edit_text(f"📣 Broadcasting… {sum(counts.values())}/{total}")
            except TelegramError:
                pass

    prog = asyncio.create_task(progress())
    await asyncio.gather(*(worker() for _ in range(BROADCAST_WORKERS)))
    prog.cancel()
    if blocked_ids:
        await db.users.update_many({"_id": {"$in": blocked_ids}}, {"$set": {"blocked": True}})
    try:
        await status.edit_text(
            f"✅ Broadcast finished\nSent: {counts['ok']}\nBlocked the bot: {counts['blocked']}\nFailed: {counts['fail']}"
        )
    except TelegramError:
        pass


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    src = update.message.reply_to_message
    if not src:
        await update.message.reply_text("Reply to the message you want to broadcast with /broadcast")
        return
    context.application.create_task(
        do_broadcast(context.bot, src.chat_id, src.message_id, update.effective_chat.id)
    )


async def _set_template(update, key, default, cmd):
    msg = update.message
    if msg.reply_to_message and msg.reply_to_message.text:
        text = msg.reply_to_message.text
    else:
        parts = msg.text.split(None, 1)
        text = parts[1].strip() if len(parts) > 1 else ""
    if not text:
        await msg.reply_text(
            f"Current message:\n\n{await get_message(key, default)}\n\n"
            f"To change it: /{cmd} your new text\n"
            f"(or reply to a message with /{cmd})\n"
            "Placeholders: {hours} = delete time, {contact} = your contact (/setcontact)\n"
            f"/reset{cmd[3:]} restores the default."
        )
        return
    await db.settings.update_one({"_id": key}, {"$set": {"text": text}}, upsert=True)
    await msg.reply_text("✅ Updated. Preview:\n\n" + await get_message(key, default))


async def _reset_template(update, key):
    await db.settings.delete_one({"_id": key})
    await update.message.reply_text("✅ Reset to default.")


async def setnoticemsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _set_template(update, "notice_message", DEFAULT_NOTICE_MESSAGE, "setnoticemsg")


async def resetnoticemsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _reset_template(update, "notice_message")


async def setdeletemsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _set_template(update, "delete_message", DEFAULT_DELETE_MESSAGE, "setdeletemsg")


async def resetdeletemsg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _reset_template(update, "delete_message")


async def setcontact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        doc = await db.settings.find_one({"_id": "contact"})
        await update.message.reply_text(
            f"Current contact: {doc['value'] if doc else 'not set'}\n"
            "Set it: /setcontact @username\nRemove it: /setcontact off"
        )
        return
    value = context.args[0].strip()
    if value.lower() in ("off", "none", "remove"):
        await db.settings.delete_one({"_id": "contact"})
        await update.message.reply_text("✅ Contact removed. The permanent-access line is hidden.")
        return
    if not value.startswith(("@", "http")):
        value = "@" + value
    await db.settings.update_one({"_id": "contact"}, {"$set": {"value": value}}, upsert=True)
    await update.message.reply_text(f"✅ Contact set to {value}. Preview:\n\n" + await get_delete_message())


# ---------- admin: manage / delete courses & chapters ----------
def btn(text, data):
    return InlineKeyboardButton(text, callback_data=data)


async def manage_home_view():
    courses = await db.courses.find().sort("name_lower", 1).to_list(200)
    if not courses:
        return "No courses yet. Use /addbatch to add one.", None
    rows = [[btn(c["name"], f"m:c:{c['_id']}")] for c in courses]
    return "🛠 Manage – choose a course:", InlineKeyboardMarkup(rows)


async def manage_course_view(course_id):
    course = await db.courses.find_one({"_id": course_id})
    if not course:
        return await manage_home_view()
    chapters = await db.chapters.find({"course_id": course_id}).sort("created_at", 1).to_list(300)
    lines = [f"🛠 {course['name']}", "Tap a chapter to delete it:"]
    rows = [
        [btn(f"🗑 {c['name']} ({c['first_id']}–{c['last_id']})", f"m:d:{c['_id']}")] for c in chapters
    ]
    if not chapters:
        lines[1] = "No chapters in this course."
    rows.append([btn("🗑 Delete whole course", f"m:dc:{course_id}")])
    rows.append([btn("⬅️ Back", "m:home:")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def manage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text, markup = await manage_home_view()
    await update.message.reply_text(text, reply_markup=markup)


async def on_manage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id not in ADMIN_IDS:
        await q.answer("Admins only.", show_alert=True)
        return
    await q.answer()
    _, action, value = q.data.split(":", 2)
    try:
        oid = ObjectId(value) if value else None
    except InvalidId:
        return

    if action == "home":
        text, markup = await manage_home_view()
    elif action == "c":
        text, markup = await manage_course_view(oid)
    elif action == "d":
        ch = await db.chapters.find_one({"_id": oid})
        if not ch:
            text, markup = await manage_home_view()
        else:
            text = (
                f"Delete chapter “{ch['name']}”?\n"
                "This only removes it from the bot. The videos stay in your private channel."
            )
            markup = InlineKeyboardMarkup(
                [[btn("✅ Yes, delete", f"m:yd:{oid}"), btn("❌ No", f"m:c:{ch['course_id']}")]]
            )
    elif action == "yd":
        ch = await db.chapters.find_one_and_delete({"_id": oid})
        text, markup = await manage_course_view(ch["course_id"]) if ch else await manage_home_view()
    elif action == "dc":
        course = await db.courses.find_one({"_id": oid})
        if not course:
            text, markup = await manage_home_view()
        else:
            n = await db.chapters.count_documents({"course_id": oid})
            text = f"Delete course “{course['name']}” and its {n} chapter(s)?\nThe videos stay in your private channel."
            markup = InlineKeyboardMarkup(
                [[btn("✅ Yes, delete", f"m:ydc:{oid}"), btn("❌ No", f"m:c:{oid}")]]
            )
    elif action == "ydc":
        await db.chapters.delete_many({"course_id": oid})
        await db.courses.delete_one({"_id": oid})
        text, markup = await manage_home_view()
    else:
        return
    await q.edit_message_text(text, reply_markup=markup)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week = now - timedelta(days=7)
    (total, blocked, new_today, new_week, courses, chapters, pending, opens, opens_today, top) = await asyncio.gather(
        db.users.count_documents({}),
        db.users.count_documents({"blocked": True}),
        db.users.count_documents({"joined": {"$gte": today}}),
        db.users.count_documents({"joined": {"$gte": week}}),
        db.courses.count_documents({}),
        db.chapters.count_documents({}),
        db.scheduled_deletes.count_documents({}),
        db.downloads.count_documents({}),
        db.downloads.count_documents({"at": {"$gte": today}}),
        db.downloads.aggregate(
            [{"$group": {"_id": "$label", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}, {"$limit": 5}]
        ).to_list(5),
    )
    fsub = len(await get_fsub())
    top_text = "\n".join(f"  {i}. {t['_id']} – {t['n']}" for i, t in enumerate(top, 1)) or "  –"
    await update.message.reply_text(
        "📊 Bot statistics\n\n"
        f"👥 Users: {total} (active {total - blocked}, blocked {blocked})\n"
        f"🆕 New today: {new_today} | last 7 days: {new_week}\n\n"
        f"📚 Courses: {courses} | 📖 Chapters: {chapters}\n"
        f"📥 Lecture opens: {opens} (today {opens_today})\n"
        f"🔥 Top chapters:\n{top_text}\n\n"
        f"🔒 Force-sub channels: {fsub}\n"
        f"🗑 Pending deletions: {pending}"
    )


async def fsub_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channels = await get_fsub()
    if not channels:
        await update.message.reply_text("No force-subscribe channels.\nAdd one: /addfsub <channel id or @username>")
        return
    lines = [f"• {c['title']} ({c['chat_id']})" for c in channels]
    await update.message.reply_text("🔒 Force-subscribe channels:\n" + "\n".join(lines) + "\n\nRemove: /delfsub <channel id>")


async def addfsub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /addfsub <channel id or @username>\nThe bot must be an admin in that channel.")
        return
    arg = context.args[0]
    target = int(arg) if arg.lstrip("-").isdigit() else arg
    try:
        chat = await context.bot.get_chat(target)
        await context.bot.get_chat_member(chat.id, update.effective_user.id)  # proves the bot can check members
    except TelegramError as e:
        await update.message.reply_text(f"❌ Couldn't use that channel: {e}\nAdd the bot as an admin there first.")
        return
    if chat.username:
        link = f"https://t.me/{chat.username}"
    else:
        try:
            link = (await context.bot.create_chat_invite_link(chat.id)).invite_link
        except TelegramError:
            await update.message.reply_text("❌ Give the bot the “Invite users via link” admin right in that channel.")
            return
    channels = [c for c in await get_fsub() if c["chat_id"] != chat.id]
    channels.append({"chat_id": chat.id, "title": chat.title or str(chat.id), "link": link})
    await db.settings.update_one({"_id": "force_sub"}, {"$set": {"channels": channels}}, upsert=True)
    await update.message.reply_text(f"✅ Force-subscribe enabled for {chat.title}")


async def delfsub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].lstrip("-").isdigit():
        await update.message.reply_text("Usage: /delfsub <channel id>  (see /fsub)")
        return
    cid = int(context.args[0])
    channels = await get_fsub()
    kept = [c for c in channels if c["chat_id"] != cid]
    await db.settings.update_one({"_id": "force_sub"}, {"$set": {"channels": kept}}, upsert=True)
    await update.message.reply_text("✅ Removed." if len(kept) != len(channels) else "That channel wasn't in the list.")


async def post_init(app: Application):
    await db.scheduled_deletes.create_index("delete_at")
    await db.chapters.create_index("course_id")
    await db.courses.create_index("name_lower", unique=True)
    await db.downloads.create_index("at")


def main():
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connection_pool_size(64)
        .pool_timeout(10)
        .post_init(post_init)
        .build()
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler("addbatch", addbatch, filters=admin_only)],
        states={
            COURSE: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_only, got_course)],
            CHAPTER: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_only, got_chapter)],
            FIRST_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_only, got_first)],
            LAST_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND & admin_only, got_last)],
        },
        fallbacks=[CommandHandler("cancel", cancel, filters=admin_only)],
    )
    app.add_handler(conv)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("broadcast", broadcast, filters=admin_only))
    app.add_handler(CommandHandler("stats", stats, filters=admin_only))
    app.add_handler(CommandHandler("fsub", fsub_list, filters=admin_only))
    app.add_handler(CommandHandler("addfsub", addfsub, filters=admin_only))
    app.add_handler(CommandHandler("delfsub", delfsub, filters=admin_only))
    app.add_handler(CommandHandler("setnoticemsg", setnoticemsg, filters=admin_only))
    app.add_handler(CommandHandler("resetnoticemsg", resetnoticemsg, filters=admin_only))
    app.add_handler(CommandHandler("setcontact", setcontact, filters=admin_only))
    app.add_handler(CommandHandler("setdeletemsg", setdeletemsg, filters=admin_only))
    app.add_handler(CommandHandler("resetdeletemsg", resetdeletemsg, filters=admin_only))
    app.add_handler(CommandHandler("manage", manage, filters=admin_only))
    app.add_handler(CallbackQueryHandler(on_manage, pattern=r"^m:"))
    app.add_handler(CallbackQueryHandler(on_button))

    app.job_queue.run_repeating(cleanup, interval=60, first=5)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
