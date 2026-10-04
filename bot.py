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

db = AsyncIOMotorClient(MONGO_URI)[DB_NAME]
admin_only = filters.User(user_id=ADMIN_IDS)

COURSE, CHAPTER, FIRST_ID, LAST_ID = range(4)


def utcnow():
    return datetime.now(timezone.utc)


def chunks(seq, n=100):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def pretty_hours():
    h = AUTO_DELETE_HOURS
    return f"{int(h)} hour(s)" if h == int(h) else f"{h} hours"


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
    markup = await courses_markup()
    if not markup:
        await update.message.reply_text("No courses added yet. Please check back soon.")
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
    await q.answer()
    action, _, value = q.data.partition(":")

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
        notice = await context.bot.send_message(
            chat_id,
            f"⏳ These lectures will be auto-deleted in {pretty_hours()}.\n"
            "Forward them to Saved Messages if you want to keep them.",
        )
        # Stored in MongoDB so the deletion survives Heroku dyno restarts.
        await db.scheduled_deletes.insert_one(
            {
                "chat_id": chat_id,
                "message_ids": sent + [notice.message_id],
                "delete_at": utcnow() + timedelta(hours=AUTO_DELETE_HOURS),
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
    ok = fail = 0
    async for u in db.users.find({"blocked": {"$ne": True}}):
        for attempt in range(2):
            try:
                await bot.copy_message(u["_id"], src_chat, src_msg)
                ok += 1
                break
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except Forbidden:
                await db.users.update_one({"_id": u["_id"]}, {"$set": {"blocked": True}})
                fail += 1
                break
            except TelegramError:
                fail += 1
                break
        await asyncio.sleep(0.05)
    await bot.send_message(admin_chat, f"📣 Broadcast done. Sent: {ok}, failed: {fail}")


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    src = update.message.reply_to_message
    if not src:
        await update.message.reply_text("Reply to the message you want to broadcast with /broadcast")
        return
    await update.message.reply_text("📣 Broadcast started…")
    context.application.create_task(
        do_broadcast(context.bot, src.chat_id, src.message_id, update.effective_chat.id)
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = await db.users.count_documents({})
    courses = await db.courses.count_documents({})
    chapters = await db.chapters.count_documents({})
    pending = await db.scheduled_deletes.count_documents({})
    await update.message.reply_text(
        f"👥 Users: {users}\n📚 Courses: {courses}\n📖 Chapters: {chapters}\n🗑 Pending deletions: {pending}"
    )


async def post_init(app: Application):
    await db.scheduled_deletes.create_index("delete_at")
    await db.chapters.create_index("course_id")
    await db.courses.create_index("name_lower", unique=True)


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

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
    app.add_handler(CallbackQueryHandler(on_button))

    app.job_queue.run_repeating(cleanup, interval=60, first=5)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
