# === Импорты и конфигурация ===
import os
import json
import asyncio
from datetime import datetime, timedelta

import pgeocode
import requests
import pandas as pd

import firebase_admin
from firebase_admin import credentials, firestore

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, CallbackQueryHandler, filters
)
from dotenv import load_dotenv
edit_state = {}  # user_id: {doc_id, data, field, message_ids}

# === Загрузка переменных окружения ===
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT_JSON"))
cred = credentials.Certificate(service_account_info)
firebase_admin.initialize_app(cred)
db = firestore.client()

# === Константы ===
PICKUP, DELIVERY, TOTAL_MILES, RATE, TRAILER, COMMENT = range(6)
STATS_SELECT, MY_STATS_DAY = range(6, 8)

nomi = pgeocode.Nominatim("us")
submit_step_texts = [
    "📍 *Step 1/6* — Enter pickup ZIP or State abbreviation (e.g., CA):",
    "📍 *Step 2/6* — Enter delivery ZIP or State abbreviation:",
    "📏 *Step 3/6* — Enter total miles:",
    "💵 *Step 4/6* — Enter total rate ($):",
    "🚛 *Step 5/6* — Choose trailer type:",
    "💬 *Step 6/6* — Add comment (or press 'Skip')"
]
submit_states = ["pickup_zip", "delivery_zip", "total_miles", "rate", "trailer", "comment"]
submit_current_messages = {}

# === Утилиты ===
def resolve_location(value):
    print("[DEBUG] Resolving:", value)  # <--- добавь
    if len(value) == 2 and value.isalpha():
        return ("", value)
    info = nomi.query_postal_code(value)
    print("[DEBUG] Result:", info)
    return (info.place_name or "", info.state_code or value)

def classify_distance(miles):
    if miles < 500:
        return "Short"
    elif 500 <= miles <= 1000:
        return "Medium"
    return "Long"

def generate_stats_message(period_label, df):
    lines = [f"📊 Load Stats — {period_label}\n"]
    avg_by_trailer = df.groupby("Trailer")["RPM Total"].mean().round(2)
    lines.append("🚛 Average RPM by Trailer Type:")
    for trailer, avg in avg_by_trailer.items():
        lines.append(f"• {trailer}: {avg:.2f}")
    lines.append("\n📏 RPM by Load Length & Trailer:")
    for category in ["Short", "Medium", "Long"]:
        cat_df = df[df["Length Category"] == category]
        lines.append(f"{category} Loads:")
        for trailer, avg in cat_df.groupby("Trailer")["RPM Total"].mean().round(2).items():
            lines.append(f"  • {trailer}: {avg:.2f}")
        lines.append("")
    return "\n".join(lines)

# === SUBMIT FLOW (Firestore) ===
async def submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data["submit_step"] = 0
    context.user_data["user_id"] = str(update.effective_user.id)
    context.user_data["username"] = f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.full_name
    await send_submit_step(update.effective_chat.id, context)
    return PICKUP

async def send_submit_step(chat_id, context):
    step = context.user_data.get("submit_step", 0)
    text = submit_step_texts[step]
    buttons = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]

    if step == 4:
        trailer_row1 = [
            InlineKeyboardButton("Dry Van", callback_data="Dry Van"),
            InlineKeyboardButton("Reefer", callback_data="Reefer"),
            InlineKeyboardButton("Flatbed", callback_data="Flatbed")
        ]
        trailer_row2 = [
            InlineKeyboardButton("Power Only", callback_data="Power Only"),
            InlineKeyboardButton("Step Deck", callback_data="Step Deck"),
            InlineKeyboardButton("Conestoga", callback_data="Conestoga")
        ]
        trailer_row3 = [InlineKeyboardButton("Other", callback_data="Other")]
        buttons = [trailer_row1, trailer_row2, trailer_row3] + buttons
    elif step == 5:
        buttons[0].insert(0, InlineKeyboardButton("➡️ Skip", callback_data="skip"))

    msg = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown")
    submit_current_messages[chat_id] = msg.message_id

async def handle_submit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_input = update.message.text.strip()
    context.user_data["last_user_message_id"] = update.message.message_id
    step = context.user_data.get("submit_step", 0)
    field = submit_states[step]
    context.user_data[field] = user_input

    if chat_id in submit_current_messages:
        await context.bot.delete_message(chat_id, submit_current_messages[chat_id])
    if "last_user_message_id" in context.user_data:
        try:
            await context.bot.delete_message(chat_id, context.user_data["last_user_message_id"])
        except:
            pass

    step += 1
    if step < len(submit_states):
        context.user_data["submit_step"] = step
        await send_submit_step(chat_id, context)
        return step
    else:
        await finalize_submission(update, context)
        return ConversationHandler.END

async def handle_submit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    step = context.user_data.get("submit_step", 0)
    field = submit_states[step]

    await query.answer()
    await context.bot.delete_message(chat_id, query.message.message_id)

    if query.data == "cancel":
        msg = await context.bot.send_message(chat_id=chat_id, text="❌ Submission canceled.")
        await asyncio.sleep(5)
        await context.bot.delete_message(chat_id, msg.message_id)
        context.user_data.clear()
        return ConversationHandler.END
    if query.data == "skip" and field == "comment":
        context.user_data[field] = ""
    else:
        context.user_data[field] = query.data

    step += 1
    if step < len(submit_states):
        context.user_data["submit_step"] = step
        await send_submit_step(chat_id, context)
        return step
    else:
        await finalize_submission(update, context)
        return ConversationHandler.END

async def finalize_submission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    date = datetime.now().strftime("%Y-%m-%d")
    pickup_city, pickup_state = resolve_location(data["pickup_zip"])
    delivery_city, delivery_state = resolve_location(data["delivery_zip"])
    pickup = f"{pickup_city}, {pickup_state}" if pickup_city else pickup_state
    delivery = f"{delivery_city}, {delivery_state}" if delivery_city else delivery_state

    try:
        total = float(str(data["total_miles"]).replace("$", "").replace(",", ""))
        rate = float(str(data["rate"]).replace("$", "").replace(",", ""))
    except:
        msg = await update.effective_message.reply_text("❌ Invalid number format.")
        await asyncio.sleep(5)
        await context.bot.delete_message(update.effective_chat.id, msg.message_id)
        return

    rpm_total = round(rate / total, 2) if total else 0

    doc = {
        "Date": date,
        "Pickup ZIP": data["pickup_zip"],
        "Delivery ZIP": data["delivery_zip"],
        "Total Miles": total,
        "Rate": rate,
        "RPM Total": rpm_total,
        "Trailer": data["trailer"],
        "User": data["username"],
        "Comment": data.get("comment", ""),
        "Posted By": data["username"],
        "User ID": data["user_id"]
    }

    db.collection("loads").add(doc)

    text = (
        f"🗓 {date}\n"
        f"🧑‍✈️ Posted by: {data['username']}\n"
        f"📍 {pickup} → {delivery}\n"
        f"📏 Miles: {int(total)}\n"
        f"💵 Rate: ${int(rate)} | RPM: Total — {rpm_total:.2f}\n"
        f"🚛 Trailer: {data['trailer']}\n"
        f"💬 Comment: {data['comment'] or '—'}"
    )

    await context.bot.send_message(chat_id="@rateguard", text=text)
    await context.bot.send_message(chat_id="-1002235875053", text=text)
    msg = await update.effective_message.reply_text("✅ Load submitted and published!")
    await asyncio.sleep(5)
    await context.bot.delete_message(update.effective_chat.id, msg.message_id)

async def load_data_from_firestore():
    docs = db.collection("loads").stream()
    records = []
    for doc in docs:
        item = doc.to_dict()
        print("[DEBUG] LOADED:", item)  # <--- ВСТАВЬ СЮДА
        try:
            item["Date"] = datetime.strptime(item["Date"], "%Y-%m-%d").date()
        except:
            item["Date"] = None
        item["Total Miles"] = float(item.get("Total Miles", 0))
        item["Rate"] = float(item.get("Rate", 0))
        item["RPM Total"] = float(item.get("RPM Total", 0))
        item["Length Category"] = classify_distance(item["Total Miles"])
        records.append(item)

    print(f"[DEBUG] Total records: {len(records)}")  # <--- И СЮДА
    return pd.DataFrame(records)
async def stats_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Today", callback_data="today"),
         InlineKeyboardButton("This Week", callback_data="this_week"),
         InlineKeyboardButton("This Month", callback_data="this_month")]
    ]
    await update.message.reply_text("📊 Choose stats period:", reply_markup=InlineKeyboardMarkup(keyboard))
    return STATS_SELECT

async def handle_stats_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_reply_markup(reply_markup=None)

    text = query.data
    now = datetime.now()

    if text == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        label = "Today"
    elif text == "this_week":
        start = now - timedelta(days=now.weekday())
        label = "This Week"
    elif text == "this_month":
        start = now.replace(day=1)
        label = "This Month"
    else:
        await query.edit_message_text("❌ Invalid selection.")
        return ConversationHandler.END

    df = await load_data_from_firestore()
    df = df[df['Date'] >= start.date()]

    msg = generate_stats_message(label, df)
    await query.edit_message_text(msg)
    return ConversationHandler.END

# === My Stats ===
async def my_stats_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[
        InlineKeyboardButton(day, callback_data=day.lower())
        for day in ["Monday", "Tuesday", "Wednesday", "Thursday"]
    ], [
        InlineKeyboardButton(day, callback_data=day.lower())
        for day in ["Friday", "Saturday", "Sunday"]
    ]]
    await update.message.reply_text("📆 Choose start of your week:", reply_markup=InlineKeyboardMarkup(keyboard))
    return MY_STATS_DAY

async def handle_my_day_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_reply_markup(reply_markup=None)

    text = query.data
    weekdays = {
        "monday": 0, "tuesday": 1, "wednesday": 2,
        "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6
    }

    if text not in weekdays:
        await query.edit_message_text("❌ Invalid day.")
        return ConversationHandler.END

    day_num = weekdays[text]
    now = datetime.now()
    today_num = now.weekday()
    days_since_start = (today_num - day_num) % 7

    if days_since_start == 0:
        start = now
    else:
        start = now - timedelta(days=days_since_start)

    end = now
    start = start.date()
    end = end.date()

    df = await load_data_from_firestore()
    user_id = str(update.effective_user.id)
    df = df[df["User ID"] == user_id]
    df = df[(df['Date'] >= start) & (df['Date'] <= end)]

    date_range = f"{start.strftime('%b %d')} to {end.strftime('%b %d')}"
    label = f"My Stats (from {text.title()}) — {date_range}"

    if df.empty:
        await query.edit_message_text(f"📊 {label}\nNo loads found for this period.")
    else:
        total_loads = len(df)
        total_miles = int(df['Total Miles'].sum())
        total_rate = int(df['Rate'].sum())
        avg_rpm = round(df['Rate'].sum() / df['Total Miles'].sum(), 2) if df['Total Miles'].sum() else "—"

        msg = (
            f"📊 {label}\n"
            f"📦 Total Loads: {total_loads}\n"
            f"📏 Total Miles: {total_miles}\n"
            f"💰 Total Rate: ${total_rate}\n"
            f"📈 Average RPM: {avg_rpm}"
        )
        await query.edit_message_text(msg)

    return ConversationHandler.END

async def my_loads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    docs = db.collection("loads").where("User ID", "==", user_id).order_by("Date", direction=firestore.Query.DESCENDING).limit(5).stream()
    messages = []

    for doc in docs:
        data = doc.to_dict()
        load_id = doc.id
        text = (
            f"🗓 {data.get('Date')}\n"
            f"📍 {data.get('Pickup ZIP')} → {data.get('Delivery ZIP')}\n"
            f"📏 Miles: {data.get('Total Miles')}\n"
            f"💵 Rate: ${data.get('Rate')} | RPM: {data.get('RPM Total')}\n"
            f"🚛 Trailer: {data.get('Trailer')}\n"
            f"💬 Comment: {data.get('Comment', '—')}"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Edit", callback_data=f"edit_{load_id}")]
        ])
        msg = await update.message.reply_text(text, reply_markup=keyboard)
        messages.append(msg.message_id)

    context.user_data["my_load_messages"] = messages

async def start_edit_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    load_id = query.data.split("_", 1)[1]

    doc_ref = db.collection("loads").document(load_id)
    doc = doc_ref.get()
    if not doc.exists:
        await query.message.reply_text("❌ Load not found.")
        return

    edit_state[str(update.effective_user.id)] = {
        "doc_id": load_id,
        "data": doc.to_dict()
    }

    await show_edit_menu(update, context)

async def show_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    row = edit_state[user_id]["data"]
    text = (
        f"🛠 *Edit Load — {row['Date']}*\n"
        f"📍 {row['Pickup ZIP']} → {row['Delivery ZIP']}\n"
        f"🚛 {row['Trailer']} | 💵 ${row['Rate']} | {row['Total Miles']} mi\n\n"
        f"Choose field to edit:"
    )
    buttons = [
        [InlineKeyboardButton("📍 Pickup ZIP", callback_data="editfield_pickup")],
        [InlineKeyboardButton("📍 Delivery ZIP", callback_data="editfield_delivery")],
        [InlineKeyboardButton("📏 Total Miles", callback_data="editfield_miles")],
        [InlineKeyboardButton("💵 Rate", callback_data="editfield_rate")],
        [InlineKeyboardButton("🚛 Trailer", callback_data="editfield_trailer")],
        [InlineKeyboardButton("💬 Comment", callback_data="editfield_comment")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_edit")]
    ]
    await update.callback_query.edit_message_text(
        text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown"
    )

async def handle_edit_field_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    field = query.data.split("_")[1]
    user_id = str(update.effective_user.id)
    edit_state[user_id]["field"] = field
    msg = await query.message.reply_text(f"✏️ Enter new value for *{field}*:", parse_mode="Markdown")
    edit_state[user_id]["msg_id"] = msg.message_id

async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in edit_state:
        return

    value = update.message.text.strip()
    field = edit_state[user_id]["field"]
    doc_id = edit_state[user_id]["doc_id"]
    doc_ref = db.collection("loads").document(doc_id)

    update_fields = {}

    if field == "miles":
        update_fields["Total Miles"] = float(value)
    elif field == "rate":
        update_fields["Rate"] = float(value)
    elif field == "pickup":
        update_fields["Pickup ZIP"] = value
    elif field == "delivery":
        update_fields["Delivery ZIP"] = value
    elif field == "trailer":
        update_fields["Trailer"] = value
    elif field == "comment":
        update_fields["Comment"] = value

    # Автоматический перерасчет RPM
    if "Rate" in update_fields or "Total Miles" in update_fields:
        existing = edit_state[user_id]["data"]
        rate = update_fields.get("Rate", existing.get("Rate", 0))
        miles = update_fields.get("Total Miles", existing.get("Total Miles", 0))
        update_fields["RPM Total"] = round(float(rate) / float(miles), 2) if miles else 0

    doc_ref.update(update_fields)

    try:
        await context.bot.delete_message(update.effective_chat.id, update.message.message_id)
        await context.bot.delete_message(update.effective_chat.id, edit_state[user_id]["msg_id"])
    except:
        pass

    msg = await update.message.reply_text("✅ Value updated.")
    await asyncio.sleep(3)
    await context.bot.delete_message(update.effective_chat.id, msg.message_id)

    del edit_state[user_id]

# === ConversationHandler ===
if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    submit_conv = ConversationHandler(
        entry_points=[CommandHandler("submit", submit)],
        states={
            PICKUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input), CallbackQueryHandler(handle_submit_callback)],
            DELIVERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input), CallbackQueryHandler(handle_submit_callback)],
            TOTAL_MILES: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input), CallbackQueryHandler(handle_submit_callback)],
            RATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input), CallbackQueryHandler(handle_submit_callback)],
            TRAILER: [CallbackQueryHandler(handle_submit_callback)],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input), CallbackQueryHandler(handle_submit_callback)]
        },
        fallbacks=[],
        per_chat=True
    )
    stats_conv = ConversationHandler(
        entry_points=[CommandHandler("stats", stats_start)],
        states={STATS_SELECT: [CallbackQueryHandler(handle_stats_selection)]},
        fallbacks=[],
        per_chat=True
    )
    app.add_handler(stats_conv)

    my_stats_conv = ConversationHandler(
        entry_points=[CommandHandler("my_stats", my_stats_start)],
        states={MY_STATS_DAY: [CallbackQueryHandler(handle_my_day_selection)]},
        fallbacks=[],
        per_chat=True
    )

    app.add_handler(my_stats_conv)
    app.add_handler(CommandHandler("my_loads", my_loads))
    app.add_handler(CallbackQueryHandler(start_edit_load, pattern="^edit_"))
    app.add_handler(CallbackQueryHandler(handle_edit_field_selection, pattern="^editfield_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_input))
    app.add_handler(submit_conv)
    app.run_polling()
