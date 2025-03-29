import os
import pandas as pd
import gspread
import pgeocode
import requests
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes,
    ConversationHandler, MessageHandler, CallbackQueryHandler, filters
)
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv


load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

FMCSA_API_KEY = "91a883766f99d16ed141dd4a254158a898fba793"

PICKUP, DELIVERY, TOTAL_MILES, RATE, TRAILER, COMMENT, CANCEL = range(7)
STATS_SELECT, MY_STATS_DAY = range(6, 8)

nomi = pgeocode.Nominatim('us')
user_stats_state = {}

def get_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    import json
    creds_json = os.getenv("SERVICE_ACCOUNT_JSON")
    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open("RateGuard_Leads").sheet1

def classify_distance(miles):
    if miles < 500:
        return "Short"
    elif 500 <= miles <= 1000:
        return "Medium"
    return "Long"

def load_data():
    sheet = get_sheet()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
    df['Total Miles'] = pd.to_numeric(df['Total Miles'], errors='coerce')
    df['RPM Total'] = pd.to_numeric(df['RPM Total'].astype(str).str.replace(',', '.'), errors='coerce')
    df['Length Category'] = df['Total Miles'].apply(classify_distance)
    return df

def generate_stats_message(period_label, df):
    lines = [f"\U0001F4CA Load Stats — {period_label}\n"]
    avg_by_trailer = df.groupby("Trailer")["RPM Total"].mean().round(2)
    lines.append("\U0001F69A Average RPM by Trailer Type:")
    for trailer, avg in avg_by_trailer.items():
        avg_display = f"{avg:.2f}" if not pd.isna(avg) else "—"
        lines.append(f"• {trailer}: Total — {avg_display}")

    lines.append("\n\U0001F4DD RPM by Load Length & Trailer Type:")
    lines.append("Length categories:\n• Short < 500 mi\n• Medium = 500 to 1000 mi\n• Long > 1000 mi\n")

    for category in ["Short", "Medium", "Long"]:
        lines.append(f"{category} Loads:")
        cat_df = df[df["Length Category"] == category]
        for trailer, avg in cat_df.groupby("Trailer")["RPM Total"].mean().round(2).items():
            avg_display = f"{avg:.2f}" if not pd.isna(avg) else "—"
            lines.append(f"  • {trailer}: Total — {avg_display}")
        lines.append("")

    return "\n".join(lines)

def generate_my_stats_message(label, df):
    total_loads = len(df)
    total_miles = int(df['Total Miles'].sum())
    total_rate = int(df['Rate'].sum())
    avg_rpm = round(df['RPM Total'].mean(), 2) if not df['RPM Total'].isna().all() else "—"

    return (
        f"📊 {label}\n"
        f"📦 Total Loads: {total_loads}\n"
        f"📏 Total Miles: {total_miles}\n"
        f"💰 Total Rate: ${total_rate}\n"
        f"📈 Average RPM: {avg_rpm}"
    )

# === /submit ===

# === Новый /submit с единым сообщением и кнопкой Cancel ===

async def submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data["msg"] = await update.message.reply_text(
        "📍 Enter pickup ZIP or State abbreviation:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])
    )
    return PICKUP

async def cancel_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    msg = context.user_data.get("msg")
    if msg:
        await msg.edit_text("❌ Submission cancelled.", reply_markup=None)
    context.user_data.clear()
    return ConversationHandler.END

async def pickup_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["pickup_zip"] = update.message.text.strip().upper()
    await context.user_data["msg"].edit_text(
        "🚩 Enter delivery ZIP or State abbreviation:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])
    )
    return DELIVERY

async def delivery_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["delivery_zip"] = update.message.text.strip().upper()
    await context.user_data["msg"].edit_text(
        "📏 Enter total miles:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])
    )
    return TOTAL_MILES

async def miles_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["total_miles"] = update.message.text.strip()
    await context.user_data["msg"].edit_text(
        "💵 Enter total rate (numbers only):",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])
    )
    return RATE

async def rate_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rate = update.message.text.strip().replace("$", "").replace(",", "")
    if not rate.replace('.', '').isdigit():
        await update.message.reply_text("❗ Please enter a valid number (e.g., 1250)")
        return RATE

    context.user_data["rate"] = rate
    keyboard = [
        [InlineKeyboardButton("Dry Van", callback_data="tr_Dry Van"),
         InlineKeyboardButton("Reefer", callback_data="tr_Reefer")],
        [InlineKeyboardButton("Flatbed", callback_data="tr_Flatbed"),
         InlineKeyboardButton("Power Only", callback_data="tr_Power Only")],
        [InlineKeyboardButton("Step Deck", callback_data="tr_Step Deck"),
         InlineKeyboardButton("Other", callback_data="tr_Other")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
    ]
    await context.user_data["msg"].edit_text("🚛 Choose trailer type:", reply_markup=InlineKeyboardMarkup(keyboard))
    return TRAILER

async def trailer_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    trailer = update.callback_query.data.replace("tr_", "")
    context.user_data["trailer"] = trailer
    await update.callback_query.edit_message_text(
        "💬 Enter comment or type 'skip':",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]])
    )
    return COMMENT

async def comment_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text.strip()
    context.user_data["comment"] = "" if comment.lower() == "skip" else comment

    user = update.effective_user
    context.user_data["username"] = f"@{user.username}" if user.username else user.full_name
    context.user_data["user_id"] = str(user.id)

    await save_to_sheet(context)

    await context.user_data["msg"].edit_text("✅ Load submitted and published!")
    context.user_data.clear()
    return ConversationHandler.END

async def save_to_sheet(context):
    sheet = get_sheet()
    data = context.user_data
    date = datetime.now().strftime("%Y-%m-%d")

    def resolve_location(value):
        if len(value) == 2 and value.isalpha():
            return ("", value)
        info = nomi.query_postal_code(value)
        return (info.place_name or "", info.state_code or value)

    pickup_city, pickup_state = resolve_location(data["pickup_zip"])
    delivery_city, delivery_state = resolve_location(data["delivery_zip"])
    pickup = f"{pickup_city}, {pickup_state}" if pickup_city else pickup_state
    delivery = f"{delivery_city}, {delivery_state}" if delivery_city else delivery_state

    total = float(str(data["total_miles"]).replace("$", "").replace(",", "").strip())
    rate = float(str(data["rate"]).replace("$", "").replace(",", "").strip())
    rpm_total = format(rate / total, '.2f') if total else ""

    sheet.append_row([
        date,                   # A - Date
        data["pickup_zip"],    # B - Pickup ZIP
        data["delivery_zip"],  # C - Delivery ZIP
        "",                    # D - Loaded Miles
        "",                    # E - Empty Miles
        total,                 # F - Total Miles
        rate,                  # G - Rate
        "",                    # H - RPM Loaded
        rpm_total,             # I - RPM Total
        data["trailer"],       # J - Trailer
        data["username"],      # K - User
        "",                    # L - Broker
        data["comment"],       # M - Comment
        data["username"],      # N - Posted By
        data["user_id"]        # O - User ID
    ])

    message = (
        f"🗓 {date}\n"
        f"🧑‍✈️ Posted by: {data['username']}\n"
        f"📍 {pickup} → {delivery}\n"
        f"📏 Miles: {int(total)}\n"
        f"💵 Rate: ${int(rate)} | RPM: Total — {rpm_total}\n"
        f"🚛 Trailer: {data['trailer']}\n"
        f"💬 Comment: {data['comment'] or '—'}"
    )

    await context.bot.send_message(chat_id="@rateguard", text=message)
    await context.bot.send_message(chat_id="-1002235875053", text=message)
# === /stats ===

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

    # Вот это — ключевое исправление:
    df = load_data()
    df = df[df['Date'] >= start.date()]

    msg = generate_stats_message(label, df)
    await query.edit_message_text(msg)
    return ConversationHandler.END
# === /my_stats ===

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
    end = now  # текущая дата, можно заменить на start + timedelta(days=6) если нужна неделя

    start = start.date()
    end = end.date()

    df = load_data()
    user_id = str(update.effective_user.id)

    if "User ID" in df.columns:
        df = df[df["User ID"].astype(str) == user_id]
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]
    else:
        await query.edit_message_text("⚠️ Your user ID was not found in any entries. Please re-submit your load to enable stats tracking.")
        return ConversationHandler.END

    date_range = f"{start.strftime('%b %d')} to {end.strftime('%b %d')}"
    label = f"My Stats (from {text.title()}) — {date_range}"

    if df.empty:
        await query.edit_message_text(f"📊 {label}\nNo loads found for this period.")
    else:
        msg = generate_my_stats_message(label, df)
        await query.edit_message_text(msg)

    return ConversationHandler.END



async def broker_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❗ Please provide MC or DOT number. Example: /broker 123456")
        return

    raw_number = context.args[0]
    number = ''.join(filter(str.isdigit, raw_number))

    if not number:
        await update.message.reply_text("❌ Invalid number format.")
        return

    url = f"https://mobile.fmcsa.dot.gov/qc/services/carriers/{number}?webKey={FMCSA_API_KEY}"

    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"API returned status code {response.status_code}")

        data = response.json()

        if not data or "content" not in data or not data["content"]:
            await update.message.reply_text("⚠️ Broker not found.")
            return

        broker = data["content"][0]
        name = broker.get("legalName", "N/A")
        dot = broker.get("dotNumber", "N/A")
        mc = broker.get("docketNumber", "N/A")
        phone = broker.get("phoneNumber", "N/A")
        status = broker.get("entityStatus", "N/A")

        message = (
            f"📦 *Broker Info:*\n"
            f"• Name: {name}\n"
            f"• DOT: {dot}\n"
            f"• MC: {mc}\n"
            f"• Phone: {phone}\n"
            f"• Status: {status}"
        )

        await update.message.reply_text(message, parse_mode="Markdown")

    except Exception as e:
        print(f"Error: {e}")
        await update.message.reply_text("❌ Error fetching broker data.")

async def my_loads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    sheet = get_sheet()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    # Приводим типы и фильтруем по user_id
    df = df[df["User ID"].astype(str) == user_id]
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df = df.dropna(subset=["Date"])  # удалим строки без даты
    df = df.sort_values(by="Date", ascending=False).head(5)

    if df.empty:
        await update.message.reply_text("🚫 You don't have any submitted loads yet.")
        return

    for _, row in df.iterrows():
        # Генерация ID груза
        load_id = f"{row['Date'].date()}_{row['Pickup ZIP']}_{user_id}"

        # Текст груза
        text = (
            f"🗓 {row['Date'].date()}\n"
            f"📍 {row['Pickup ZIP']} → {row['Delivery ZIP']}\n"
            f"📏 Miles: {row['Total Miles']}\n"
            f"💵 Rate: ${row['Rate']} | RPM: {row.get('RPM Total', '—')}\n"
            f"🚛 Trailer: {row.get('Trailer', '—')}\n"
            f"💬 Comment: {row.get('Comment', '—')}"
        )

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Edit", callback_data=f"edit_{load_id}")]
        ])

        await update.message.reply_text(text, reply_markup=keyboard)




if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    submit_conv = ConversationHandler(
        entry_points=[CommandHandler("submit", submit)],
        states={
            PICKUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, pickup_step)],
            DELIVERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_step)],
            TOTAL_MILES: [MessageHandler(filters.TEXT & ~filters.COMMAND, miles_step)],
            RATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, rate_step)],
            TRAILER: [CallbackQueryHandler(trailer_step, pattern=r"^tr_")],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, comment_step)],
        },
        fallbacks=[CallbackQueryHandler(cancel_flow, pattern="^cancel$")],
        per_chat=True


    )

    stats_conv = ConversationHandler(
        entry_points=[CommandHandler("stats", stats_start)],
        states={
            STATS_SELECT: [CallbackQueryHandler(handle_stats_selection)]
        },
        fallbacks=[],
        per_chat=True
    )

    my_stats_conv = ConversationHandler(
        entry_points=[CommandHandler("my_stats", my_stats_start)],
        states={
            MY_STATS_DAY: [CallbackQueryHandler(handle_my_day_selection)]
        },
        fallbacks=[],
        per_chat=True
    )

    app.add_handler(submit_conv)
    app.add_handler(stats_conv)
    app.add_handler(my_stats_conv)
    app.add_handler(CommandHandler("broker", broker_lookup))
    app.add_handler(CommandHandler("my_loads", my_loads))

    app.run_polling()

