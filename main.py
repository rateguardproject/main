import os
import pandas as pd
import gspread
import pgeocode
import requests
import asyncio
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

edit_state = {}  # user_id: {row_index, message_ids, step}

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

# === START SUBMIT FLOW ===
submit_states = ["pickup_zip", "delivery_zip", "total_miles", "rate", "trailer", "comment"]
submit_step_texts = [
    "📍 *Step 1/6* — Enter pickup ZIP or State abbreviation (e.g., CA):",
    "📍 *Step 2/6* — Enter delivery ZIP or State abbreviation:",
    "📏 *Step 3/6* — Enter total miles:",
    "💵 *Step 4/6* — Enter total rate ($):",
    "🚛 *Step 5/6* — Choose trailer type:",
    "💬 *Step 6/6* — Add comment (or press 'Skip')"
]
submit_current_messages = {}  # для отслеживания сообщения с кнопками

async def submit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data["submit_step"] = 0
    await send_submit_step(update.effective_chat.id, context)
    return PICKUP

async def send_submit_step(chat_id, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get("submit_step", 0)
    text = submit_step_texts[step]

    # Кнопки отмены
    buttons = [[InlineKeyboardButton("❌ Cancel", callback_data="cancel")]]

    if step == 4:  # трейлеры
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
    # Сохраняем ID последнего сообщения пользователя, чтобы потом удалить
    context.user_data["last_user_message_id"] = update.message.message_id
    step = context.user_data.get("submit_step", 0)
    field = submit_states[step]
    context.user_data[field] = user_input

    # Удаление старого сообщения бота
    if chat_id in submit_current_messages:
        await context.bot.delete_message(chat_id, submit_current_messages[chat_id])
    # Удаляем последнее сообщение пользователя
    if "last_user_message_id" in context.user_data:
        try:
            await context.bot.delete_message(chat_id, context.user_data["last_user_message_id"])
        except:
            pass  # Игнорируем, если нельзя удалить

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
        context.user_data.clear()  # очищаем данные
        return ConversationHandler.END  # завершаем сценарий
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
    user = update.effective_user
    username = f"@{user.username}" if user.username else user.full_name
    user_id = str(user.id)
    data = context.user_data

    # Конвертация
    try:
        total = float(str(data["total_miles"]).replace("$", ""))
        rate = float(str(data["rate"]).replace("$", ""))
    except ValueError:
        msg = await update.effective_message.reply_text("❌ Submission failed. Invalid numbers.")
        await context.bot.delete_message(update.effective_chat.id, msg.message_id, delay=5)
        return

    date = datetime.now().strftime("%Y-%m-%d")
    pickup_city, pickup_state = resolve_location(data["pickup_zip"])
    delivery_city, delivery_state = resolve_location(data["delivery_zip"])
    pickup = f"{pickup_city}, {pickup_state}" if pickup_city else pickup_state
    delivery = f"{delivery_city}, {delivery_state}" if delivery_city else delivery_state
    rpm_total = format(rate / total, '.2f') if total else ""

    sheet = get_sheet()
    sheet.append_row([
        date,
        data["pickup_zip"],
        data["delivery_zip"],
        "", "",
        total,
        rate,
        "",
        rpm_total,
        data["trailer"],
        username,
        "",
        data.get("comment", ""),
        username,
        user_id
    ])

    text = (
        f"🗓 {date}\n"
        f"🧑‍✈️ Posted by: {username}\n"
        f"📍 {pickup} → {delivery}\n"
        f"📏 Miles: {int(total)}\n"
        f"💵 Rate: ${int(rate)} | RPM: Total — {rpm_total}\n"
        f"🚛 Trailer: {data['trailer']}\n"
        f"💬 Comment: {data['comment'] or '—'}"
    )

    m1 = await context.bot.send_message(chat_id="@rateguard", text=text)
    m2 = await context.bot.send_message(chat_id="-1002235875053", text=text)
    m3 = await update.effective_message.reply_text("✅ Load submitted and published!")
    await asyncio.sleep(5)
    await context.bot.delete_message(update.effective_chat.id, m3.message_id)

async def start_edit_load(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data.split("_")
    if len(data) < 3:
        await query.message.reply_text("❌ Invalid load ID.")
        return

    _, date_str, pickup_zip, user_id = data
    user_id = str(update.effective_user.id)

    # Загрузка таблицы
    sheet = get_sheet()
    records = sheet.get_all_records()
    # Очистка старых сообщений из /my_loads
    if "my_load_messages" in context.user_data:
        for msg_id in context.user_data["my_load_messages"]:
            try:
                await context.bot.delete_message(update.effective_chat.id, msg_id)
            except:
                pass
        context.user_data["my_load_messages"] = []  # очистка списка
    for i, row in enumerate(records):
        if row["Pickup ZIP"] == pickup_zip and str(row["User ID"]) == user_id:
            edit_state[user_id] = {
                "row_index": i + 2,  # +2, т.к. строки начинаются с 1, и есть заголовок
                "data": row,
                "step": 0
            }
            break
    else:
        await query.message.reply_text("❌ Load not found.")
        return

    await show_edit_menu(update, context)

async def show_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    row = edit_state[user_id]["data"]
    pickup = row["Pickup ZIP"]
    delivery = row["Delivery ZIP"]
    trailer = row["Trailer"]
    rate = row["Rate"]
    miles = row["Total Miles"]
    date = row["Date"]

    text = (
        f"🛠 *Edit Load — {date}*\n"
        f"📍 {pickup} → {delivery}\n"
        f"🚛 {trailer} | 💵 ${rate} | {miles} mi\n\n"
        f"Choose field to edit:"
    )
    buttons = [
        [InlineKeyboardButton("📍 Pickup ZIP", callback_data="editfield_pickup")],
        [InlineKeyboardButton("📍 Delivery ZIP", callback_data="editfield_delivery")],
        [InlineKeyboardButton("📏 Total Miles", callback_data="editfield_miles")],
        [InlineKeyboardButton("💵 Rate", callback_data="editfield_rate")],
        [InlineKeyboardButton("🚛 Trailer", callback_data="editfield_trailer")],
        [InlineKeyboardButton("💬 Comment", callback_data="editfield_comment")],
        [InlineKeyboardButton("🔁 Cancel", callback_data="cancel_edit")]
    ]

    await update.callback_query.edit_message_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown")

def resolve_location(value):
    if len(value) == 2 and value.isalpha():
        return ("", value)
    info = nomi.query_postal_code(value)
    return (info.place_name or "", info.state_code or value)


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

async def handle_edit_field_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = str(update.effective_user.id)
    field = query.data.split("_")[1]

    edit_state[user_id]["field"] = field

    question_map = {
        "pickup": "Enter new Pickup ZIP:",
        "delivery": "Enter new Delivery ZIP:",
        "miles": "Enter new total miles:",
        "rate": "Enter new rate ($):",
        "trailer": "Enter new trailer type:",
        "comment": "Enter new comment:"
    }

    msg = await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=question_map[field],
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_edit")]])
    )

    edit_state[user_id]["question_msg_id"] = msg.message_id
    return


async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in edit_state:
        return

    value = update.message.text.strip()
    field = edit_state[user_id]["field"]
    row_idx = edit_state[user_id]["row_index"]
    sheet = get_sheet()

    # Очистим вопрос и ответ
    try:
        await context.bot.delete_message(update.effective_chat.id, edit_state[user_id]["question_msg_id"])
        await context.bot.delete_message(update.effective_chat.id, update.message.message_id)
    except:
        pass

    # Обновим всезначение
    field_map = {
        "pickup": "Pickup ZIP",
        "delivery": "Delivery ZIP",
        "miles": "Total Miles",
        "rate": "Rate",
        "trailer": "Trailer",
        "comment": "Comment"
    }
    col_name = field_map[field]

    sheet.update_cell(row_idx, get_column_index(sheet, col_name), value)
    edit_state[user_id]["data"][col_name] = value

    # ✅ Добавить вот это:
    if field in ["miles", "rate"]:
        update_rpm_in_edit(sheet, row_idx, edit_state[user_id]["data"])

    msg = await context.bot.send_message(update.effective_chat.id, "✅ Value updated.")
    await asyncio.sleep(3)
    await context.bot.delete_message(update.effective_chat.id, msg.message_id)

    await show_edit_menu(update, context)


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

async def cancel_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg = await query.message.reply_text("❌ Editing canceled.")
    await asyncio.sleep(5)
    await context.bot.delete_message(chat_id=msg.chat.id, message_id=msg.message_id)
    user_id = str(update.effective_user.id)
    if user_id in edit_state:
        del edit_state[user_id]
        return ConversationHandler.END

def get_column_index(sheet, column_name):
    header = sheet.row_values(1)
    return header.index(column_name) + 1

def update_rpm_in_edit(sheet, row_idx, updated_data):
    """
    Обновляет RPM Total в Google Sheet после редактирования 'Rate' или 'Total Miles'.
    """
    try:
        rate = float(updated_data.get("Rate", 0))
        miles = float(updated_data.get("Total Miles", 0))
        rpm = round(rate / miles, 2) if miles else ""
    except Exception as e:
        rpm = ""

    if rpm != "":
        # Найдём индекс колонки "RPM Total"
        header = sheet.row_values(1)
        if "RPM Total" in header:
            col_index = header.index("RPM Total") + 1
            sheet.update_cell(row_idx, col_index, str(rpm))
            return True
    return False



if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    submit_conv = ConversationHandler(
        entry_points=[CommandHandler("submit", submit)],
        states={
            PICKUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input),
                     CallbackQueryHandler(handle_submit_callback)],
            DELIVERY: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input),
                       CallbackQueryHandler(handle_submit_callback)],
            TOTAL_MILES: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input),
                          CallbackQueryHandler(handle_submit_callback)],
            RATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input),
                   CallbackQueryHandler(handle_submit_callback)],
            TRAILER: [CallbackQueryHandler(handle_submit_callback)],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_submit_input),
                      CallbackQueryHandler(handle_submit_callback)]
        },
        fallbacks=[],
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
    app.add_handler(CallbackQueryHandler(start_edit_load, pattern="^edit_"))
    app.add_handler(CallbackQueryHandler(handle_edit_field_selection, pattern="^editfield_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_input))
    app.add_handler(CallbackQueryHandler(cancel_edit, pattern="^cancel_edit$"))

    app.run_polling()

