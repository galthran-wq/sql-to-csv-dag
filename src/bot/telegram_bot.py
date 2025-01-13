import os
import logging

from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
import requests

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Your Airflow API endpoint
AIRFLOW_API_URL = f"http://{os.environ.get('AIRFLOW_API_HOST', 'localhost')}:{os.environ.get('AIRFLOW_API_PORT', '8080')}/api/v1/dags/sql_to_csv/dagRuns"

# Your Telegram bot token
TELEGRAM_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'

def start(update: Update, context: CallbackContext) -> None:
    update.message.reply_text('Hi! Send me a MEGA link to start the conversion process.')

def handle_message(update: Update, context: CallbackContext) -> None:
    text = update.message.text
    if "mega.nz" in text:
        # Extract MEGA link
        mega_link = text.split()[0]  # Assuming the link is the first word
        update.message.reply_text(f"Received MEGA link: {mega_link}. Starting conversion...")

        # Trigger Airflow DAG
        response = requests.post(
            AIRFLOW_API_URL,
            json={"conf": {"mega_url": mega_link}},
            auth=('airflow', 'airflow')  # Replace with your Airflow credentials
        )

        if response.status_code == 200:
            update.message.reply_text("DAG triggered successfully!")
        else:
            update.message.reply_text("Failed to trigger DAG.")

def main() -> None:
    updater = Updater(TELEGRAM_TOKEN)

    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main() 