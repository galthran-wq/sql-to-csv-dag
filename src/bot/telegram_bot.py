import os
import logging
import time
import httpx
import asyncio

from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackContext, filters, ApplicationBuilder
import requests

from src.common.config import get_config

config = get_config()

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger('httpx').setLevel(logging.WARNING)

# Your Airflow API endpoint
AIRFLOW_API_URL = f"http://{os.environ.get('AIRFLOW_API_HOST', 'localhost')}:{os.environ.get('AIRFLOW_API_PORT', '8080')}/api/v1/dags/sql_to_csv/dagRuns"
AUTH = ('airflow', 'airflow')

# Your Telegram bot token
TELEGRAM_TOKEN = config.telegram_token

async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text('Hi! Send me a MEGA link to start the conversion process.')

async def handle_message(update: Update, context: CallbackContext) -> None:
    text = update.message.text
    if "mega.nz" in text:
        # Extract MEGA link
        mega_link = text.split()[0]  # Assuming the link is the first word
        await update.message.reply_text(f"Received MEGA link: {mega_link}. Starting conversion...")

        # Trigger Airflow DAG
        response = requests.post(
            AIRFLOW_API_URL,
            json={"conf": {"mega_url": mega_link}},
            auth=AUTH
        )

        if response.status_code == 200:
            dag_run_id = response.json().get('dag_run_id')
            await update.message.reply_text("DAG triggered successfully! Monitoring execution in the background...")

            # Monitor DAG execution in the background
            asyncio.create_task(monitor_dag_execution(update, dag_run_id))
        else:
            await update.message.reply_text("Failed to trigger DAG.")

async def monitor_dag_execution(update: Update, dag_run_id: str) -> None:
    status_url = f"{AIRFLOW_API_URL}/{dag_run_id}"
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(status_url, auth=AUTH)
            response_data = response.json()
            if response.status_code == 200:
                state = response_data.get('state')
                if state == 'success':
                    await update.message.reply_text("Conversion successful! Retrieving link...")
                    # Retrieve and send the resulting MEGA link
                    await send_resulting_link(update, response_data)
                    break
                elif state == 'failed':
                    await update.message.reply_text("DAG execution failed. Fetching logs...")
                    await fetch_and_send_logs(update, dag_run_id)
                    break
            else:
                await update.message.reply_text("Failed to retrieve DAG status.")
                break
            await asyncio.sleep(60)

async def fetch_and_send_logs(update: Update, dag_run_id: str) -> None:
    # Fetch logs for the failed task
    logs_url = f"{AIRFLOW_API_URL}/{dag_run_id}/taskInstances"
    async with httpx.AsyncClient() as client:
        response = await client.get(logs_url, auth=AUTH)
        if response.status_code == 200:
            task_instances = response.json().get('task_instances', [])
            for task_instance in task_instances:
                if task_instance['state'] == 'failed':
                    task_id = task_instance['task_id']
                    log_url = f"{AIRFLOW_API_URL}/{dag_run_id}/taskInstances/{task_id}/logs/1"
                    log_response = await client.get(log_url, auth=AUTH)
                    if log_response.status_code == 200:
                        logs = log_response.text
                        await update.message.reply_text(f"Logs for task {task_id}:\n{logs}")
                    else:
                        await update.message.reply_text(f"Failed to fetch logs for task {task_id}.")
        else:
            await update.message.reply_text("Failed to fetch task instances.")


async def send_resulting_link(update: Update, response_data: dict) -> None:
    # Placeholder for retrieving the resulting MEGA link
    # You need to implement the logic to get the link from your DAG
    await update.message.reply_text(f"Output: {response_data}")


if __name__ == '__main__':
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    application.run_polling()
