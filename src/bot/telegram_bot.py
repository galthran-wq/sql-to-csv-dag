import os
import logging
import time
import httpx
import asyncio

from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackContext, filters, ApplicationBuilder
from telegram.helpers import escape_markdown
import requests

from src.common.config import get_config
from src.lib.mega_tools import upload_file_to_mega


config = get_config()

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger('httpx').setLevel(logging.WARNING)

# Your Airflow API endpoint
AIRFLOW_API_URL = f"http://{os.environ.get('AIRFLOW_API_HOST', 'localhost')}:{os.environ.get('AIRFLOW_API_PORT', '8080')}/api/v1/dags/sql_to_csv/dagRuns"
AUTH = ('airflow', 'airflow')

# Your Telegram bot token
TELEGRAM_TOKEN = config.telegram_token

# Dictionary to store the state of tasks for each user
STATE_DICT = {}

async def start(update: Update, context: CallbackContext) -> None:
    logging.info("Received /start command from user: %s", update.message.from_user.id)
    await update.message.reply_text('Hi! Send me a MEGA link to start the conversion process.')

async def trigger_dag(update: Update, mega_link: str, telegram_id: int) -> None:
    response = requests.post(
        AIRFLOW_API_URL,
        json={"conf": {"mega_url": mega_link}},
        auth=AUTH
    )

    if response.status_code == 200:
        dag_run_id = response.json().get('dag_run_id')
        logging.info("DAG triggered successfully with ID: %s", dag_run_id)
        await update.message.reply_text("DAG triggered successfully! Monitoring execution in the background...")

        # Update STATE_DICT with the new task
        task_info = {
            "id": dag_run_id,
            "date": time.time(),
            "parameters": {"mega_url": mega_link},
            "resulting_link": None,
            "current_step": "Triggered DAG",
            "status": "In Progress"
        }
        if telegram_id not in STATE_DICT:
            STATE_DICT[telegram_id] = []
        STATE_DICT[telegram_id].append(task_info)
        # Keep only the latest 20 tasks
        STATE_DICT[telegram_id] = STATE_DICT[telegram_id][-20:]

        # Monitor DAG execution in the background
        asyncio.create_task(monitor_dag_execution(update, dag_run_id, telegram_id))
    else:
        logging.error("Failed to trigger DAG. Status code: %s", response.status_code)
        await update.message.reply_text("Failed to trigger DAG.")

async def handle_message(update: Update, context: CallbackContext) -> None:
    telegram_id = update.message.from_user.id
    logging.info("Received message from user %s", telegram_id)

    document = update.message.document or update.message.reply_to_message.document
    logging.info("Detected document: %s", document.file_name)
    if document:
        # Step 2: Download the document
        file_id = document.file_id
        file_name = document.file_name
        file = await context.bot.get_file(file_id)
        file_path = f"/tmp/{file_name}"
        await file.download_to_drive(file_path)
        logging.info("Downloaded file: %s", file_path)

        # Step 3: Upload to MEGA
        mega_email = config.mega.email
        mega_password = config.mega.password
        resulting_link = upload_file_to_mega(file_path, mega_email, mega_password)
        
        if resulting_link:
            logging.info("Uploaded to MEGA: %s", resulting_link)
            await update.message.reply_text(f"File uploaded to MEGA: {resulting_link}. Starting conversion...")
            await trigger_dag(update, resulting_link, telegram_id)
            os.remove(file_path)
            logging.info("Deleted temp file: %s", file_path)
        else:
            await update.message.reply_text("Failed to upload file to MEGA.")
    else:
        text = update.message.text
        logging.info("Received message from user %s: %s", telegram_id, text)
        if "mega.nz" in text:
            # Extract MEGA link
            mega_link = text.split()[0]  # Assuming the link is the first word
            logging.info("Extracted MEGA link: %s", mega_link)
            await update.message.reply_text(f"Received MEGA link: {mega_link}. Starting conversion...")
            await trigger_dag(update, mega_link, telegram_id)

async def monitor_dag_execution(update: Update, dag_run_id: str, telegram_id: int) -> None:
    logging.info("Monitoring DAG execution for DAG ID: %s", dag_run_id)
    status_url = f"{AIRFLOW_API_URL}/{dag_run_id}"
    task_instances_url = f"{AIRFLOW_API_URL}/{dag_run_id}/taskInstances"
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(status_url, auth=AUTH)
            response_data = response.json()
            if response.status_code == 200:
                state = response_data.get('state')

                # Fetch task instances to get the current step
                task_instances_response = await client.get(task_instances_url, auth=AUTH)
                if task_instances_response.status_code == 200:
                    task_instances = task_instances_response.json().get('task_instances', [])
                    current_task_name = None
                    for task_instance in task_instances:
                        if task_instance['state'] == 'running':
                            current_task_name = task_instance['task_id']
                            break

                logging.info("DAG ID %s is in state: %s; step: %s", dag_run_id, state, current_task_name)

                # Update the current step and status in STATE_DICT
                for task in STATE_DICT.get(telegram_id, []):
                    if task["parameters"]["mega_url"] == response_data.get("conf", {}).get("mega_url"):
                        task["current_step"] = current_task_name or "Monitoring DAG Execution"
                        task["status"] = state.capitalize()
                        break

                if state == 'success':
                    await update.message.reply_text("Conversion successful! Retrieving link...")
                    # Retrieve the resulting link from the last task
                    await retrieve_resulting_link(update, dag_run_id, telegram_id)
                    break
                elif state == 'failed':
                    await update.message.reply_text("DAG execution failed. Fetching logs...")
                    await fetch_and_send_logs(update, dag_run_id)
                    break
            else:
                logging.error("Failed to retrieve DAG status for DAG ID: %s", dag_run_id)
                await update.message.reply_text("Failed to retrieve DAG status.")
                break
            await asyncio.sleep(60)

async def fetch_and_send_logs(update: Update, dag_run_id: str) -> None:
    logging.info("Fetching logs for DAG ID: %s", dag_run_id)
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
                        # Split logs into chunks of 4000 characters
                        for i in range(0, len(logs), 4000):
                            chunk = logs[i:i+4000]
                            await update.message.reply_text(f"```\n{escape_markdown(chunk, version=2)}\n```", parse_mode='MarkdownV2')
                    else:
                        await update.message.reply_text(f"Failed to fetch logs for task {task_id}.")
        else:
            logging.error("Failed to fetch task instances for DAG ID: %s", dag_run_id)
            await update.message.reply_text("Failed to fetch task instances.")

async def retrieve_resulting_link(update: Update, dag_run_id: str, telegram_id: int) -> None:
    logging.info("Retrieving resulting link for DAG ID: %s", dag_run_id)
    # the last task's ID is known and it stores the output link in XCom
    last_task_id = "upload_files_to_mega"
    xcom_url = f"{AIRFLOW_API_URL}/{dag_run_id}/taskInstances/{last_task_id}/xcomEntries/return_value"
    async with httpx.AsyncClient() as client:
        response = await client.get(xcom_url, auth=AUTH)
        if response.status_code == 200:
            xcom_data = response.json()
            resulting_link = xcom_data.get('value')
            if resulting_link:
                await update.message.reply_text(f"Resulting link: {resulting_link}")

                # Update the resulting link in STATE_DICT using the unique ID
                for task in STATE_DICT.get(telegram_id, []):
                    if task["id"] == dag_run_id:  # Use the unique ID to find the correct task
                        task["resulting_link"] = resulting_link
                        task["current_step"] = "Completed"
                        task["status"] = "Success"
                        break
            else:
                print(xcom_data)
                await update.message.reply_text("Failed to retrieve the resulting link.")
        else:
            logging.error("Failed to retrieve XCom data for DAG ID: %s", dag_run_id)
            await update.message.reply_text("Failed to retrieve XCom data.")

async def status(update: Update, context: CallbackContext) -> None:
    telegram_id = update.message.from_user.id
    logging.info("Checking status for user: %s", telegram_id)
    tasks = STATE_DICT.get(telegram_id, [])

    if not tasks:
        await update.message.reply_text("No tasks found for your account.")
        return

    status_messages = []
    status_messages.append("; ".join(["Date", "Link", "Step", "Status", "Result Link"]))
    for task in tasks:
        message = (
            f"{escape_markdown(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(task['date'])), version=2)}; " + 
            f"[link]({escape_markdown(task['parameters']['mega_url'], version=2)}); " + 
            f"{escape_markdown(task['current_step'], version=2)}; " + 
            f"{escape_markdown(task['status'], version=2)}; " + 
            (f"[result]({escape_markdown(task['resulting_link'], version=2)})" if task['resulting_link'] else "")
        )
        status_messages.append(message)

    message = "\n".join(status_messages) if status_messages else "No tasks found"

    await update.message.reply_text(message, parse_mode='MarkdownV2')

if __name__ == '__main__':
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(MessageHandler(
        (filters.TEXT & ~filters.COMMAND) | 
        (filters.FORWARDED & filters.Document.ALL) |
        (filters.Document.ALL), 
        handle_message
    ))

    application.run_polling()
