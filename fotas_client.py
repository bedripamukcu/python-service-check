import click
import asyncio
import logging
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
from rpc import Client
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO)

def maxxx(data, start):
    ch = np.argmax(data) + start
    logger.info(data)
    logger.info("%d %.3f %.3f", ch, ch * 9.96, ch*9.96+32198)

last_graph_time = None

async def on_graph(*arg, **argv):
    global last_graph_time
    last_graph_time = dt.datetime.now()

def send_email(subject, message, recipient):
    sender_email = "bedri.pamukcu@samm.com"
    password = "Bedri1995."

    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print("An error occurred while sending the email:", str(e))

async def start_client(client: Client, host: str, port: int):
    await client.connect(host, port)
    client.on("graph", on_graph)
    await client.call("subscribeGraph", {"graph": "Raw"})
    while client.connected:
        if last_graph_time:
            if dt.datetime.now() - last_graph_time > dt.timedelta(minutes=1):
                recipients = ["bedri.pamukcu@samm.com"]
                send_email("Graph Notification", "Sinyal bulunamadı.", recipients)
        await asyncio.sleep(0.1)

    if not client.connected:
        recipients = ["bedri.pamukcu@samm.com"]
        send_email("Sinyal Kesildi", "Sisteminiz çalışmayı durdurmuştur.", recipients)


@click.command()
@click.option("--host", default="10.60.0.30", help="Server host IP address.")
@click.option("--port", default=3002, type=int, help="Server port number.")
def main(host, port):
    processor = None
    try:
        client = Client()
        loop = asyncio.get_event_loop()

        loop.run_until_complete(start_client(client, host, port))
    finally:
        logger.info("Closing")
        if processor is not None:
            processor.stop()


if __name__ == "__main__":
    main()
