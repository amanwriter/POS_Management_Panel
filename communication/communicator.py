import json
import redis
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


config = json.load(open("mail_config.json", "r"))

def send_mail(event):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = event["subject"]
    msg["From"] = event["from"]
    msg["To"] = event["recipient"]
    message_body = event["message"]
    msg.attach(MIMEText(message_body, "plain"))
    mail = smtplib.SMTP("smtp.gmail.com", 587)

    mail.ehlo()
    mail.starttls()
    mail.login(config["user_id"], config["password"])
    mail.sendmail(config["user_id"], event["recipient"], msg.as_string())
    mail.quit()


def event_subscriber():
    redis_client = redis.Redis()
    while True:
        event = redis_client.brpop("event_pipeline")
        event = json.loads(event)
        # TODO: Processing of the event to add message and recipient
        send_mail(event)

event_subscriber()