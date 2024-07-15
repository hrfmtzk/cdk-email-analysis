import json
import os
from dataclasses import dataclass
from datetime import datetime
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.data_classes import SESEvent, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
tracer = Tracer()

bucket_name = os.environ["BUCKET_NAME"]
raw_email_path = os.environ["RAW_EMAIL_PATH"]
json_email_path = os.environ["JSON_EMAIL_PATH"]
timezone = os.environ["TIMEZONE"]


s3 = boto3.client("s3")


class HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.reset()
        self.fed: list[str] = []

    def handle_data(self, d: str):
        self.fed.append(d)

    def get_data(self) -> str:
        return "".join(self.fed)


def strip_html(html: str) -> str:
    stripper = HTMLStripper()
    stripper.feed(html)
    return stripper.get_data()


@dataclass
class Email:
    id: str
    subject: str
    from_: str
    date: datetime
    body: str


class EmailParser:
    def read_email(self, email_id: str, timezone: str) -> Email:
        response = s3.get_object(
            Bucket=bucket_name,
            Key=str(Path(f"{raw_email_path}/{email_id}")),
        )
        raw_email = response["Body"].read()

        message = BytesParser(policy=policy.default).parsebytes(raw_email)
        assert isinstance(message, EmailMessage)  # for type hinting

        email = Email(
            id=email_id,
            subject=message["subject"],
            from_=message["from"],
            date=self._get_email_date(message=message, timezone=timezone),
            body=self._get_email_body(message=message),
        )
        return email

    def _get_email_date(
        self,
        message: EmailMessage,
        timezone: str,
    ) -> datetime:
        date: datetime | None = parsedate_to_datetime(message["date"])

        if date is None:  # pragma: no cover
            raise ValueError("Email date not found")

        return date.astimezone(ZoneInfo(timezone))

    def _get_email_body(self, message: EmailMessage) -> str:
        body: str | None = None
        if message.is_multipart():
            for part in message.iter_parts():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True).decode(
                        part.get_content_charset()
                    )
                    break
                elif part.get_content_type() == "text/html":
                    body = strip_html(
                        part.get_payload(decode=True).decode(
                            part.get_content_charset()
                        )
                    )
        else:
            if message.get_content_type() == "text/plain":
                body = message.get_payload(decode=True).decode(
                    message.get_content_charset()
                )
            elif message.get_content_type() == "text/html":
                body = strip_html(
                    message.get_payload(decode=True).decode(
                        message.get_content_charset()
                    )
                )

        if body is None:
            raise ValueError("Email body not found")

        return body

    def write_email(self, email: Email) -> None:
        key = str(
            Path(
                f"{json_email_path}/{email.date.year}/{email.date.month}/{email.date.day}/{email.id}.json"  # noqa: E501
            )
        )
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(
                {
                    "id": email.id,
                    "subject": email.subject,
                    "from": email.from_,
                    "date": email.date.isoformat(),
                    "body": email.body,
                }
            ).encode("utf-8"),
            ContentType="application/json",
        )


@tracer.capture_lambda_handler
@logger.inject_lambda_context(log_event=True)
@event_source(data_class=SESEvent)
def lambda_handler(event: SESEvent, context: LambdaContext) -> None:
    email_parser = EmailParser()

    for record in event.records:
        email = email_parser.read_email(
            email_id=record.ses.mail.message_id,
            timezone=timezone,
        )
        logger.info(f"Email parsed: {email}")
        email_parser.write_email(email=email)

    return
