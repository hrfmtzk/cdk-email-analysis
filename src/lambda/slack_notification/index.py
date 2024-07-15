import json
import os
from dataclasses import dataclass, field
from datetime import datetime

import requests
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.data_classes import SNSEvent, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
tracer = Tracer()


slack_webhook_url = os.environ["SLACK_WEBHOOK_URL"]


@dataclass
class EmailSummary:
    subject: str
    date: str
    from_: str
    summary: str


@dataclass
class Message:
    success: bool
    emails: list[EmailSummary] = field(default_factory=list)
    error: str | None = None

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(
            success=d["success"],
            emails=[EmailSummary(**email) for email in d.get("emails", [])],
            error=d.get("error"),
        )


def generate_payload(emails: list[EmailSummary]) -> dict:
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":email: 新しいメールが見つかりました",
            },
        },
    ]

    for email in emails:
        blocks.append(
            {
                "type": "divider",
            }
        )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{email.subject}*",
                },
            }
        )
        blocks.append(
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": datetime.fromisoformat(email.date).strftime(
                            r"%Y/%m/%d %H:%M"
                        ),
                    },  # type: ignore
                    {
                        "type": "mrkdwn",
                        "text": email.from_,
                    },  # type: ignore
                ],
            }
        )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": email.summary,
                    "emoji": True,
                },
            }
        )

    return {"blocks": blocks}


def send_email_summaries(emails: list[EmailSummary]) -> None:
    requests.post(slack_webhook_url, json=generate_payload(emails))


def send_error_message(error: str) -> None:
    payload = {"text": f"Error: {error}"}
    requests.post(slack_webhook_url, json=payload)


@tracer.capture_lambda_handler
@logger.inject_lambda_context(log_event=True)
@event_source(data_class=SNSEvent)
def lambda_handler(event: SNSEvent, context: LambdaContext) -> None:
    for record in event.records:
        message = Message.from_dict(json.loads(record.sns.message))
        logger.info(message)
        if message.success:
            send_email_summaries(message.emails)
        else:
            send_error_message(message.error or "no error contained")
    return
