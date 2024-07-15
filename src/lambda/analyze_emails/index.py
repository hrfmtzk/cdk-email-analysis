import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import boto3
from anthropic import AnthropicBedrock
from anthropic.types import TextBlock
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from jinja2 import Template

logger = Logger()
tracer = Tracer()

bucket_name = os.environ["BUCKET_NAME"]
json_email_path = os.environ["JSON_EMAIL_PATH"]
timezone = os.environ["TIMEZONE"]
topic_arn = os.environ["TOPIC_ARN"]

s3 = boto3.client("s3")

with open("prompt.jinja2") as fh:
    prompt_template = fh.read()


@dataclass
class Email:
    id: str
    subject: str
    from_: str
    date: datetime
    body: str


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

    def asdict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"success": self.success}
        if self.success:
            value["emails"] = [asdict(email) for email in self.emails]
        else:
            value["error"] = self.error
        return value


@tracer.capture_method
def get_emails(bucket_name: str, path: str) -> list[Email]:
    emails = []
    list_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path)
    logger.debug(list_response)
    for obj in list_response["Contents"]:
        response = s3.get_object(Bucket=bucket_name, Key=obj["Key"])
        email_json = json.loads(response["Body"].read().decode("utf-8"))
        email = Email(
            id=email_json["id"],
            subject=email_json["subject"],
            from_=email_json["from"],
            date=datetime.fromisoformat(email_json["date"]),
            body=email_json["body"],
        )
        logger.debug(f"Found email {email.id}")
        emails.append(email)

    return emails


def generate_user_prompt(emails: list[Email]) -> str:
    template = Template(prompt_template)
    return template.render(emails=emails)


@tracer.capture_method
def request_message(prompt: str) -> Message:
    client = AnthropicBedrock()
    message = client.messages.create(
        model="anthropic.claude-v2:1",
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": prompt,
            },
            {
                "role": "assistant",
                "content": "[",
            },
        ],
        temperature=1.0,
        top_p=0.999,
        top_k=250,
    )

    assert isinstance(message.content[0], TextBlock)
    response_text = "[" + message.content[0].text
    logger.info(f"Response: {response_text}")
    logger.info(f"Tokens used: {message.usage}")

    try:
        email_summaries = [
            EmailSummary(
                subject=email["subject"],
                date=email["date"],
                from_=email["from"],
                summary=email["summary"],
            )
            for email in json.loads(response_text)
        ]
        result_message = Message(
            success=True,
            emails=email_summaries,
        )
    except (json.JSONDecodeError, KeyError):
        result_message = Message(
            success=False,
            error=f"Failed to parse response: {response_text}",
        )

    return result_message


def send_to_topic(message: Message) -> None:
    sns = boto3.client("sns")
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message.asdict()),
    )


@tracer.capture_lambda_handler
@logger.inject_lambda_context(log_event=True)
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> None:
    if "path_override" in event:
        path = event["path_override"]
    else:
        target = datetime.now(ZoneInfo(timezone)) - timedelta(days=1)
        path = (
            str(
                Path(
                    f"{json_email_path}/{target.year}/{target.month}/{target.day}"  # noqa: E501
                )
            )
            + "/"
        )

    emails = get_emails(bucket_name=bucket_name, path=path)

    logger.info(f"Found {len(emails)} emails for {path}")

    if not emails:
        return

    message = request_message(prompt=generate_user_prompt(emails=emails))

    logger.info(message)

    if message.success and not message.emails:
        return

    send_to_topic(message)

    return
