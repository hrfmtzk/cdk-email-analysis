import os

import aws_cdk as cdk
from dotenv import find_dotenv, load_dotenv

from cdk_email_analysis.cdk_email_analysis_stack import CdkEmailAnalysisStack

load_dotenv(find_dotenv())


app = cdk.App()
CdkEmailAnalysisStack(
    app,
    "CdkEmailAnalysisStack",
    env=cdk.Environment(
        account=os.getenv("AWS_ACCOUNT"),
        region=os.getenv("AWS_REGION"),
    ),
    receiving_emails=(
        os.getenv("RECEIVING_EMAILS", "").split(",")
        if os.getenv("RECEIVING_EMAILS")
        else []
    ),
    hosted_zone_name=os.getenv("HOSTED_ZONE_NAME"),
    existing_rule_set_name=os.getenv("EXISTING_RULE_SET_NAME"),
    timezone=os.getenv("TIMEZONE", "UTC"),
    slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
    powertools_log_level=os.getenv("POWERTOOLS_LOG_LEVEL", "INFO"),
    sentry_dsn=os.getenv("SENTRY_DSN"),
)

app.synth()
