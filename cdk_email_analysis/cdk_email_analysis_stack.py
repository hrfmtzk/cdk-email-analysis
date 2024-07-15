from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python_alpha as lambda_python,
    aws_logs as logs,
    aws_route53 as route53,
    aws_s3 as s3,
    aws_ses as ses,
    aws_ses_actions as ses_actions,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
)
from constructs import Construct


class CdkEmailAnalysisStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        receiving_emails: list[str],
        hosted_zone_name: str | None = None,
        existing_rule_set_name: str | None = None,
        timezone: str = "UTC",
        slack_webhook_url: str | None = None,
        powertools_service_name: str = "email_analysis",
        powertools_log_level: str = "INFO",
        sentry_dsn: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Setup for email receiving

        domains = list(
            set([email.split("@")[1] for email in receiving_emails])
        )  # noqa: E501

        hosted_zone = (
            route53.HostedZone.from_lookup(
                self,
                "HostedZone",
                domain_name=hosted_zone_name,
            )
            if hosted_zone_name
            else None
        )

        for domain in domains:
            if hosted_zone and domain.endswith(hosted_zone.zone_name):
                route53.MxRecord(
                    self,
                    f"MXRecord-{domain}",
                    zone=hosted_zone,
                    record_name=domain,
                    values=[
                        route53.MxRecordValue(
                            host_name=f"inbound-smtp.{self.region}.amazonaws.com",  # noqa: E501
                            priority=10,
                        ),
                    ],
                    comment=f"MX record for {domain}",
                )

        # Receiving rule

        raw_email_path = "raw_emails/"
        json_email_path = "json_emails/"

        if existing_rule_set_name:
            receipt_rule_set = ses.ReceiptRuleSet.from_receipt_rule_set_name(
                self,
                "ReceiptRuleSet",
                receipt_rule_set_name=existing_rule_set_name,
            )
        else:
            receipt_rule_set = ses.ReceiptRuleSet(
                self,
                "ReceiptRuleSet",
                drop_spam=True,
            )

        bucket = s3.Bucket(
            self,
            "Bucket",
            bucket_name=f"email-analysis-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            public_read_access=False,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(7),
                    noncurrent_version_expiration=Duration.days(7),
                ),
            ],
            removal_policy=RemovalPolicy.DESTROY,
        )

        email_parser_function = lambda_python.PythonFunction(
            self,
            "EmailParserFunction",
            entry="src/lambda/email_parser",
            index="index.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "RAW_EMAIL_PATH": raw_email_path,
                "JSON_EMAIL_PATH": json_email_path,
                "TIMEZONE": timezone,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level,
                "SENTRY_DSN": sentry_dsn or "",
            },
            tracing=lambda_.Tracing.ACTIVE,
            timeout=Duration.minutes(3),
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        bucket.grant_read_write(email_parser_function)

        ses.ReceiptRule(
            self,
            "ReceiptRule",
            rule_set=receipt_rule_set,
            recipients=receiving_emails,
            scan_enabled=True,
            tls_policy=ses.TlsPolicy.REQUIRE,
            actions=[
                ses_actions.S3(
                    bucket=bucket,
                    object_key_prefix=raw_email_path,
                ),
                ses_actions.Lambda(
                    function=email_parser_function,
                    invocation_type=ses_actions.LambdaInvocationType.EVENT,
                ),
            ],
        )

        topic = sns.Topic(
            self,
            "Topic",
        )

        # Analyze emails

        anthropic_policy = iam.PolicyStatement(
            actions=[
                "bedrock:InvokeModel",
            ],
            resources=[
                f"arn:aws:bedrock:{self.region}::foundation-model/anthropic.claude-v2:1"  # noqa: E501
            ],
        )

        analyze_emails_function = lambda_python.PythonFunction(
            self,
            "AnalyzeEmailsFunction",
            entry="src/lambda/analyze_emails",
            index="index.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "JSON_EMAIL_PATH": json_email_path,
                "TIMEZONE": timezone,
                "TOPIC_ARN": topic.topic_arn,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level,
                "SENTRY_DSN": sentry_dsn or "",
            },
            tracing=lambda_.Tracing.ACTIVE,
            timeout=Duration.minutes(3),
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        bucket.grant_read(analyze_emails_function)
        topic.grant_publish(analyze_emails_function)
        analyze_emails_function.add_to_role_policy(anthropic_policy)

        events.Rule(
            self,
            "AnalyzeEmailsRule",
            schedule=events.Schedule.cron(
                hour="3",
                minute="0",
            ),
            targets=[
                events_targets.LambdaFunction(
                    handler=analyze_emails_function,
                ),
            ],
        )

        if slack_webhook_url:
            self.add_slack_notification(
                topic=topic,
                slack_webhook_url=slack_webhook_url,
                powertools_log_level=powertools_log_level,
                sentry_dsn=sentry_dsn,
            )

    def add_slack_notification(
        self,
        topic: sns.Topic,
        slack_webhook_url: str,
        powertools_service_name: str = "email_analysis",
        powertools_log_level: str = "INFO",
        sentry_dsn: str | None = None,
    ) -> None:
        function = lambda_python.PythonFunction(
            self,
            "SlackNotificationFunction",
            entry="src/lambda/slack_notification",
            index="index.py",
            handler="lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            environment={
                "SLACK_WEBHOOK_URL": slack_webhook_url,
                "POWERTOOLS_SERVICE_NAME": powertools_service_name,
                "POWERTOOLS_LOG_LEVEL": powertools_log_level,
                "SENTRY_DSN": sentry_dsn or "",
            },
            tracing=lambda_.Tracing.ACTIVE,
            timeout=Duration.minutes(1),
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        topic.add_subscription(
            sns_subscriptions.LambdaSubscription(
                function,
            ),
        )
