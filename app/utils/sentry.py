import os
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

def init_sentry():
    """Initialize Sentry for error tracking"""
    sentry_dsn = os.getenv("SENTRY_DSN")
    
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            integrations=[FastApiIntegration(), SqlalchemyIntegration()],
            traces_sample_rate=1.0,
            profiles_sample_rate=1.0,
            send_default_pii=True,
            environment=os.getenv("ENVIRONMENT", "production")
        )
        print("✓ Sentry initialized")
    else:
        print("⚠ Sentry DSN not found - error tracking disabled")
