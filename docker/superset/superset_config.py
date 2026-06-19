import os

SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_DATABASE_URI"]
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-local-dev-only")

WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
