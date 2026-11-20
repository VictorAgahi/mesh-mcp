from celery import Celery

from layered_py.report import build_report

celery_app = Celery("layered_py", broker="redis://localhost:6379/0")


@celery_app.task
def generate_report(order_id):
    build_report(order_id)
