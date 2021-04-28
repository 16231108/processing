import queue
import pytest
import threading

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src import app

import logging
log = logging.getLogger(__name__)

work_threading = threading.Thread(target=app.work_thread)
work_threading.start()

@pytest.fixture
def client():
    flask_app = app.app

    client = flask_app.test_client()
    ctx = flask_app.app_context()
    ctx.push()

    yield client  # this is where the testing happens!

    ctx.pop()
