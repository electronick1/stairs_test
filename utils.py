import pytest
import json
import redis
import uuid
import multiprocessing
from stepist import Step
from contextlib import contextmanager


def run_pipelines(app):
    try:
        app.project.run_pipelines(die_when_empty=True)
    except SystemExit:
        pass


def run_pipelines_process(app) -> multiprocessing.Process:
    t = multiprocessing.Process(target=app.project.run_pipelines)
    t.start()
    return t


def check_keys(keys1, keys2):
    return set(keys1) == set(keys2)


def function_as_step(func):

    return type('input',
                (object,),
                dict(step=Step(None, func, None, False, False),
                     __name__=lambda: func.__name__))


class GlobalTestData:
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.r = redis.Redis()

    def save_one_item(self, **data):
        self.r.set(self.id, json.dumps(data), ex=30)
        return data

    def save_multiple_items(self, **data):
        current_data = self.get_result()
        if current_data is None:
            current_data = []
        current_data.append(data)
        self.r.set(self.id, json.dumps(current_data), ex=30)
        return data

    def get_result(self):
        d = self.r.get(self.id)
        if d is None:
            return None
        return json.loads(d)
