from stepist import Step
from contextlib import contextmanager


def run_pipelines(app):
    try:
        app.project.run_pipelines(die_when_empty=True)
    except SystemExit:
        pass


def check_keys(keys1, keys2):
    return set(keys1) == set(keys2)


def function_as_step(func):

    return type('input',
                (object,),
                dict(step=Step(None, func, None, False, False)))
