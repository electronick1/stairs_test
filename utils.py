from contextlib import contextmanager


def run_pipelines(app):
    try:
        app.project.run_pipelines(die_when_empty=True)
    except SystemExit:
        pass


