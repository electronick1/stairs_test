
import pytest

from stepist.app import App as StepistApp
from stepist.flow.workers.adapters.rm_queue import RQAdapter
from stepist.flow.workers.adapters.sqs_queue import SQSAdapter

from stairs.core.worker.data_pipeline import DataPipeline, DataFrame
from stairs.core.project import StairsProject
from stairs.core import app as stairs_app


@pytest.fixture()
def stepist_app():
    return StepistApp()


@pytest.fixture()
def redis_project(stepist_app):
    return StairsProject(stepist_app)


@pytest.fixture()
def rmq_project(stepist_app):
    worker_engine = RQAdapter()
    return StairsProject(stepist_app, worker_engine=worker_engine)


@pytest.fixture()
def sqs_project(stepist_app):
    return StairsProject(stepist_app)


@pytest.fixture(params=[redis_project, rmq_project, sqs_project])
def app(project):
    app = stairs_app.App(project)
    project.add_app(app)
    return app


@pytest.fixture()
def simple_pipeline(app):
    pipeline = lambda worker: DataFrame(DataPipeline.make_empty(app, worker))
    return app.pipeline()(pipeline)
