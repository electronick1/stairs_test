
import pytest

from stepist.app import App as StepistApp
from stepist.flow.workers.adapters.rm_queue import RQAdapter
from stepist.flow.workers.adapters.sqs_queue import SQSAdapter

from stairs.core.pipeline.data_pipeline import DataPipeline, DataFrame
from stairs.core.project import StairsProject
from stairs.core import app as stairs_app


@pytest.fixture()
def stepist_app():
    return StepistApp()


@pytest.fixture()
def project(stepist_app):
    return StairsProject(stepist_app)


def redis_project():
    return StairsProject()


def rmq_project():
    worker_engine = RQAdapter()
    return StairsProject(worker_engine=worker_engine)


def sqs_project():
    worker_engine = SQSAdapter()
    return StairsProject(worker_engine=worker_engine)


@pytest.fixture(params=[redis_project],
                ids=['redis'])
def app(request):
    project = request.param()
    project.dbs.redis_db.flushall()
    app = stairs_app.App("test")
    project.add_app(app)
    return app


@pytest.fixture()
def simple_pipeline(app):
    @app.pipeline()
    def simple_pipeline(worker):
        return DataFrame(DataPipeline.make_empty(app, worker))

    return simple_pipeline
