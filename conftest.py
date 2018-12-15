
import pytest

from stepist.app import App as StepistApp

from stairs.core.project import StairsProject
from stairs.core import app as stairs_app


@pytest.fixture()
def stepist_app():
    return StepistApp()


@pytest.fixture()
def project(stepist_app):
    return StairsProject(stepist_app)


@pytest.fixture()
def app(project):
    app = stairs_app.App(project)
    project.add_app(app)
    return app
