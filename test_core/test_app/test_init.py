from stairs import App, signals
from stairs import Pipeline


def test_project_signals(project):
    """
    Check whether signals were sent when app initialized
    """
    is_project_ready = []

    @signals.on_app_ready('test_signal')
    def init_based_on_signal(app):
        is_project_ready.append(app)

    App("test_signal")

    assert len(is_project_ready)


def test_app_compile(project):
    """
    Check whether app compiled, and initialize all needed components
    """

    def pipeline_builder(pipeline, value):
        return value.subscribe_func(lambda x: dict(), as_worker=True)

    app = App('test_app')
    pipeline = Pipeline(app, pipeline_builder, dict())

    assert len(project.steps_to_run()) == 1

    pipeline.compile()

    # After pipeline compile new stepist step as worker should be initialized
    assert len(project.steps_to_run()) == 2