from stairs.core.project import StairsProject
from stairs import signals


def test_project_init():
    project = StairsProject(redis_kwargs=dict(host='localhost', port=6380))
    assert project.stepist_app.config.redis_kwargs['port'] == 6380


def test_project_init_from_config():
    stairs_project = StairsProject(config_file="config.py")
    assert stairs_project.stepist_app.config.redis_kwargs['port'] == 6380


def test_project_init_from_config_lower_case():
    """
    Check how project initialized, based on config upper case variables.
    """
    stairs_project = StairsProject(config_file="config.py")
    assert 'test_lower_case' in stairs_project.stepist_app.config


def test_project_signals():
    """
    Check whether signals were sent when project initialized
    """
    is_project_ready = []

    @signals.on_project_ready()
    def init_based_on_signal():
        is_project_ready.append("is_ready")

    StairsProject(config_file="config.py")

    assert len(is_project_ready)
