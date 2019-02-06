from stairs.core.project import StairsProject


def test_project_init():
    project = StairsProject(redis_kwargs=dict(host='localhost', port=6380))
    assert project.stepist_app.config.redis_kwargs['port'] == 6380


def test_project_init_from_config():
    stairs_project = StairsProject(config_file="config.py")
    assert stairs_project.stepist_app.config.redis_kwargs['port'] == 6380