from utils import run_pipelines, GlobalTestData


def test_simple_producer(app):

    t = GlobalTestData()

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(callback_func)
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    callback_func.compile()
    simple_producer.flush()
    simple_producer.run()
    run_pipelines(app)

    assert len(t.get_result()) == 100


def test_none_default_pipelines(app):

    t = GlobalTestData()

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(callback_func)
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    callback_func.compile()
    simple_producer.flush()
    simple_producer.run()
    run_pipelines(app)

    assert len(t.get_result()) == 100
