from utils import function_as_step, run_pipelines, TestData


def test_simple_producer(app):

    t = TestData()

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

    t = TestData()

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(callback_func)
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    callback_func.compile()
    simple_producer.flush()
    simple_producer.run(custom_callbacks_keys=['callback_func'])
    run_pipelines(app)

    assert len(t.get_result()) == 100


def test_none_default_app_pipelines(app):

    t = TestData()

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer()
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    simple_producer.flush()
    simple_producer.run(custom_callbacks_keys=['callback_func'])
    callback_func.compile()
    run_pipelines(app)
    assert len(t.get_result()) == 100