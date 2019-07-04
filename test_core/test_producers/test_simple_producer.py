from stairs import producer_signals

from utils import run_pipelines, run_pipelines_process, GlobalTestData


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


def test_multiple_pipelines(app):
    t = GlobalTestData()

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.pipeline()
    def callback_func2(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(callback_func, callback_func2)
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    callback_func.compile()
    callback_func2.compile()

    simple_producer.flush()
    simple_producer.run()

    run_pipelines(app)

    assert len(t.get_result()) == 200


def test_repeat_producer(app):

    t = GlobalTestData()

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(callback_func,
                  repeat_on_signal=producer_signals.on_pipeline_empty,
                  repeat_times=2)
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    callback_func.compile()

    process = run_pipelines_process(app)

    simple_producer.flush()
    simple_producer()

    process.terminate()

    # 200 because we repeat simple_producer two times + first initial run
    assert len(t.get_result()) == 300
