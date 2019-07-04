from stairs import producer_signals

from utils import function_as_step, run_pipelines, GlobalTestData
from stairs.core.producer import run_jobs_processor


def test_simple_producer_redirect(app):

    t = GlobalTestData()

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(callback_func)
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    @app.producer_redirect(simple_producer, callback_func)
    def simple_producer2(data):
        return data

    callback_func.compile()
    simple_producer2.flush()
    simple_producer2.run()
    run_pipelines(app)

    assert len(t.get_result()) == 100


def test_batch_producer_redirect(app):
    t = GlobalTestData()

    @app.pipeline()
    def simple_pipeline(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(simple_pipeline)
    def batch_handler(x):
        yield dict(x=x)

    @app.producer_redirect(batch_handler, simple_pipeline)
    def simple_producer2(data):
        return data

    @app.batch_producer(simple_producer2)
    def worker_producer():
        for i in range(10):
            # here we should yield batch of data
            yield dict(x=1)

    simple_pipeline.compile()
    worker_producer()
    try:
        worker_producer()
        run_jobs_processor(app.project,
                           [simple_producer2],
                           die_when_empty=True)
    except SystemExit:
        pass

    run_pipelines(app)

    assert len(t.get_result()) == 20  # 10 batches with 2 jobs