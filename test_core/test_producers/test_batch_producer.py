from utils import function_as_step, run_pipelines, GlobalTestData
from stairs.core.producer import run_jobs_processor


def test_worker_producer(app):
    t = GlobalTestData()

    @app.pipeline()
    def simple_pipeline(pipeline, x):
        return x.subscribe_func(t.save_multiple_items)

    @app.producer(simple_pipeline)
    def batch_handler(x):
        yield dict(x=x)

    @app.batch_producer(batch_handler)
    def worker_producer():
        for i in range(10):
            # here we should yield batch of data
            yield dict(x=1)

    simple_pipeline.compile()
    worker_producer()
    try:
        worker_producer()
        run_jobs_processor(app.project,
                           [batch_handler],
                           die_when_empty=True)
    except SystemExit:
        pass

    run_pipelines(app)

    assert len(t.get_result()) == 20  # 10 batches with 2 jobs
