from utils import function_as_step, run_pipelines


def test_worker_producer(app):
    producer_output = []

    @app.consumer()
    def callback_func(x):
        producer_output.append(x)

    @app.pipeline()
    def simple_pipeline(pipeline, x):
        return x.subscribe_consumer(callback_func)

    @app.producer(simple_pipeline)
    def batch_handler(x):
        yield dict(x=x)

    @app.batch_producer(batch_handler)
    def worker_producer():
        for i in range(10):
            # here we should yield batch of data
            yield dict(x=1)

    worker_producer()
    try:
        worker_producer()
        batch_handler.run_jobs_processor(die_when_empty=True)
    except SystemExit:
        pass

    simple_pipeline.compile()
    run_pipelines(app)

    assert len(producer_output) == 20  # 10 batches with 2 jobs
