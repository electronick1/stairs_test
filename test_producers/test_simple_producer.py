from utils import function_as_step, run_pipelines


def test_simple_producer(app):
    producer_output = []

    def callback_func(x):
        producer_output.append(x)

    @app.producer(function_as_step(callback_func))
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    simple_producer.run()

    assert len(producer_output) == 100


def test_none_default_pipelines(app):
    producer_output = []

    def callback_func(x):
        producer_output.append(x)

    @app.producer(custom=[function_as_step(callback_func)])
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    simple_producer.run(custom_callbacks_keys=['input'])

    assert len(producer_output) == 100


def test_none_default_app_pipelines(app):
    producer_output = []

    @app.consumer()
    def consumer(x):
        producer_output.append(x)

    @app.pipeline()
    def callback_func(pipeline, x):
        return x.subscribe_consumer(consumer)

    @app.producer()
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    simple_producer.flush()
    simple_producer.run(custom_callbacks_keys=['callback_func'])
    callback_func.compile()
    run_pipelines(app)
    assert len(producer_output) == 100