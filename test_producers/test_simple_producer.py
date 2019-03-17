from utils import function_as_step


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

    @app.producer(none_default=function_as_step(callback_func))
    def simple_producer():
        for i in range(100):
            yield dict(x=1)

    simple_producer.run(custom_callbacks_keys=['none_default'])

    assert len(producer_output) == 100
