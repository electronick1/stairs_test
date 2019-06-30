from utils import run_pipelines


def test_connected_pipelines(app):
    @app.consumer_iter()
    def save_globaly(func1, func2, **kwargs):
        return dict(func1=func1, func2=func2)

    @app.pipeline()
    def p_builder(worker, sentence):
        return sentence \
            .subscribe_func(lambda sentence: dict(func1="ok"), name='root1') \
            .subscribe_func_as_producer(
            lambda sentence: [dict(func2="ok"), dict(func2="ok")], name='root2') \
            .subscribe_consumer(save_globaly)

    p_builder.compile()
    p_builder(sentence="Oleg")
    run_pipelines(app)
    it = save_globaly.iter(die_when_empty=True)
    result = next(it)

    assert len(result) == 2
