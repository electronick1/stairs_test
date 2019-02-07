from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple)

from utils import run_pipelines


def test_make_add_value_with_flow(app):
    result = dict()

    def save_result(**data):
        result.update(data)
        return data

    @app.pipeline(config=dict(path='123'))
    def p_builder2(worker, sentence):
        return sentence\
            .make(data=sentence)\
            .add_value(path=worker.config['path'])\
            .subscribe_flow(NameExtractionOneWayFlow(use_lower=True))\
            .subscribe_func(save_result)

    @app.pipeline()
    def p_builder_general2(worker, sentence):
        return sentence.subscribe_pipeline(p_builder2, config=dict(path='321'))

    p_builder_general2(sentence="Oleg")
    run_pipelines(app)

    assert list(result.keys()) == ['sentence', 'path', 'names']
    assert result['names'][0] == "oleg"
    assert result['path'] == '321'
