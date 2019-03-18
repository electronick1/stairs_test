from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple)

from utils import run_pipelines, check_keys, TestData


def test_make_add_value_with_flow(app):
    t = TestData()

    @app.pipeline(config=dict(path='123'))
    def p_builder3(worker, sentence2):
        return sentence2\
            .rename(sentence=sentence2)\
            .add_value(path=worker.config['path'])\
            .subscribe_flow(NameExtractionOneWayFlow(use_lower=True))\
            .subscribe_func(t.save_one_item)

    @app.pipeline()
    def p_builder_general3(worker, sentence2):
        return sentence2.subscribe_pipeline(p_builder3, config=dict(path='321'))

    p_builder3.compile()
    p_builder_general3.compile()
    p_builder_general3(sentence2="Oleg")
    run_pipelines(app)

    result = t.get_result()
    assert check_keys(result.keys(), ['sentence', 'path', 'names'])
    assert result['names'][0] == "oleg"
    assert result['path'] == '321'
