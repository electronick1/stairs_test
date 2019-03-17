import utils
from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple)

from stairs.core.worker.data_pipeline import concatenate


def test_make_with_flow(app):
    @app.pipeline()
    def p_builder(worker, sentence2):
        return sentence2\
            .rename(sentence=sentence2)\
            .apply_flow(NameExtractionOneWayFlow(use_lower=True))

    p_builder.compile()
    result = p_builder(sentence2="Oleg")

    assert list(result.keys()) == ['names']
    assert result['names'][0] == "oleg"


def test_make_and_func(app):
    @app.pipeline()
    def p_builder(worker, sentence):
        return sentence\
            .rename(data=sentence)\
            .subscribe_func(lambda data: dict(result="ok"), name="result_ok")

    p_builder.compile()
    result = p_builder(sentence="Oleg")

    assert result['result'] == "ok"


def test_make_add_value_with_flow(app):
    @app.pipeline()
    def p_builder(worker, sentence2):
        return sentence2\
            .rename(sentence=sentence2)\
            .add_value(path='123')\
            .subscribe_flow(NameExtractionOneWayFlow(use_lower=True))

    p_builder.compile()
    result = p_builder(sentence2="Oleg")

    assert utils.check_keys(result.keys(), ['names', 'sentence', 'path'])
    assert result['names'][0] == "oleg"
    assert result['path'] == '123'


def test_one_way_flow_using_stepist(app):
    @app.pipeline()
    def p_builder(worker, sentence):
        data_with_name = sentence\
            .apply_flow(NameExtractionOneWayFlow(use_lower=True))
        return data_with_name

    p_builder.compile()
    result = p_builder(sentence="Oleg")

    assert list(result.keys()) == ['names']
    assert result['names'][0] == "oleg"


def test_multiple_vars(app):
    @app.pipeline()
    def p_builder(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data\
            .subscribe_flow(NameExtractionOneWayFlow(), as_worker=False)\
            .apply_flow(NameExtractionFlowMultiple(), as_worker=False)

        return data_with_name

    p_builder.compile()
    result = p_builder(sentence="Oleg", use_lower=True)

    assert utils.check_keys(result.keys(), ['names', 'sentence'])
    assert result['names'][0] == "Oleg"


def test_long_concatenate(app):
    @app.pipeline()
    def p_builder(worker, sentence, use_lower):
        data_with_name = sentence \
            .apply_flow(NameExtractionOneWayFlow(), as_worker=False)

        data = concatenate(sentence=data_with_name.get('names'),
                           use_lower=use_lower)

        return data

    p_builder.compile()
    result = p_builder(sentence="Oleg", use_lower=True)

    assert utils.check_keys(result.keys(), ['use_lower', 'sentence'])
    assert result['sentence'][0] == "Oleg"


def test_multiple_concatenate(app):
    @app.pipeline()
    def p_builder(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data \
            .apply_flow(NameExtractionOneWayFlow(), as_worker=False)

        data = concatenate(sentence=data_with_name.get('names'),
                           use_lower2=use_lower)
        return data

    p_builder.compile()
    result = p_builder(sentence="Oleg", use_lower=True)

    assert utils.check_keys(result.keys(), ['use_lower2', 'sentence'])
    assert result['sentence'][0] == "Oleg"


def test_functions(app):
    global_data = dict()

    @app.consumer()
    def save_globaly(sentence, result, **kwargs):
        global_data["result"] = result['asd']

    @app.pipeline()
    def p_builder(worker, sentence):
        branch_1 = sentence \
            .subscribe_func(lambda sentence: dict(s2="ok"), name='s2') \
            .subscribe_func_as_producer(lambda sentence: [dict(s3="ok")], name='s3') \
            .apply_func(lambda sentence: dict(asd="applied"))

        return concatenate(sentence=sentence, result=branch_1) \
            .subscribe_consumer(save_globaly)

    p_builder.compile()
    result = p_builder(sentence="Oleg")

    assert global_data['result'] == "applied"
