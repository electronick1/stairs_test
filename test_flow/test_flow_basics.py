import json

from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple,
                                                NameExtractionWorkersOneWayFlow)

from stairs.core.worker import Worker
from stairs.core.worker.data_pipeline import concatenate


def test_make_with_flow(app):

    def p_builder(worker, sentence):
        return sentence\
            .make(data=sentence)\
            .subscribe_flow(NameExtractionOneWayFlow(use_lower=True))

    worker = Worker(
        app=app,
        pipeline_builder=p_builder,
        worker_config={}
    )
    result = worker(sentence="Oleg")

    assert list(result.keys()) == ['names']
    assert result['names'][0] == "oleg"


def test_one_way_flow_using_stepist(app):
    def p_builder(worker, sentence):
        data_with_name = sentence\
            .subscribe_flow(NameExtractionOneWayFlow(use_lower=True))
        return data_with_name

    worker = Worker(
        app=app,
        pipeline_builder=p_builder,
        worker_config={}
    )
    result = worker(sentence="Oleg")

    assert list(result.keys()) == ['names']
    assert result['names'][0] == "oleg"


def test_multiple_vars(app):
    def p_builder(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data\
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)\
            .subscribe_flow(NameExtractionFlowMultiple(),
                            as_worker=False)
        return data_with_name

    worker = Worker(
        app=app,
        pipeline_builder=p_builder,
        worker_config={}
    )
    result = worker(sentence="Oleg",
                    use_lower=True)

    assert list(result.keys()) == ['names', 'sentence']
    assert result['names'][0] == "Oleg"


def test_long_concatenate(app):
    def p_builder(worker, sentence, use_lower):
        data_with_name = sentence \
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)

        data = concatenate(sentence=data_with_name.get('names'),
                           use_lower=use_lower)

        return data

    worker = Worker(
        app=app,
        pipeline_builder=p_builder,
        worker_config={}
    )
    result = worker(sentence="Oleg",
                    use_lower=True)

    assert list(result.keys()) == ['use_lower', 'sentence']
    assert result['sentence'][0] == "Oleg"


def test_multiple_concatenate(app):
    def p_builder(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data \
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)

        data = concatenate(sentence=data_with_name.get('names'),
                           use_lower2=use_lower)
        return data

    worker = Worker(
        app=app,
        pipeline_builder=p_builder,
        worker_config={}
    )

    result = worker(sentence="Oleg",
                    use_lower=True)

    assert list(result.keys()) == ['use_lower2', 'sentence']
    assert result['sentence'][0] == "Oleg"

