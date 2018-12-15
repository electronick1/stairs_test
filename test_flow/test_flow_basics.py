import json

from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                      NameExtractionFlowMultiple,
                                                      NameExtractionWorkersOneWayFlow)

from stairs.core.worker import Worker
from stairs.tests.utils import run_stepist_workers
from stairs.core.worker.data_pipeline import concatenate


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
    print(result)
    assert result['names'][0] == "oleg"


def test_multiple_vars(app):
    def p_builder2(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data\
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)
        return data_with_name

    worker = Worker(
        app=app,
        pipeline_builder=p_builder2,
        worker_config={}
    )
    result = worker(sentence="Oleg",
                    use_lower=True)

    #assert list(result.keys()) == ['names']
    assert result['names'][0] == "Oleg"


def test_long_concatenate(app):
    def p_builder3(worker, sentence, use_lower):
        data_with_name = sentence \
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)

        data = concatenate(sentence=data_with_name.get('names'),
                           use_lower=use_lower)

        return data

    worker = Worker(
        app=app,
        pipeline_builder=p_builder3,
        worker_config={}
    )
    result = worker(sentence="Oleg",
                    use_lower=True)

    #assert list(result.keys()) == ['use_lower', 'sentence']
    assert result['sentence'][0] == "Oleg"


def test_multiple_concatenate(app):
    def p_builder4(worker, sentence, use_lower):
        p_component = sentence.data_pipeline.get_last_item().p_component
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data \
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)

        data = concatenate(sentence=data_with_name.get('names'),
                           use_lower2=use_lower)
        print("CONTEXT:")
        for c in p_component._context_list:
            print(c.to_p_component.id, c.transformation)

        print("GRAPH:", str(data.data_pipeline.graph))
        return data

    worker = Worker(
        app=app,
        pipeline_builder=p_builder4,
        worker_config={}
    )

    result = worker(sentence="Oleg",
                    use_lower=True)
    print(result)

    #assert list(result.keys()) == ['use_lower2', 'sentence']
    assert result['sentence'][0] == "Oleg"

#
#
# def test_one_way_flow_using_stepist_global(worker):
#
#     worker.pipeline.add_flow(NameExtractionOneWayFlow(use_lower=True))
#     worker.compile()
#
#     sentence = "Hello from Oleg."
#     result = worker(sentence=sentence)
#
#     assert len(result['names']) == 1
#     assert result['names'][0] == "oleg"
#
#
# def test_multiple_way_flow(worker):
#
#     worker.pipeline.add_flow(NameExtractionFlowMultiple(use_lower=True))
#     worker.compile()
#
#     sentence = " Hello from Oleg. "
#     result = worker(sentence=sentence)
#
#     assert len(result['names']) == 1
#     assert result['names'][0] == "oleg"
#     assert result['clean'] == sentence.strip()
#
#
# def test_one_way_workers_flow_using_stepist(worker, redis):
#
#     worker.pipeline.add_flow(NameExtractionWorkersOneWayFlow(redis=redis))
#     worker.compile()
#
#     sentence = "Hello from Oleg."
#     worker(sentence=sentence)
#
#     run_stepist_workers()
#     results = redis.get("test")
#     assert results
#     result = json.loads(results)
#
#     assert len(result['names']) == 1
#     assert result['names'][0] == "Oleg"
