from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple,
                                                NameExtractionWorkersOneWayFlow)

from stairs.core.worker import Worker
from stairs.core.worker.data_pipeline import concatenate


def test_multiple_vars(app):
    def p_builder(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data\
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)

        return data_with_name

    _worker = Worker(
        app=app,
        pipeline_builder=p_builder,
        worker_config={}
    )

    def p_builder_general(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        return data.subscribe_pipeline(_worker)\
                   .subscribe_flow(NameExtractionFlowMultiple(),
                                   as_worker=False)

    worker = Worker(
        app=app,
        pipeline_builder=p_builder_general,
        worker_config={}
    )

    result = worker(sentence="Oleg",
                    use_lower=True)

    assert result['names'][0] == "Oleg"

