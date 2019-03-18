from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple)

from stairs.core.worker.data_pipeline import concatenate

from utils import run_pipelines, TestData


def test_connected_pipelines(app):
    @app.pipeline()
    def p_builder(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data \
            .subscribe_flow(NameExtractionOneWayFlow(),
                            as_worker=False)

        return data_with_name

    @app.pipeline()
    def p_builder_general(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        return data.subscribe_pipeline(p_builder)\
                   .subscribe_flow(NameExtractionFlowMultiple(),
                                   as_worker=False)

    p_builder.compile()
    p_builder_general.compile()
    p_builder_general(sentence="Oleg", use_lower=True)
    run_pipelines(app)


def test_connected_pipelines_multiple(app):
    t = TestData()

    @app.pipeline()
    def p_builder(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        data_with_name = data\
            .subscribe_flow(NameExtractionOneWayFlow())

        return data_with_name

    @app.pipeline()
    def p_builder_general(worker, sentence, use_lower):
        data = concatenate(sentence=sentence,
                           use_lower=use_lower)

        v1 = data.subscribe_pipeline(p_builder)\
                 .subscribe_flow(NameExtractionFlowMultiple(use_lower=use_lower))
        v2 = data.subscribe_pipeline(p_builder)\
                 .subscribe_flow(NameExtractionFlowMultiple(use_lower=use_lower))

        return concatenate(v1=v1, v2=v2).subscribe_func(t.save_one_item)

    p_builder.compile()
    p_builder_general.compile()
    p_builder_general(sentence="Oleg", use_lower=True)
    run_pipelines(app)
    result = t.get_result()
    print(result)
    assert result['v1']['names'][0] == "oleg"
    assert result['v2']['names'][0] == "oleg"
