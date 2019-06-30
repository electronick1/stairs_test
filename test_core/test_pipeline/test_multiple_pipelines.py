from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple)

from stairs.core.pipeline.data_pipeline import concatenate

from utils import run_pipelines, GlobalTestData


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
    t = GlobalTestData()

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

    assert result['v1']['names'][0] == "oleg"
    assert result['v2']['names'][0] == "oleg"



def test_deep_tree_functions(app):
    t = GlobalTestData()


    @app.consumer()
    def save_globaly(name, result , **kwargs):
        t.save_multiple_items(**{name: result})

    @app.pipeline()
    def p_builder(worker, sentence):
        root_branch = sentence \
            .subscribe_func(lambda sentence: dict(func1="ok"), name='root1') \
            .subscribe_func(lambda sentence: dict(func2="ok"), name='root2')

        return concatenate(
            branch_1=root_branch.subscribe_pipeline(branch_1),
            branch_2=root_branch.subscribe_pipeline(branch_2),
            branch_3=root_branch.subscribe_pipeline(branch_3)
        )

    @app.pipeline()
    def branch_1(work, func1, func2):
        root_branch = concatenate(func1=func1, func2=func2)

        return root_branch \
            .add_value(name='branch_1') \
            .subscribe_func(lambda func1, func2: dict(func1_1="ok"),
                            name='branch_1_1') \
            .subscribe_func(lambda func1, func2: dict(result="branch_1"),
                            name='branch_1_2')\
            .subscribe_consumer(save_globaly)

    @app.pipeline()
    def branch_2(work, func1, func2):
        root_branch = concatenate(func1=func1, func2=func2)
        return root_branch \
            .add_value(name='branch_2') \
            .subscribe_func(lambda func1, func2: dict(func1_1="ok"),
                            name='branch_2_1') \
            .subscribe_func(lambda func1, func2: dict(result="branch_2"),
                            name='branch_2_2')\
            .subscribe_consumer(save_globaly)

    @app.pipeline()
    def branch_3(work, func1, func2):
        root_branch = concatenate(func1=func1, func2=func2)
        return root_branch \
            .add_value(name='branch_3') \
            .subscribe_func(lambda func1, func2: dict(func1_1="ok"),
                            name='branch_3_1') \
            .subscribe_func(lambda func1, func2: dict(result="branch_3"),
                            name='branch_3_2')\
            .subscribe_consumer(save_globaly)


    p_builder.compile()
    branch_1.compile()
    branch_2.compile()
    branch_3.compile()

    p_builder(sentence="Oleg", use_lower=True)
    run_pipelines(app)
    result = t.get_result()

    assert len(result) == 3
    assert list(result[0].keys()) == list(result[0].values())
    assert list(result[1].keys()) == list(result[1].values())
    assert list(result[2].keys()) == list(result[2].values())


def test_deep_tree_functions_of_producers(app):
    t = GlobalTestData()

    @app.consumer()
    def save_globaly(name, result , **kwargs):
        t.save_multiple_items(**{name: result})

    @app.pipeline()
    def p_builder(worker, sentence):
        root_branch = sentence \
            .subscribe_func_as_producer(lambda sentence: [dict(func1="ok"),
                                                         dict(func1="ok"), ],
                                     name='root1') \
            .subscribe_func_as_producer(lambda sentence: [dict(func2="ok"),
                                                          dict(func2="ok"), ],
                                     name='root2')

        return concatenate(
            branch_1=root_branch.subscribe_pipeline(branch_1),
            branch_2=root_branch.subscribe_pipeline(branch_2),
            branch_3=root_branch.subscribe_pipeline(branch_3)
        )

    @app.pipeline()
    def branch_1(work, func1, func2):
        root_branch = concatenate(func1=func1, func2=func2)

        return root_branch \
            .add_value(name='branch_1') \
            .subscribe_func(lambda func1, func2: dict(func1_1="ok"),
                            name='branch_1_1') \
            .subscribe_func(lambda func1, func2: dict(result="branch_1"),
                            name='branch_1_2')\
            .subscribe_consumer(save_globaly)

    @app.pipeline()
    def branch_2(work, func1, func2):
        root_branch = concatenate(func1=func1, func2=func2)
        return root_branch \
            .add_value(name='branch_2') \
            .subscribe_func(lambda func1, func2: dict(func1_1="ok"),
                            name='branch_2_1') \
            .subscribe_func(lambda func1, func2: dict(result="branch_2"),
                            name='branch_2_2')\
            .subscribe_consumer(save_globaly)

    @app.pipeline()
    def branch_3(work, func1, func2):
        root_branch = concatenate(func1=func1, func2=func2)
        return root_branch \
            .add_value(name='branch_3') \
            .subscribe_func(lambda func1, func2: dict(func1_1="ok"),
                            name='branch_3_1') \
            .subscribe_func(lambda func1, func2: dict(result="branch_3"),
                            name='branch_3_2')\
            .subscribe_consumer(save_globaly)


    p_builder.compile()
    branch_1.compile()
    branch_2.compile()
    branch_3.compile()

    p_builder(sentence="Oleg", use_lower=True)
    run_pipelines(app)
    result = t.get_result()

    assert len(result) == 12