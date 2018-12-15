import time
import json
from stepist.flow import session
from uuid import uuid4
from stairs.tests.utils import run_stepist_workers

from stairs.tests.flows.name_extraction import (NameExtractionOneWayFlow,
                                                NameExtractionFlowMultiple)


def test_pipeline_one_flow(worker):

    worker.pipeline.add_flow(NameExtractionOneWayFlow(), name=uuid4())
    worker.compile()

    sentence = "Hello from Oleg."

    result = worker(sentence=sentence)

    assert len(result['names']) == 1
    assert result['names'][0] == "Oleg"


def test_pipeline_multiple_one_app_flows(worker):
    worker.pipeline.add_flow(NameExtractionOneWayFlow(), name=uuid4())
    worker.pipeline.add_flow(NameExtractionFlowMultiple(use_lower=True),
                             name=uuid4())
    worker.compile()

    sentence = " Hello from Oleg. "
    result = worker(sentence=sentence)

    assert len(result['names']) == 1
    assert result['names'][0] == "oleg"
    assert result['clean'] == sentence.strip()


def test_pipeline_and_additional_worker(worker, worker_flow_multiple, redis):
    worker.pipeline.add_flow(NameExtractionOneWayFlow(), name=uuid4())
    worker.pipeline.add_worker(worker_flow_multiple, name=uuid4())
    worker.compile()

    sentence = " Hello from Oleg. "
    worker(sentence=sentence)

    run_stepist_workers()
    results = redis.get("test")
    assert results
    result = json.loads(results)

    assert len(result['names']) == 1
    assert result['names'][0] == "oleg"
