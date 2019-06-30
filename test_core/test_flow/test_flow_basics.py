from test_core.test_flow import flows


def test_branches_flow(simple_pipeline):
    f = flows.FlowMultipleBranches()
    f.compile(simple_pipeline)
    r = f(a=1)

    assert r['mul_two'] == 2
    assert r['mul_three'] == 3


def test_save_result(simple_pipeline):
    f = flows.FlowWithSaveResult()
    f.compile(simple_pipeline)
    r = f(a=1)

    assert r['mul_two'] == 2
    assert r['mul_three'] == 3


def test_flow_reconnect(simple_pipeline):
    f = flows.FlowReconnect()
    f.compile(simple_pipeline)
    r = f(a=1)

    assert r['mul_two_patched'] == 2
    assert r['mul_three'] == 3


def test_hard_inheritance(simple_pipeline):
    f = flows.HardInheritance()
    f.compile(simple_pipeline)
    r = f(a=1)

    assert r['mul_two_patched'] == 2
    assert r['mul_three'] == 3
