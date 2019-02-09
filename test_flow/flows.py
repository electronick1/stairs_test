from stairs import Flow, step


class SimpleFlow(Flow):

    def __call__(self, a):
        r = self.start_from(self.initial_step, a=a)

        return {**r.second_step}

    @step(None)
    def second_step(self, a):
        print("second")
        return dict(mul_two=a * 2)

    @step(second_step)
    def initial_step(self, a):
        print("initial")
        return dict(a=a)


class FlowMultipleBranches(Flow):

    def __call__(self, a):
        r = self.start_from(self.initial_step, a=a)

        return {**r.step_left, **r.step_right}

    @step(None)
    def step_left(self, a):
        return dict(mul_two=a * 2)

    @step(None)
    def step_right(self, a):
        return dict(mul_three=a * 3)

    @step(step_left, step_right)
    def initial_step(self, a):
        return dict(a=a)


class FlowWithSaveResult(Flow):

    def __call__(self, a):
        r = self.start_from(self.initial_step, a=a)

        return {**r.third_step, **r.second_step}

    @step(None)
    def third_step(self, a):
        return dict(mul_three=a * 3)

    @step(third_step, save_result=True)
    def second_step(self, a):
        return dict(mul_two=a * 2)

    @step(second_step)
    def initial_step(self, a):
        return dict(a=a)


class FlowReconnect(SimpleFlow):

    def __call__(self, a):
        r = self.start_from(self.initial_step, a=a)

        return {**r.third_step, **r.second_step}

    def __reconnect__(self):
        self.second_step.set_next(self.third_step)
        self.second_step.set(save_result=True)

    @step(None)
    def third_step(self, a):
        return dict(mul_three=a * 3)