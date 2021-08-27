import asyncio
from time import sleep

import pytest
import ray
from ray.util.queue import Queue

from magda.pipeline.parallel.parallel_pipeline import ParallelPipeline
from magda.module.module import Module
from magda.decorators.accepting import accept
from magda.decorators.finalizing import finalize
from magda.decorators.exposing import expose
from magda.exceptions import ClosedPipelineException


@accept(self=True)
@finalize
class ModuleA(Module.Runtime):
    def run(self, *args, **kwargs):
        sleep(0.03)


@accept(ModuleA, self=True)
@finalize
class ModuleB(Module.Runtime):
    def run(self, *args, **kwargs):
        sleep(0.02)


@accept(ModuleA, ModuleB)
@finalize
class ModuleC(Module.Runtime):
    def run(self, request, *args, **kwargs):
        sleep(0.01)
        return f'{request}:C'


@accept(self=True)
@finalize
class ModuleTestTeardown(Module.Runtime):
    async def bootstrap(self):
        queue: Queue = self.context['up']
        await queue.put_async(self.name)

    def run(self, **kwargs):
        return 'run'

    async def teardown(self):
        queue: Queue = self.context['down']
        await queue.put_async(self.name)


@pytest.fixture(scope='class')
def ray_context():
    ray.init(num_cpus=1)
    yield None
    ray.shutdown()


class ModuleParameters:
    @staticmethod
    def single_module_group():
        return [ModuleB('m1', group='g1')]

    @staticmethod
    def two_modules_one_groups():
        m1 = ModuleA('m1', group='g1')
        m2 = ModuleB('m2', group='g1').depends_on(m1)
        return [m1, m2]

    @staticmethod
    def two_modules_two_groups():
        m1 = ModuleA('m1', group='g1')
        m2 = ModuleB('m2', group='g2').depends_on(m1)
        return [m1, m2]

    @staticmethod
    def four_modules_two_groups():
        m1 = ModuleA('m1', group='g1')
        m2 = ModuleA('m2', group='g1').depends_on(m1)
        m3 = ModuleB('m3', group='g2').depends_on(m2)
        m4 = ModuleC('m4', group='g2').depends_on(m3)
        return [m1, m2, m3, m4]


class TestParallelPipelineSerial:

    @pytest.mark.asyncio
    async def test_can_create(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(ModuleA('m1', group='g1'))
        pipeline = await builder.build()
        assert isinstance(pipeline, ParallelPipeline.Runtime)
        assert len(pipeline.groups) == 1
        assert set([g.name for g in pipeline.groups]) == set(['g1'])

    @pytest.mark.asyncio
    async def test_can_create_multiple_groups(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(ModuleB('m2', group='g2').depends_on(builder.get_module('m1')))
        pipeline = await builder.build()
        assert isinstance(pipeline, ParallelPipeline.Runtime)
        assert len(pipeline.groups) == 2
        assert set([g.name for g in pipeline.groups]) == set(['g1', 'g2'])

    @pytest.mark.asyncio
    async def test_should_call_hooks_in_groups(self, ray_context):
        builder = ParallelPipeline()

        callable_1_counter = Queue()
        callable_2_counter = Queue()

        def callable_1():
            callable_1_counter.put(1)

        def callable_2():
            callable_2_counter.put(2)

        builder.add_group(builder.Group('g1', after_created=[callable_1, callable_2]))
        builder.add_group(builder.Group('g2', after_created=[callable_2]))
        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(ModuleB('m2', group='g2').depends_on(builder.get_module('m1')))
        pipeline = await builder.build()
        assert isinstance(pipeline, ParallelPipeline.Runtime)
        assert len(pipeline.groups) == 2
        assert set([g.name for g in pipeline.groups]) == set(['g1', 'g2'])
        assert 1 == callable_1_counter.qsize()
        assert 2 == callable_2_counter.qsize()

    @pytest.mark.asyncio
    async def test_should_fail_on_adding_to_group_hooks_with_incorrect_type(self, ray_context):
        builder = ParallelPipeline()

        with pytest.raises(Exception):
            builder.add_group(builder.Group('g1', after_created="test"))

    @pytest.mark.asyncio
    async def test_should_fail_on_adding_to_group_non_callable_hooks(self, ray_context):
        builder = ParallelPipeline()

        def callable_1():
            pass

        with pytest.raises(Exception):
            builder.add_group(builder.Group('g1', after_created=[callable_1, "test"]))

    @pytest.mark.asyncio
    async def test_can_create_multiple_dependent_groups(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(ModuleB('m2', group='g2').depends_on(builder.get_module('m1')))
        builder.add_module(ModuleC('m3', group='g3').depends_on(builder.get_module('m2')))
        pipeline = await builder.build()
        assert isinstance(pipeline, ParallelPipeline.Runtime)
        assert len(pipeline.groups) == 3
        assert set([g.name for g in pipeline.groups]) == set(['g1', 'g2', 'g3'])
        g1 = next((g for g in pipeline.groups if g.name == 'g1'))
        g2 = next((g for g in pipeline.groups if g.name == 'g2'))
        g3 = next((g for g in pipeline.groups if g.name == 'g3'))
        assert g1.dependencies == set([])
        assert g2.dependencies == set(['g1'])
        assert g3.dependencies == set(['g2'])

    @pytest.mark.asyncio
    async def test_cannot_create_empty_group(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(ModuleB('m2', group='g2').depends_on(builder.get_module('m1')))
        builder.add_module(ModuleC('m3', group='g3').depends_on(builder.get_module('m2')))
        builder.add_group(builder.Group('g4'))
        with pytest.raises(Exception):
            await builder.build()

    @pytest.mark.parametrize('modules', [
        [ModuleB('m2')],
        [ModuleB('m2', group='g1'), ModuleB('m2')],
        [ModuleB('m2'), ModuleB('m2', group='g1')],
        [ModuleB('m2'), ModuleB('m2', group='g1'), ModuleB('m2')],
        [ModuleB('m2'), ModuleB('m2', group='g1'), ModuleB('m2', group='g1')],
    ])
    @pytest.mark.asyncio
    async def test_cannot_create_without_group(self, modules, ray_context):
        builder = ParallelPipeline()
        for module in modules:
            builder.add_module(module)
        with pytest.raises(Exception):
            await builder.build()

    @pytest.mark.parametrize('modules', [
        ModuleParameters.single_module_group(),
        ModuleParameters.two_modules_one_groups(),
        ModuleParameters.two_modules_two_groups(),
        ModuleParameters.four_modules_two_groups()
    ])
    @pytest.mark.asyncio
    async def test_listing_group_modules(self, modules, ray_context):
        builder = ParallelPipeline()
        for module in modules:
            builder.add_module(module)
        pipeline = await builder.build()

        names = set([m.name for m in modules])
        pipeline_names = set([m.name for m in pipeline.modules])
        assert names == pipeline_names

        def prepare_comparison(group) -> bool:
            expected = set([
                m.name
                for m in modules
                if m.group == group.name
            ])
            current = set([m.name for m in group.modules])
            return expected == current

        compared_modules = [prepare_comparison(group) for group in pipeline.groups]
        assert all(compared_modules)

    @pytest.mark.asyncio
    async def test_can_pass_context(self, ray_context):
        context = dict(ctx='MAGDA')
        tag = 'output'

        @expose(tag)
        @finalize
        class ModuleWithContext(Module.Runtime):
            def run(self, *args, **kwargs):
                return self.context

        builder = ParallelPipeline()
        builder.add_module(ModuleWithContext('m1', group='g1'))
        pipeline = await builder.build(context)
        assert pipeline.context == context
        results, _ = await pipeline.run()
        assert results[tag] == context

    @pytest.mark.asyncio
    async def test_can_pass_shared_parameters(self, ray_context):
        context = dict(ctx='MAGDA')
        shared_parameters = {'shared_param1': 1}
        tag = 'output'

        @expose(tag)
        @finalize
        class ModuleWithSharedParams(Module.Runtime):
            def run(self, *args, **kwargs):
                return self.shared_parameters

        builder = ParallelPipeline()
        builder.add_module(ModuleWithSharedParams('m1', group='g1'))
        pipeline = await builder.build(context, shared_parameters)
        assert pipeline.shared_parameters == shared_parameters
        results, _ = await pipeline.run()
        assert results[tag] == shared_parameters

    @pytest.mark.asyncio
    async def test_build_run_with_defaults(self, ray_context):
        tag = 'output_tag'

        @expose(tag)
        @finalize
        class MockModule(Module.Runtime):
            def run(self, *args, **kwargs):
                sleep(0.01)
                return 'output_result'

        builder = ParallelPipeline()
        builder.add_module(MockModule('m1', group='g1'))
        pipeline = await builder.build()
        results, _ = await pipeline.run()

        assert 'output_tag' in results
        assert results['output_tag'] == 'output_result'


class TestParallelPipeline:
    @pytest.mark.asyncio
    async def test_request(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(ModuleB('m2', group='g2'))
        builder.add_module(
            ModuleB('m3', group='g2')
            .depends_on(builder.get_module('m2'))
        )
        builder.add_module(
            ModuleC('m4', group='g3')
            .depends_on(builder.get_module('m3'))
            .depends_on(builder.get_module('m1'))
            .expose_result('final')
        )
        pipeline = await builder.build()
        result, _ = await pipeline.run('R1')
        assert result['final'] == 'R1:C'

    @pytest.mark.asyncio
    async def test_request_checking_dependencies(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(ModuleA('m0', group='g0'))
        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(
            ModuleB('m2', group='g2')
            .depends_on(builder.get_module('m0'))
        )
        builder.add_module(
            ModuleB('m3', group='g2')
            .depends_on(builder.get_module('m2'))
        )
        builder.add_module(
            ModuleC('m4', group='g3')
            .depends_on(builder.get_module('m3'))
            .depends_on(builder.get_module('m1'))
            .expose_result('final')
        )
        pipeline = await builder.build()
        result, _ = await pipeline.run('R1')
        assert result['final'] == 'R1:C'

    @pytest.mark.asyncio
    async def test_parallel_requests(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(ModuleB('m2', group='g2'))
        builder.add_module(
            ModuleB('m3', group='g2')
            .depends_on(builder.get_module('m2'))
        )
        builder.add_module(
            ModuleC('m4', group='g3')
            .depends_on(builder.get_module('m3'))
            .depends_on(builder.get_module('m1'))
            .expose_result('final')
        )
        pipeline = await builder.build()
        results = await asyncio.gather(
            asyncio.create_task(pipeline.run('R1')),
            asyncio.create_task(pipeline.run('R2')),
            asyncio.create_task(pipeline.run('R3')),
        )

        outputs = set([r[0]['final'] for r in results])
        assert outputs == set(['R1:C', 'R2:C', 'R3:C'])

    @pytest.mark.asyncio
    async def test_close_pipeline(self, ray_context):
        # m1:g1 ----------> \
        # m2:g2 -> m3:g2 -> m4:g3
        builder = ParallelPipeline()
        builder.add_module(ModuleTestTeardown('m1', group='g1'))
        builder.add_module(ModuleTestTeardown('m2', group='g2'))
        builder.add_module(
            ModuleTestTeardown('m3', group='g2')
            .depends_on(builder.get_module('m2'))
        )
        builder.add_module(
            ModuleTestTeardown('m4', group='g3')
            .depends_on(builder.get_module('m3'))
            .depends_on(builder.get_module('m1'))
            .expose_result('final')
        )

        up, down = Queue(), Queue()
        pipeline = await builder.build(dict(up=up, down=down))
        assert(up.size() == len(pipeline.modules))
        assert(down.size() == 0)

        await pipeline.run()
        await pipeline.close()
        assert pipeline.closed
        assert(up.size() == len(pipeline.modules))
        assert(down.size() == len(pipeline.modules))

        with pytest.raises(ClosedPipelineException):
            await pipeline.run()
        with pytest.raises(ClosedPipelineException):
            await pipeline.process()

    @pytest.mark.asyncio
    async def test_failed_pipeline_run_not_affecting_other_runs(self, ray_context):
        builder = ParallelPipeline()

        @finalize
        class ModuleA(Module.Runtime):
            def run(self, request, *args, **kwargs):
                sleep(0.01)
                return 'a'

        @accept(ModuleA)
        @finalize
        class ModuleError(Module.Runtime):
            def run(self, request, *args, **kwargs):
                if request == 'error':
                    raise Exception('Whoops')

        @accept(ModuleError)
        @expose()
        @finalize
        class ModuleC(Module.Runtime):
            def run(self, request, *args, **kwargs):
                sleep(0.01)
                return 'c'

        builder.add_module(ModuleA('m1', group='g1'))
        builder.add_module(ModuleError('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(ModuleError('m3', group='g2'))
        builder.add_module(
            ModuleC('m4', group='g3')
            .depends_on(builder.get_module('m2'))
            .depends_on(builder.get_module('m3'))
        )

        pipeline = await builder.build()
        results = await asyncio.gather(*[
            asyncio.create_task(pipeline.run(request_value))
            for request_value in ['error', 'R2']
        ])
        result1, error1 = results[0]
        result2, error2 = results[1]

        assert result1 is None
        assert isinstance(error1, Exception)
        assert str(error1) == 'Whoops'
        assert 'm4' in result2
        assert result2['m4'] == 'c'
        assert error2 is None


class TestStatefulParallelPipeline:

    @expose()
    @finalize
    class ModuleA(Module.Runtime):
        def run(self, *args, **kwargs):
            return 'a'

    @expose()
    @accept(ModuleA, self=True)
    @finalize
    class ModuleB(Module.Runtime):
        def run(self, *args, **kwargs):
            return 'b'

    @expose('agg')
    @accept(ModuleA, ModuleB)
    @finalize
    class ModuleAgg(Module.Aggregate):
        def aggregate(self, data, **kwargs):
            """ Request Aggregate """
            self.add_data([(d.name, d.result) for d in data.collection])
            return self.state

    @expose()
    @accept(ModuleB, ModuleAgg, self=True)
    @finalize
    class ModuleC(Module.Runtime):
        def run(self, data, *args, **kwargs):
            if data.has('agg'):
                return data.get('agg') * 2
            else:
                return 'c'

    @pytest.mark.asyncio
    async def test_simple_pipeline_multiple_run_and_agg(self, ray_context):
        # m1:g1 ----------> \
        # m2:g2 -> m3:g2 -> m4:g3(agg)
        builder = ParallelPipeline()
        builder.add_module(self.ModuleA('m1', group='g1'))
        builder.add_module(self.ModuleB('m2', group='g2'))
        builder.add_module(
            self.ModuleB('m3', group='g2')
            .depends_on(builder.get_module('m2'))
        )
        builder.add_module(
            self.ModuleAgg('m4', group='g3')
            .depends_on(builder.get_module('m3'))
            .depends_on(builder.get_module('m1'))
            .expose_result('final')
        )

        pipeline = await builder.build()
        r1, _ = await pipeline.run('R1')
        r2, _ = await pipeline.run('R2')
        agg1, _ = await pipeline.process('A1')
        r3, _ = await pipeline.run('R3')
        agg2, _ = await pipeline.process('A1')

        assert len(r1) == len(r2) == len(r3) == 3
        assert len(agg1['final']) == 2
        assert len(agg2['final']) == 1

    @pytest.mark.asyncio
    async def test_stateful_pipeline_mixed(self, ray_context):
        # m1:g1 -> m2:g1 -> \
        # m3:g2 -> m4:g2 ---> m5:g3 -> m6:g3(agg) -> m7:g3
        builder = ParallelPipeline()
        builder.add_module(self.ModuleA('m1', group='g1'))
        builder.add_module(self.ModuleB('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleA('m3', group='g2'))
        builder.add_module(self.ModuleB('m4', group='g2').depends_on(builder.get_module('m3')))
        builder.add_module(
            self.ModuleB('m5', group='g3')
            .depends_on(builder.get_module('m2'))
            .depends_on(builder.get_module('m4'))
        )
        builder.add_module(self.ModuleAgg('m6', group='g3').depends_on(builder.get_module('m5')))
        builder.add_module(self.ModuleC('m7', group='g3').depends_on(builder.get_module('m6')))

        pipeline = await builder.build()
        res, _ = await pipeline.run('R1')
        agg, _ = await pipeline.process('A1')

        assert len(res) == 5
        assert len(agg['agg']) == 1
        assert len(agg['m7']) == 2

    @pytest.mark.asyncio
    async def test_stateful_pipeline_with_stateful_group(self, ray_context):
        # m1:g1 -> m2:g1 -> \
        # m3:g2 -> m4:g2 ---> m5:g3 -> m6:g4(agg) -> m7:g4
        builder = ParallelPipeline()
        builder.add_module(self.ModuleA('m1', group='g1'))
        builder.add_module(self.ModuleB('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleA('m3', group='g2'))
        builder.add_module(self.ModuleB('m4', group='g2').depends_on(builder.get_module('m3')))
        builder.add_module(
            self.ModuleB('m5', group='g3')
            .depends_on(builder.get_module('m2'))
            .depends_on(builder.get_module('m4'))
        )
        builder.add_module(self.ModuleAgg('m6', group='g4').depends_on(builder.get_module('m5')))
        builder.add_module(self.ModuleC('m7', group='g4').depends_on(builder.get_module('m6')))

        pipeline = await builder.build()
        await pipeline.run('R1')
        res, _ = await pipeline.run('R2')
        agg, _ = await pipeline.process('A1')

        assert len(res) == 5
        assert len(agg) == 2
        assert len(agg['agg']) == 2
        assert agg['agg'][0][0] == agg['agg'][1][0] == ('m5', 'b')

    @pytest.mark.asyncio
    async def test_stateful_pipeline_with_two_afteragg_modules(self, ray_context):
        # m1:g1 -> m2:g1 -> \
        # m3:g2 -> m4:g2 ---> m5:g3 -> m6:g3(agg) -> m7:g4 -> m8:g4
        builder = ParallelPipeline()
        builder.add_module(self.ModuleA('m1', group='g1'))
        builder.add_module(self.ModuleB('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleA('m3', group='g2'))
        builder.add_module(self.ModuleB('m4', group='g2').depends_on(builder.get_module('m3')))
        builder.add_module(
            self.ModuleB('m5', group='g3')
            .depends_on(builder.get_module('m2'))
            .depends_on(builder.get_module('m4'))
        )
        builder.add_module(self.ModuleAgg('m6', group='g3').depends_on(builder.get_module('m5')))
        builder.add_module(self.ModuleC('m7', group='g4').depends_on(builder.get_module('m6')))
        builder.add_module(self.ModuleC('m8', group='g4').depends_on(builder.get_module('m7')))

        pipeline = await builder.build()
        await pipeline.run('R1')
        await pipeline.run('R2')
        agg, _ = await pipeline.process('A1')

        assert len(agg) == 3
        assert type(agg['agg']) == list
        assert len(agg['agg']) == 2
        assert type(agg['m7']) == list
        assert len(agg['m7']) == 4
        assert type(agg['m8']) == str
        assert agg['m8'] == 'c'

    @pytest.mark.asyncio
    async def test_stateful_pipeline_confluence_example(self, ray_context):
        # m1:g1 -> m2:g1 -> \   /--> m5:g3 -> m6:g3
        # m3:g2 ----------> m4:g3 -> agg7:g3 ----> m8:g4 -> m9:g4
        builder = ParallelPipeline()
        builder.add_module(self.ModuleA('m1', group='g1'))
        builder.add_module(self.ModuleB('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleA('m3', group='g2'))
        builder.add_module(
            self.ModuleB('m4', group='g3')
            .depends_on(builder.get_module('m2'))
            .depends_on(builder.get_module('m3'))
        )
        builder.add_module(self.ModuleB('m5', group='g3').depends_on(builder.get_module('m4')))
        builder.add_module(self.ModuleB('m6', group='g3').depends_on(builder.get_module('m5')))
        builder.add_module(self.ModuleAgg('agg7', group='g3').depends_on(builder.get_module('m4')))
        builder.add_module(self.ModuleC('m8', group='g4').depends_on(builder.get_module('agg7')))
        builder.add_module(self.ModuleC('m9', group='g4').depends_on(builder.get_module('m8')))

        pipeline = await builder.build()
        await pipeline.run('R1')
        await pipeline.run('R2')
        res, _ = await pipeline.run('R3')
        agg, _ = await pipeline.process('A1')

        assert len(res) == 6
        assert len(agg) == 3
        assert type(agg['agg']) == list
        assert len(agg['agg']) == 3
        assert type(agg['m8']) == list
        assert len(agg['m8']) == 6
        assert type(agg['m9']) == str
        assert agg['m9'] == 'c'

    @pytest.mark.asyncio
    async def test_stateful_pipeline_with_two_agg_modules_double_run_and_agg(self, ray_context):
        #                                           /-> m12:g6 -> m13:g6
        # m1:g1 -> m2:g1 -> \   /--> m5:g3 -> m6:g3 --> agg10:g5 -> m11:g5
        # m3:g2 ----------> m4:g3 -> agg7:g3 ---------> m8:g4 -> m9:g4

        builder = ParallelPipeline()
        builder.add_module(self.ModuleA('m1', group='g1'))
        builder.add_module(self.ModuleB('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleA('m3', group='g2'))
        builder.add_module(
            self.ModuleB('m4', group='g3')
            .depends_on(builder.get_module('m2'))
            .depends_on(builder.get_module('m3'))
        )
        builder.add_module(self.ModuleB('m5', group='g3').depends_on(builder.get_module('m4')))
        builder.add_module(self.ModuleB('m6', group='g3').depends_on(builder.get_module('m5')))
        builder.add_module(self.ModuleAgg('agg7', group='g3').depends_on(builder.get_module('m4')))
        builder.add_module(self.ModuleC('m8', group='g4').depends_on(builder.get_module('agg7')))
        builder.add_module(self.ModuleC('m9', group='g4').depends_on(builder.get_module('m8')))
        builder.add_module(
            self.ModuleAgg('agg10', group='g5')
            .depends_on(builder.get_module('m6'))
            .expose_result('second_agg')
        )
        builder.add_module(self.ModuleC('m11', group='g5').depends_on(builder.get_module('agg10')))
        builder.add_module(self.ModuleB('m12', group='g6').depends_on(builder.get_module('m6')))
        builder.add_module(self.ModuleB('m13', group='g6').depends_on(builder.get_module('m12')))

        pipeline = await builder.build()
        res1, _ = await pipeline.run('R1')
        agg1, _ = await pipeline.process('A1')
        res2, _ = await pipeline.run('R2')
        agg2, _ = await pipeline.process('A2')

        assert len(res1) == len(res2) == 8
        assert len(agg1) == len(agg2) == 5
        assert type(agg2['agg']) == list
        assert type(agg2['m8']) == list
        assert type(agg2['m9']) == str
        assert type(agg2['second_agg']) == list
        assert type(agg2['m11']) == str

    @pytest.mark.asyncio
    async def test_stateful_pipeline_invalid(self, ray_context):
        # m1:g1 -> m2:g1(agg)-\
        # m3:g2 -> m4:g2 ------> m5:g3
        builder = ParallelPipeline()
        builder.add_module(self.ModuleA('m1', group='g1'))
        builder.add_module(self.ModuleAgg('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleA('m3', group='g2'))
        builder.add_module(self.ModuleB('m4', group='g2').depends_on(builder.get_module('m3')))
        builder.add_module(
            self.ModuleC('m5', group='g3')
            .depends_on(builder.get_module('m2'))
            .depends_on(builder.get_module('m4'))
        )

        with pytest.raises(Exception):
            await builder.build()

    @pytest.mark.asyncio
    async def test_stateful_pipeline_with_only_nonregular_modules(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(self.ModuleAgg('m1', group='g1'))
        builder.add_module(self.ModuleC('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleC('m3', group='g2').depends_on(builder.get_module('m1')))

        pipeline = await builder.build()
        res, _ = await pipeline.run('R1')
        agg, _ = await pipeline.process('A1')

        assert len(res) == 0
        assert len(agg) == 3
        assert agg['agg'] == [[]]
        assert agg['m2'] == agg['m3'] == [[], []]

    @pytest.mark.asyncio
    async def test_stateful_pipeline_with_nonregular_modules_different_groups(self, ray_context):
        builder = ParallelPipeline()
        builder.add_module(self.ModuleAgg('m1', group='g1'))
        builder.add_module(self.ModuleC('m2', group='g1').depends_on(builder.get_module('m1')))
        builder.add_module(self.ModuleC('m3', group='g2').depends_on(builder.get_module('m2')))

        pipeline = await builder.build()
        res, _ = await pipeline.run('R1')
        agg, _ = await pipeline.process('A1')

        assert len(res) == 0
        assert len(agg) == 3
        assert agg['agg'] == [[]]
        assert agg['m2'] == [[], []]
        assert agg['m3'] == 'c'
