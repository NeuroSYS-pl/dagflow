from dataclasses import dataclass

import pytest
import ray
from pathlib import Path
from ray.util.queue import Queue

from magda.module.module import Module
from magda.module.factory import ModuleFactory
from magda.pipeline.sequential import SequentialPipeline
from magda.config_reader import ConfigReader
from magda.decorators import finalize, accept, produce, expose
from magda.exceptions import (WrongParametersStructureException,
                              WrongParameterValueException, ConfiguartionFileException)


class MockTestInterface(Module.Interface):
    pass


@pytest.fixture(scope='class')
def ray_context():
    ray.init(num_cpus=1)
    yield None
    ray.shutdown()


class TestConfigReader:

    def get_config_file(self, config_name):
        return Path(__file__).parent / 'test_configs' / config_name

    def prepare_basic_hooks_test_modules_factory(self):
        @produce(MockTestInterface)
        @finalize
        class DependingModule(Module.Runtime):
            def run(self, *args, **kwargs):
                pass

        @accept(MockTestInterface)
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)
        ModuleFactory.register('DependingModule', DependingModule)

    @pytest.mark.asyncio
    async def test_should_not_read_config_parameters_with_nested_structure(self):
        config_file = self.get_config_file('depending_modules_of_correct_interface.yaml')
        incorrect_parameters = {'ARG1': 'correct', 'ARG2': {'ARG3': 'incorrect'}}

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory, incorrect_parameters)

    @pytest.mark.asyncio
    async def test_should_not_read_config_parameters_with_incorrect_values(self):
        config_file = self.get_config_file('depending_modules_of_correct_interface.yaml')

        incorrect_parameters = {'ARG1': 'correct', 'ARG2': MockTestInterface}
        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory, incorrect_parameters)

    @pytest.mark.asyncio
    async def test_should_not_read_config_parameters_other_than_dictionary(self):
        config_file = self.get_config_file('depending_modules_of_correct_interface.yaml')

        incorrect_parameters = [{'ARG1': 'value1'}, {'ARG2': 'value2'}]
        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParametersStructureException):
                await ConfigReader.read(config, ModuleFactory, incorrect_parameters)

    @pytest.mark.asyncio
    async def test_should_not_read_config_parameters_with_wrong_dict_keys(self):
        config_file = self.get_config_file('depending_modules_of_correct_interface.yaml')

        incorrect_parameters = {MockTestInterface: 'value1', 2: 'value2'}
        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory, incorrect_parameters)

    @pytest.mark.asyncio
    async def test_should_pass_with_correct_variables_in_config(self):

        @produce(MockTestInterface)
        @finalize
        class DependingModule(Module.Runtime):
            def run(self, *args, **kwargs):
                pass

        @accept(MockTestInterface)
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)
        ModuleFactory.register('DependingModule', DependingModule)

        config_file = self.get_config_file('correctly_parametrized_config.yaml')

        correct_parameters = {
            'TYPE_1': 'DependingModule',
            'NAME_2': 'mod2',
            'THRESHOLD': 0.2,
            'BIAS': 0.1,
            'group_name': 'group1'
        }

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory, correct_parameters)

        assert 3 == len(pipeline.modules)
        assert issubclass(pipeline.modules[0].interface, MockTestInterface)
        assert issubclass(pipeline.modules[1].interface, MockTestInterface)
        assert pipeline.modules[2].input_modules == ['mod1', 'mod2']

    @pytest.mark.asyncio
    async def test_should_not_pass_with_excessive_variables_in_config(self):
        config_file_with_excessive_vars = self.get_config_file(
            'incorrectly_parametrized_config.yaml'
        )

        correct_parameters = {
            'NAME_2': 'mod2',
            'THRESHOLD': 0.2
        }

        with open(config_file_with_excessive_vars) as config:
            config = config.read()
            with pytest.raises(ConfiguartionFileException):
                await ConfigReader.read(
                    config,
                    ModuleFactory,
                    correct_parameters
                )

    @pytest.mark.asyncio
    async def test_should_raise_error_on_incorrect_expose_type(self):
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)

        config_file = self.get_config_file('wrong_type_of_exposed_module_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory)

    @pytest.mark.asyncio
    async def test_should_raise_exception_on_duplicated_expose_values(self):
        @produce(MockTestInterface)
        @finalize
        class DependingModule(Module.Runtime):
            def run(self, *args, **kwargs):
                pass

        @accept(MockTestInterface)
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)
        ModuleFactory.register('DependingModule', DependingModule)

        config_file = self.get_config_file('duplicated_exposed_values_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(Exception):
                await ConfigReader.read(config, ModuleFactory)

    @pytest.mark.asyncio
    async def test_should_correctly_assign_expose_values_when_given(self):

        @produce(MockTestInterface)
        @finalize
        class DependingModule(Module.Runtime):
            def run(self, *args, **kwargs):
                pass

        @accept(MockTestInterface)
        @produce(MockTestInterface)
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        @accept(MockTestInterface)
        @finalize
        class ModuleSuccessor(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)
        ModuleFactory.register('DependingModule', DependingModule)
        ModuleFactory.register('ModuleSuccessor', ModuleSuccessor)

        config_file = self.get_config_file('correctly_exposed_modules_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory)

        modules_exposed_dict = {
            module.name: module.exposed
            for module in pipeline.modules
        }

        assert 4 == len(pipeline.modules)
        assert modules_exposed_dict['mod0'] == 'sample-result'
        assert modules_exposed_dict['mod1'] is None
        assert modules_exposed_dict['mod2'] == 'mod2'
        assert modules_exposed_dict['mod4'] is None

    @pytest.mark.asyncio
    async def test_should_overide_declared_expose_values(self):

        @produce(MockTestInterface)
        @expose('declared-depending-result')
        @finalize
        class DependingModule(Module.Runtime):
            def run(self, *args, **kwargs):
                pass

        @accept(MockTestInterface)
        @produce(MockTestInterface)
        @expose('declared-sample-result')
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        @accept(MockTestInterface)
        @expose('should-remain-untouched')
        @finalize
        class ModuleSuccessor(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)
        ModuleFactory.register('DependingModule', DependingModule)
        ModuleFactory.register('ModuleSuccessor', ModuleSuccessor)

        config_file = self.get_config_file('correctly_exposed_modules_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory)

        modules_exposed_dict = {
            module.name: module.exposed
            for module in pipeline.modules
        }

        assert 4 == len(pipeline.modules)
        assert modules_exposed_dict['mod0'] == 'sample-result'
        assert modules_exposed_dict['mod1'] is None
        assert modules_exposed_dict['mod2'] == 'mod2'
        assert modules_exposed_dict['mod4'] == 'should-remain-untouched'

    @pytest.mark.asyncio
    async def test_should_correctly_access_exposed_results(self):

        @dataclass(frozen=True)
        class DataInterface(Module.Interface):
            data: str

        @produce(DataInterface)
        @expose('should-be-overridden')
        @finalize
        class DependingModule(Module.Runtime):
            def run(self, *args, **kwargs):
                return DataInterface('depending')

        @accept(DataInterface)
        @produce(DataInterface)
        @finalize
        class ModuleSample(Module.Runtime):
            def run(self, *args, **kwargs):
                return DataInterface('parent')

        ModuleFactory.register('ModuleSample', ModuleSample)
        ModuleFactory.register('DependingModule', DependingModule)

        config_file = self.get_config_file('correctly_exposed_modules_for_results_check.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory)

        runtime, _ = await pipeline.run()

        assert 'depending-result' in runtime
        assert 'parent-result' in runtime
        assert 'should-be-overridden' not in runtime
        assert runtime['depending-result'].data == 'depending'
        assert runtime['parent-result'].data == 'parent'

    @pytest.mark.asyncio
    async def test_should_raise_error_on_incorrect_name_in_config(self):
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)

        config_file = self.get_config_file('wrong_type_of_pipeline_name_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory)

    @pytest.mark.asyncio
    async def test_should_raise_error_on_incorrect_name_specified_directly(self):
        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)
        incorrect_name = ['test']

        config_file = self.get_config_file('correct_pipeline_name_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory, name=incorrect_name)

    @pytest.mark.asyncio
    async def test_should_correctly_assign_pipeline_name_from_config(self):

        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)

        config_file = self.get_config_file('correct_pipeline_name_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory)

        assert 1 == len(pipeline.modules)
        assert 'TestPipeline' == pipeline.name

    @pytest.mark.asyncio
    async def test_should_correctly_override_assigned_pipeline_name(self):

        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)

        overriding_name = 'OverridingName'

        config_file = self.get_config_file('correct_pipeline_name_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory, name=overriding_name)

        assert 1 == len(pipeline.modules)
        assert overriding_name == pipeline.name

    @pytest.mark.asyncio
    async def test_should_have_not_empty_pipeline_name_when_none_assigned(self):

        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)

        config_file = self.get_config_file('no_pipeline_name_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory)

        assert 1 == len(pipeline.modules)
        assert pipeline.name is not None
        assert 'Pipeline-' in pipeline.name

    @pytest.mark.asyncio
    async def test_should_correctly_assign_numeric_pipeline_name(self):

        @finalize
        class ModuleSample(Module.Runtime):
            pass

        ModuleFactory.register('ModuleSample', ModuleSample)

        numeric_name = 5

        config_file = self.get_config_file('no_pipeline_name_in_config.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory, name=numeric_name)

        assert 1 == len(pipeline.modules)
        assert pipeline.name == numeric_name

    @pytest.mark.asyncio
    async def test_should_correctly_init_parallel_pipeline_with_hooks_in_list(self, ray_context):

        self.prepare_basic_hooks_test_modules_factory()

        callable_1_counter = Queue()
        callable_2_counter = Queue()

        def callable_1():
            callable_1_counter.put(1)

        def callable_2():
            callable_2_counter.put(2)

        config_file = self.get_config_file('correct_exemplary_config_with_groups.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(
                config, ModuleFactory,
                after_created=[callable_1, callable_2, callable_2]
            )

        assert 3 == len(pipeline.modules)
        assert 4 == callable_1_counter.qsize()
        assert 8 == callable_2_counter.qsize()

    @pytest.mark.asyncio
    async def test_should_correctly_init_parallel_pipeline_with_hooks_in_dict(self, ray_context):

        self.prepare_basic_hooks_test_modules_factory()

        callable_1_counter = Queue()
        callable_2_counter = Queue()

        def callable_1():
            callable_1_counter.put(1)

        def callable_2():
            callable_2_counter.put(2)

        config_file = self.get_config_file('correct_exemplary_config_with_groups.yaml')

        with open(config_file) as config:
            config = config.read()
            pipeline = await ConfigReader.read(config, ModuleFactory, after_created={
                "g1": [callable_1, callable_2, callable_2],
                "g2": [callable_1],
                "g3": []
            })

        assert 3 == len(pipeline.modules)
        assert 3 == callable_1_counter.qsize()
        assert 4 == callable_2_counter.qsize()

    @pytest.mark.asyncio
    async def test_should_not_accept_non_callables_in_hooks_list(self):

        self.prepare_basic_hooks_test_modules_factory()

        def callable_1():
            pass

        config_file = self.get_config_file('correct_exemplary_config_with_groups.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(
                    config,
                    ModuleFactory,
                    after_created=[callable_1, 'non-callable']
                )

    @pytest.mark.asyncio
    async def test_should_not_accept_non_callables_in_hooks_dict_structure(self):

        self.prepare_basic_hooks_test_modules_factory()

        def callable_1():
            pass

        config_file = self.get_config_file('correct_exemplary_config_with_groups.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory, after_created={
                    'g1': [callable_1, callable_1],
                    'g2': [callable_1, 34]
                })

    @pytest.mark.asyncio
    async def test_should_not_accept_non_existing_groups_in_hooks_dict(self):

        self.prepare_basic_hooks_test_modules_factory()

        def callable_1():
            pass

        config_file = self.get_config_file('correct_exemplary_config_with_groups.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(config, ModuleFactory, after_created={
                    'g1': [callable_1, callable_1],
                    'g20': [callable_1]
                })

    @pytest.mark.asyncio
    async def test_should_not_accept_wrong_type_of_hooks_parameter(self):

        self.prepare_basic_hooks_test_modules_factory()

        config_file = self.get_config_file('correct_exemplary_config_with_groups.yaml')

        with open(config_file) as config:
            config = config.read()
            with pytest.raises(WrongParameterValueException):
                await ConfigReader.read(
                    config,
                    ModuleFactory,
                    after_created='THIS IS A WRONG VALUE'
                )
