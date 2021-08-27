from __future__ import annotations

import re
import yaml
import warnings
from dataclasses import dataclass, field
from numbers import Number
from typing import Optional, List, Dict, Any, Type, Union, Callable, Set

from magda.module.factory import ModuleFactory
from magda.pipeline.base import BasePipeline
from magda.pipeline.sequential import SequentialPipeline
from magda.pipeline.parallel.parallel_pipeline import ParallelPipeline
from magda.utils.logger import MagdaLogger
from magda.exceptions import (WrongParametersStructureException,
                              WrongParameterValueException, ConfiguartionFileException)


class ConfigReader:
    @dataclass(frozen=True)
    class ConfigModule:
        name: str
        type: str
        expose: Optional[Union[str, bool]] = field(default=None)
        group: Optional[str] = field(default=None)
        depends_on: List[str] = field(default_factory=list)
        parameters: Dict[str, Any] = field(default=None)

    @classmethod
    async def read(
        cls: Type[ConfigReader],
        config: str,
        module_factory: ModuleFactory,
        config_parameters: Optional[Dict] = None,
        name: Optional[str] = None,
        context: Optional[Any] = None,
        shared_parameters: Optional[Dict] = None,
        *,
        logger: Optional[MagdaLogger.Config] = None,
        after_created: Optional[Union[Dict[str, List[Callable]], List[Callable]]] = None
    ) -> BasePipeline.Runtime:
        if config_parameters:
            cls._validate_config_parameters_structure(config_parameters)

        config = cls._check_and_substitute_declared_variables(config, config_parameters)

        parsed_yaml = yaml.safe_load(config)

        config_pipeline_name, modules, shared_parameters, group_options = \
            cls._extract_information_from_yaml(parsed_yaml, shared_parameters)

        cls._check_expose_settings(modules)

        if name and config_pipeline_name:
            warnings.warn('The pipeline name specified in config wil be overriden '
                          'by ConfigReader.read parameter')

        name = name or config_pipeline_name

        if name:
            cls._check_pipeline_name(name)

        is_parallel_pipeline = any([m.group is not None for m in modules])

        pipeline = (
            ParallelPipeline(name=name)
            if is_parallel_pipeline
            else SequentialPipeline(name=name)
        )

        pipeline = cls._add_modules_to_pipeline(modules, pipeline, module_factory)

        if after_created:
            if not is_parallel_pipeline:
                warnings.warn('Hooks passed in parameter `after_created` '
                              'will not be used in SequentialPipeline')
            else:
                cls._check_hooks(after_created, {m.group for m in modules})

        pipeline = cls._add_group_options(group_options, pipeline, after_created)

        # connect modules
        for mod in modules:
            curr_mod_obj = pipeline.get_module(mod.name)
            for dependent_mod_name in mod.depends_on:
                dependent_mod_obj = pipeline.get_module(dependent_mod_name)
                if dependent_mod_obj:
                    curr_mod_obj.depends_on(dependent_mod_obj)
                else:
                    raise AttributeError(
                        f"Module '{dependent_mod_name}' hasn't been defined in the config file, "
                        "whereas it's used as a dependency."
                    )

        runtime = await pipeline.build(
            context=context,
            shared_parameters=shared_parameters,
            logger=logger,
        )

        return runtime

    @staticmethod
    def _check_and_substitute_declared_variables(config_str, config_parameters):
        declared_variables = list(set(re.findall(r'\${(\w+)}', config_str)))

        if declared_variables:
            if not config_parameters:
                raise ConfiguartionFileException(
                    "Config file contains declared variables and"
                    f"no config parameters were passed. Found variables: {declared_variables}"
                )
            else:
                parameters_variables = config_parameters.keys()

                unlinked_variables = [
                    declared_var
                    for declared_var in declared_variables
                    if declared_var not in parameters_variables
                ]

                if unlinked_variables:
                    raise ConfiguartionFileException(
                        "Config file contains declared variables "
                        "that were not specified in parameters."
                        f" Found unlinked variables {unlinked_variables}"
                    )

                for parameters_var in parameters_variables:
                    if parameters_var not in declared_variables:
                        warnings.warn(
                            "Parameters contain an additional "
                            f"variable that is not used in config file: {parameters_var}"
                        )
                    else:
                        config_str = config_str.replace(
                            f"${{{parameters_var}}}",
                            str(config_parameters[parameters_var])
                        )

        return config_str

    @staticmethod
    def _check_expose_settings(modules: List[ConfigModule]):
        for module in modules:
            if not isinstance(module.expose, (str, bool)) and module.expose:
                raise WrongParameterValueException(
                    "Parameter 'expose' in config should accept string and bools only. "
                    f"For module: '{module.name}' found value: '{module.expose}'."
                )

    @staticmethod
    def _validate_config_parameters_structure(config_parameters):
        if not isinstance(config_parameters, Dict):
            raise WrongParametersStructureException(
                "Configuration parameters should be passed in a dictionary."
            )
        for key, value in config_parameters.items():
            if not re.match(r'^\w+$', str(key)):
                raise WrongParameterValueException(
                    "Configuration parameters keys should contain "
                    f"only alphanumeric chars and underscores. Found: {key}."
                )
            if not isinstance(value, (str, Number)):
                raise WrongParameterValueException(
                    "Configuration parameters values should be of "
                    f"string or numeric type. Found value: {value}."
                )

    @staticmethod
    def _check_pipeline_name(name: str):
        if not isinstance(name, (str, Number)):
            raise WrongParameterValueException(
                "Parameter 'name' should accept strings or numbers only."
                f"Found value: '{name}'."
            )

    @staticmethod
    def _check_hooks(
        hooks: Union[Dict[str, List[Callable]], List[Callable]],
        group_names: Set[str]
    ):
        if isinstance(hooks, list):
            if len(hooks) > 0:
                if not all(callable(hook) for hook in hooks):
                    raise WrongParameterValueException(
                        "Parameter 'after_created' contains list with non-callable elements."
                    )
            else:
                warnings.warn("Parameter 'after_created' contains an empty list.")
        elif isinstance(hooks, dict):
            if len(hooks) > 0:
                if not all(group in group_names for group in hooks.keys()):
                    raise WrongParameterValueException(
                        "Parameter 'after_created' contains a dictionary "
                        "with non-existing groups as keys."
                    )
                if not all(isinstance(hook, list) for hook in hooks.values()):
                    raise WrongParameterValueException(
                        "Parameter 'after_created' contains a dictionary "
                        "with values that are not lists."
                    )

                flattened_dict_values = [
                    hook
                    for group_hooks in hooks.values()
                    for hook in group_hooks
                ]

                if not all(callable(hook) for hook in flattened_dict_values):
                    raise WrongParameterValueException(
                        "Parameter 'after_created' contains a dictionary "
                        "with non-callable values."
                    )
            else:
                warnings.warn("Parameter 'after_created' contains an empty dict.")
        else:
            raise WrongParameterValueException(
                "Parameter 'after_created' should accept "
                "list of callables or dictionary Dict[str, List[Callable] indexed by groups. "
                f"Found value: '{hooks}'."
            )

    @staticmethod
    def _extract_information_from_yaml(parsed_yaml, shared_parameters):
        try:
            modules = [ConfigReader.ConfigModule(**data) for data in parsed_yaml['modules']]
        except TypeError:
            raise Exception("Every module defined in a config file has to have a name and a type.")

        pipeline_name = parsed_yaml.get('name')

        if 'shared_parameters' in parsed_yaml:
            shared_parameters = parsed_yaml['shared_parameters']

        group_options = {}
        if 'groups' in parsed_yaml:
            group_options = {
                group['name']: group['options']
                for group in parsed_yaml['groups']
            }

        for module in modules:
            if module.group not in group_options and module.group is not None:
                group_options[module.group] = {}

        return pipeline_name, modules, shared_parameters, group_options

    @staticmethod
    def _add_modules_to_pipeline(modules, pipeline, module_factory):
        for mod in modules:
            created_module = module_factory.create(mod.name, mod.type, mod.group)
            if mod.expose is not None:
                if created_module.exposed:
                    warnings.warn("The 'expose' setting declared in decorator for "
                                  f"module: {created_module.name} will be overriden "
                                  "by setting in config file.")
                if isinstance(mod.expose, str):
                    created_module.expose_result(mod.expose)
                elif not mod.expose:
                    created_module.expose_result(enable=False)
                else:
                    created_module.expose_result(mod.name)
            if mod.parameters:
                created_module.set_parameters(mod.parameters)
            pipeline.add_module(created_module)
        return pipeline

    @classmethod
    def _add_group_options(cls, group_options, pipeline, hooks):
        if group_options and isinstance(pipeline, ParallelPipeline):
            for name, params in group_options.items():
                if hooks:
                    group_hooks = hooks[name] if isinstance(hooks, dict) else hooks
                else:
                    group_hooks = None
                group = ParallelPipeline.Group(name, after_created=group_hooks)
                group.set_replicas(params['replicas'] if 'replicas' in params else 1)
                params.pop('replicas', None)
                if params.keys():
                    group.set_options(**params)
                pipeline.add_group(group)
        return pipeline
