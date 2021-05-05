from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, List, Optional
from weakref import ref

from magda.module.module import Module
from magda.utils.logger import MagdaLogger


class Graph:
    """ Graph class

    Graph holds all modules added to the same group.
    It's responsible for sorting as well as running all modules.
    """

    class TopologicalSorter:
        def __init__(self, modules: List[Module.Runtime]):
            self.modules = modules

        def get(self) -> List[Module.Runtime]:
            """ Get topological order of modules.

            Construct a graph structure as a dictionary, with keys containing nodes
            that are an input to other nodes, and with values equal to nodes they are
            connected to, representing an association list. The graph is then sorted,
            via topological sorting, ensuring an optimal module call sequence.
            """
            scope = [m.name for m in self.modules]
            self.graph = defaultdict(list)
            for mod in self.modules:
                if len(mod.output_modules) > 0:
                    self.graph[mod.name] = [
                        output
                        for output in mod.output_modules
                        if output in scope
                    ]

            visited = {}
            for mod in self.modules:
                visited[mod.name] = False
            stack = []

            for mod in self.modules:
                if not visited[mod.name]:
                    self._topological_sort(mod.name, visited, stack)

            sorted_modules = []
            for s in stack:
                sorted_modules.append(self._get_module(s))
            return sorted_modules

        def _topological_sort(self, v, visited, stack):
            """ Util method for sorting."""
            visited[v] = True
            for i in self.graph[v]:
                if not visited[i]:
                    self._topological_sort(i, visited, stack)
            stack.insert(0, v)

        def _get_module(self, module_name):
            return next((mod for mod in self.modules if mod.name == module_name), None)

    def __init__(self, modules: List[Module.Runtime]):
        self._modules = self.TopologicalSorter(modules).get()
        self._logger = None

    @property
    def modules(self) -> List[Module.Runtime]:
        return self._modules.copy()

    async def run(
        self,
        request,
        results: List[Module.Result] = [],
        is_regular_runtime=True,
    ) -> List[Module.Result]:
        """ Main method for running regular modules in the pipeline

        Modules are run one after another. The output from every module is saved to results array.
        Results are filtered for those containing the inputs for current module,
        and only those are passed to it. The method returns last module's output as it's result.
        """
        results = results.copy()
        for module in self._modules:
            if self._should_be_run(module=module, is_regular_runtime=is_regular_runtime):
                data = Module.ResultSet([r for r in results if r.name in module.input_modules])
                logger = self._logger.chain(module=ref(module), request=request)
                logger.event('START')
                module_result = await self._run_method_helper(
                    module, data, request, is_regular_runtime, logger,
                )
                results.append(
                    Module.Result(
                        result=module_result,
                        interface=module.interface,
                        name=module.name,
                        src_class=type(module),
                        expose=self._exposed_result(module, is_regular_runtime),
                    )
                )
        return results

    def attach_logger(self, logger: MagdaLogger.Facade) -> None:
        self._logger = logger

    async def bootstrap(self) -> None:
        for module in self._modules:
            logger = self._logger.chain(module=ref(module))
            logger.event('BOOTSTRAP')
            await module._on_bootstrap(logger=logger)

    async def teardown(self):
        for module in self._modules:
            logger = self._logger.chain(module=ref(module))
            logger.event('TEARDOWN')
            if asyncio.iscoroutinefunction(module.teardown):
                await module.teardown(logger=logger)
            else:
                module.teardown(logger=logger)

    def _should_be_run(
        self,
        module: Module.Runtime,
        is_regular_runtime: bool,
    ) -> bool:
        if is_regular_runtime:
            return module.is_regular_module or issubclass(type(module), Module.Aggregate)
        else:
            return not module.is_regular_module or issubclass(type(module), Module.Aggregate)

    def _exposed_result(
        self,
        module: Module.Runtime,
        is_regular_runtime: bool,
    ) -> Optional[str]:
        if is_regular_runtime and issubclass(type(module), Module.Aggregate):
            return None
        else:
            return module.exposed

    async def _run_method_helper(
        self,
        module: Module.Runtime,
        data: Module.ResultSet,
        request: Any,
        is_regular_runtime: bool,
        logger: MagdaLogger.Facade,
    ) -> Any:
        module_result = None
        props = dict(
            data=data,
            request=request,
            logger=logger,
        )

        if issubclass(type(module), Module.Aggregate):
            if is_regular_runtime:
                if asyncio.iscoroutinefunction(module.aggregate):
                    await module.aggregate(**props)
                else:
                    module.aggregate(**props)
                module_result = []
            else:
                module_result = (
                    await module.process(**props)
                    if asyncio.iscoroutinefunction(module.process)
                    else module.process(**props)
                )
        else:
            module_result = (
                await module.run(**props)
                if asyncio.iscoroutinefunction(module.run)
                else module.run(**props)
            )
        return module_result
