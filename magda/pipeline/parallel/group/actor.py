from __future__ import annotations

from uuid import UUID
from typing import List, Optional, List, Callable

import ray

from magda.module.module import Module
from magda.pipeline.graph import Graph
from magda.pipeline.parallel.future_result import FutureResult
from magda.pipeline.parallel.group.state_type import StateType
from magda.utils.logger import MagdaLogger


@ray.remote
class Actor:
    def __init__(
        self,
        *,
        name: str,
        index: Optional[int],
        state_type: StateType,
        modules: List[Module],
    ):
        self.name = name
        self.index = index
        self.state_type = state_type
        self.graph = Graph(modules)
        self._logger = None

    async def bootstrap(self, logger: MagdaLogger, hooks: Optional[List[Callable]] = None):
        self._logger = logger.chain(
            group=MagdaLogger.Parts.Group(
                name=self.name,
                replica=self.index,
            ),
        )
        if hooks:
            for hook in hooks:
                hook()
        await self.graph.bootstrap(self._logger)

    async def run(self, job_id: UUID, request, results=[], is_regular_runtime=True):
        result = await self.graph.run(request, results, is_regular_runtime)
        return FutureResult(
            job=job_id,
            group=self.name,
            result=result,
        )

    async def teardown(self):
        await self.graph.teardown()
