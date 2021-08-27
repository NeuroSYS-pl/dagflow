from __future__ import annotations

import asyncio
from typing import List, Optional, Callable

from magda.pipeline.parallel.group.actor import Actor
from magda.utils.logger import MagdaLogger


class ParallelActorPool:
    def __init__(self, actors: List[Actor]):
        self._replicas = len(actors)
        self._actors = actors
        self._idle_actors: asyncio.Queue[Actor] = asyncio.Queue()
        for actor in actors:
            self._idle_actors.put_nowait(actor)

    @property
    def replicas(self) -> int:
        return self._replicas

    async def bootstrap(self, logger: MagdaLogger, hooks: Optional[List[Callable]] = None):
        await asyncio.gather(*[
            actor.bootstrap.remote(logger, hooks)
            for actor in self._actors
        ])

    async def run(self, **job):
        actor = await self._idle_actors.get()
        result = await actor.run.remote(**job)
        await self._idle_actors.put(actor)
        return result

    async def teardown(self):
        for _ in range(self._replicas):
            actor = await self._idle_actors.get()
            await actor.teardown.remote()
