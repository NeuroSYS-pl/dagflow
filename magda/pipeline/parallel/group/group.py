from __future__ import annotations

import warnings
from typing import List, Optional, Callable

from magda.module.module import Module
from magda.pipeline.parallel.group.runtime import GroupRuntime


class Group:
    Runtime = GroupRuntime

    def __init__(
        self,
        name: str,
        *,
        replicas: int = 1,
        state_type=None,
        after_created: Optional[List[Callable]] = None,
        **options,
    ):
        self.name = name
        self.hooks = after_created
        self.replicas = replicas
        self.options = options
        self._state_type = state_type
        self._validate_after_created_hooks(self.hooks)

    def set_replicas(self, replicas: int) -> Group:
        self.replicas = replicas
        return self

    def set_options(self, **options) -> Group:
        self.options = options
        return self

    @property
    def state_type(self):
        return self._state_type

    @state_type.setter
    def state_type(self, state_type):
        self._state_type = state_type

    def build(
        self,
        modules: List[Module],
        dependent_modules: List[Module],
        dependent_modules_nonregular: List[Module],
    ) -> Group.Runtime:
        return self.Runtime(
            name=self.name,
            hooks=self.hooks,
            modules=modules,
            dependent_modules=dependent_modules,
            dependent_modules_nonregular=dependent_modules_nonregular,
            replicas=self.replicas,
            state_type=self.state_type,
            options=self.options,
        )

    def _validate_after_created_hooks(self, hooks):
        if hooks:
            if isinstance(hooks, list):
                if len(hooks) > 0:
                    if not all(callable(hook) for hook in hooks):
                        raise Exception("Parameter 'after_created' in Group constructor "
                                        "contains a list with non-callable elements.")
                else:
                    warnings.warn("Parameter 'after_created' in Group constructor "
                                  "contains an empty list.")
            else:
                raise Exception("Parameter 'after_created' in Group constructor "
                                "should be a list")
