import asyncio
from time import sleep

from magda.module import Module
from magda.decorators import register, accept, finalize, produce
from magda.utils.logger import MagdaLogger

from examples.interfaces.common import Request, Context
from examples.interfaces.string import StringInterface
from examples.interfaces.fn import LambdaInterface


@accept(StringInterface, LambdaInterface)
@produce(StringInterface)
@register('A')
@finalize
class ModuleA(Module.Runtime):
    SLEEP_TIME = 2

    def bootstrap(self, logger: MagdaLogger.Facade):
        ctx: Context = self.context
        logger.info(f'Context.timer = {ctx.timer}')

    async def teardown(self, logger: MagdaLogger.Facade):
        ctx: Context = self.context
        logger.info(f'Long... | Context.timer = {ctx.timer}')
        await asyncio.sleep(1)
        logger.info(f'...Teardown | Context.timer = {ctx.timer}')

    def run(self, data: Module.ResultSet, request: Request, *args, **kwargs):
        # Access context
        ctx: Context = self.context

        # Access results from the previous modules
        #   `src` is a list of strings
        text = [t.fn() for t in data.of(LambdaInterface)]
        src = [t.data for t in data.of(StringInterface)] + text

        # Some heavy computational operations for example
        sleep(self.SLEEP_TIME)

        # Produce declared interface
        return StringInterface(
            f'{ctx.prefix} ('
            + ' + '.join(src)
            + (' = ' if len(src) else '')
            + f'{self.name}) {request.value}'
        )
