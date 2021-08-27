import asyncio
from time import time
from pathlib import Path

from magda import ConfigReader
from magda.pipeline.parallel import init
from magda.module.factory import ModuleFactory
from magda.utils.logger.logger import MagdaLogger
from magda.utils.logger.hooks import actor_logging_hook

from examples.interfaces.common import Context, Request
from examples.modules import *


class ExampleParallelConfigReader:
    def __init__(self):
        init()

    @classmethod
    async def demo(cls):
        example = cls()
        await example.build()
        await example.run()

    def get_config_file(self, config_name):
        return Path(__file__).parent / 'configs' / config_name

    async def build(self, prefix: str = '{CTX}'):
        config_file = self.get_config_file('sample_config_parallel.yaml')
        config_params = {
            'GROUP_M1': 'g1',
            'GROUP_M4': 'g3',
        }

        with open(config_file, 'r') as config:
            config = config.read()
            self.pipeline = await ConfigReader.read(
                config,
                ModuleFactory,
                config_parameters=config_params,
                context=lambda: Context(prefix),
                logger=MagdaLogger.Config(output=MagdaLogger.Config.Output.LOGGING),
                after_created=[actor_logging_hook]
            )

    async def run(self, value: str = 'R', n_jobs: int = 3):
        # Run jobs and measure duration
        start = time()
        results = await asyncio.gather(*[
            asyncio.create_task(
                self.pipeline.run(Request(value=f'{value}{i}'))
            )
            for i in range(n_jobs)
        ])
        end = time()

        # Close pipeline and teardown modules
        await self.pipeline.close()
        await asyncio.sleep(0.5)

        # Print results
        print(f'Duration = {end - start:.2f} s')
        print('Results:')
        for index, (result, _) in enumerate(results):
            print(f' {index}:')
            for key, value in result.items():
                print(f'  {key}\t → {value}')


if __name__ == "__main__":
    asyncio.run(ExampleParallelConfigReader.demo())
