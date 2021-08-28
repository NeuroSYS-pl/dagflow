---
sidebar_position: 2
---

import SchemaSvg from './assets/quick-start/schema.svg';
import PipelineSvg from './assets/quick-start/pipeline.svg';


# Quick Start ⏱️

## Installation

###### pip
```bash
pip install magda==0.1
```

###### From the repository
```bash
pip install https://github.com/NeuroSYS-pl/magda/archive/refs/tags/v0.1.zip
```

## Usage

Having installed *MAGDA*, a simplistic pipeline can be created with just a few lines of code.

<SchemaSvg className="diagram" width="50%" />

The above pipeline is composed of just 2 modules. The first one sums all numbers from a given list and outputs a single number. And the second module raises that number to a given power.

There can be implemented **several approaches to building the same pipeline**. 

### 1. The simplest `SequentialPipeline`

Every pipeline consists of a couple of steps:
1. Class definition - defining each `Module` (and `Interface`)
2. Module initialization - getting every `Module` instance, defining its dependencies and parameters
3. Pipeline creation - defining a pipeline and adding `Modules` to it
4. Pipeline build
5. Pipeline run

<PipelineSvg className="diagram" width="50%" />

```python title="main.py"
import asyncio
from magda.module import Module
from magda.decorators import accept, finalize, expose
from magda.pipeline import SequentialPipeline


@finalize
class AddingNumbersModule(Module.Runtime):
    def run(self, data, request):
        return sum(request)

@accept(AddingNumbersModule)
@expose()
@finalize
class RaisingToPowerModule(Module.Runtime):
    def run(self, data, **kwargs):
        number = data.get(AddingNumbersModule)
        return number ** self.parameters['power']


sum_module = AddingNumbersModule('module_sum')
power_module = RaisingToPowerModule('module_power')
power_module.depends_on(sum_module)
power_module.set_parameters({'power': 2})

builder = SequentialPipeline()
builder.add_module(sum_module)
builder.add_module(power_module)

runtime = asyncio.run(builder.build())
result = asyncio.run(runtime.run(request=[1, 2, 3]))
print(result['module_power'])
# output: 36
```

### 2. `SequentialPipeline` with Interfaces

*MAGDA* Interfaces are just classes encapsulating data passed between modules. However, it's recommended to use them as they straighten the code up, providing more clarity and flexibility. 

The above code can be rewritten as follows:

```python title="main.py"
import asyncio
from dataclasses import dataclass
from magda.module import Module
from magda.decorators import accept, produce, finalize, expose
from magda.pipeline import SequentialPipeline


@dataclass
class Number(Module.Interface):
    value: int

@dataclass
class Power(Module.Interface):
    number: int
    power: int = 1


@produce(Number)
@finalize
class AddingNumbersModule(Module.Runtime):
    def run(self, data, request):
        return Number(sum(request))

@accept(Number)
@produce(Power)
@expose()
@finalize
class RaisingToPowerModule(Module.Runtime):
    def run(self, data, **kwargs):
        number = data.get(Number).value
        power = self.parameters['power']
        return Power(number ** power, power=power)


sum_module = AddingNumbersModule('module_sum')
power_module = RaisingToPowerModule('module_power')
power_module.depends_on(sum_module)
power_module.set_parameters({'power': 2})

builder = SequentialPipeline()
builder.add_module(sum_module)
builder.add_module(power_module)

runtime = asyncio.run(builder.build())
result = asyncio.run(runtime.run(request=[1, 2, 3]))
print(result['module_power'])
# output: Power(number=36, power=2)
```

### 3. `SequentialPipeline` built from a config file

It's also recommended to use configs - `yaml` files that enable to define a pipeline easily. The *Pipeline creation* and *Pipeline build* steps are now replaced by registering `Modules` in the `ModuleFactory` and reading the pipeline from a configuration file.  

The same pipeline as before can be obtained using the below config:

```yaml title="my_config_file.yaml"
modules:
  - name: module_sum
    type: adding-numbers-module
  - name: module_power
    type: raising-to-power-module
    depends_on:
      - module_sum
    parameters:
      power: 2
```

```python title="main.py"
import asyncio
from dataclasses import dataclass
from magda.module import Module
from magda.decorators import accept, produce, finalize, expose
from magda.pipeline import SequentialPipeline
from magda.module.factory import ModuleFactory
from magda.config_reader import ConfigReader


@dataclass
class Number(Module.Interface):
    value: int

@dataclass
class Power(Module.Interface):
    number: int
    power: int = 1


@produce(Number)
@finalize
class AddingNumbersModule(Module.Runtime):
    def run(self, data, request):
        return Number(sum(request))

@accept(Number)
@produce(Power)
@expose()
@finalize
class RaisingToPowerModule(Module.Runtime):
    def run(self, data, **kwargs):
        number = data.get(Number).value
        power = self.parameters['power']
        return Power(number ** power, power=power)


ModuleFactory.register('adding-numbers-module', AddingNumbersModule)
ModuleFactory.register('raising-to-power-module', RaisingToPowerModule)

with open('my_config_file.yaml') as file:
    config = file.read()
    runtime = asyncio.run(ConfigReader.read(config, ModuleFactory))

result = asyncio.run(runtime.run(request=[1, 2, 3]))
print(result['module_power'])
# output: Power(number=36, power=2)
```
