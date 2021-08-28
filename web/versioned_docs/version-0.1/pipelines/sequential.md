---
title: Sequential
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import CycleSvg from '../assets/sequential/cycle.svg';
import DisjointSvg from '../assets/sequential/disjoint.svg';
import FlowSvg from '../assets/sequential/flow.svg';
import RuntimeSvg from '../assets/sequential/runtime.svg';
import AggregateSvg from '../assets/sequential/aggregate.svg';


# Sequential Pipeline

The first type of a pipeline is a *Sequential Pipeline*. The other, more complex one is a *Parallel Pipeline*, which is in fact just multiple sequential pipelines running in parallel. 

## Overview

A *Pipeline* is composed of a set of *Modules*. It's a manager for creating and controlling the flow throughout *DAG* (*Directed Acyclic Graph*).

Like *Modules*, a *Pipeline* can be in one of two distinct states:
- **Builder**<br/>
is responsible for constructing a graph of modules and then sorting it,

- **Execution**<br/>
is tasked with the execution of proper modules via `run` or `process` methods, which are described in detail [here](../modules/stateful.md#pipeline-modes).

Additionally, a pipeline may be also closed by running `close` function on the runtime object. Closing a module results in calling `teardown` method for each module. A closed pipeline cannot be rerun. 

## Directed Acyclic Graph

When invoking `Pipeline.build` a *Graph*, consisting of all modules previously added to a sequential pipeline, is built. 
Before a `Module` is run **all of it's dependencies** have to be completed. In order for the `Module` to be run as soon as possible all *Modules* are sorted.

MAGDA *Graph* employs **topological sorting**. It's a graph ordering algorithm which is viable only when the graph is directed and doesn't have any cycles.

### Disallowed circular dependencies

<CycleSvg className="diagram" width="30%" />

Topological sorting orders the modules resulting in the sequence where every module follows its dependent modules. The algorithm prioritizes the execution of *Modules* that either have no dependencies or are essential for the next `Module` to be run as soon as possible.

*MAGDA* works on basis of a **single connected graph**, so bear in mind that the pipeline cannot consist of multiple disjoint subgraphs. All the modules need to be connected.

### Disallowed disjoint graphs definition

<DisjointSvg className="diagram" width="30%" />

During the pipeline building both disjoint graphs and circular dependencies are checked for, which result in appropriate errors if found.
### Sequential Pipeline flow
Once the pipeline is built and modules are in the correct order, it can be run. As the name of the pipeline suggests, the modules are called in a **sequence**.

Given the example below, *MAGDA* guarantees that module *A* will be run first after which either *B* or *C* is run and then module *D* as the last one. Both  *A -> B -> C -> D* and *A -> C -> B -> D* are possible run sequences. In this case, it doesn't really matter whether module *B* or *C* runs first. What's important, modules *B* and *C* won't run in parallel here (though it could be achieved with the usage of a [Parallel Pipeline](./parallel.md))

<FlowSvg className="diagram" width="30%" />

## Technical details

The schematic creation of a sequential pipeline is as follows:

``` python
from magda.pipeline import SequentialPipeline

# Building a pipeline
builder = SequentialPipeline().add_module(...)
runtime = builder.build(context)

# Running the pipeline
runtime.run(request)
runtime.process(request)
```

## Simple case study

Here is presented a very simple example of a pipeline usage. For more complex examples see [the next section](#blueprints). 

### Creating a pipeline instance

In order for the pipeline to be built and run, it must consist of modules. Having implemented `Module` classes, the modules need to be added to the pipeline instance. This can be achieved with the use of a configuration file or manually from code. For in-depth workflow please refer to [workflow](../modules/workflow.md).

<Tabs
  defaultValue="code"
  values={[
    {label: "Code-first", value: "code"},
    {label: "Config-first", value: "config"},
  ]}
>
<TabItem value="code">

``` python
from magda.pipeline import SequentialPipeline

builder = SequentialPipeline()
builder.add_module(SomeModule('module_a'))
builder.add_module(SomeOtherModule('module_b').depends_on(builder.get_module('module_a')))

runtime = builder.build(context)
```

To avoid using the `builder.get_module` method, the module can be instantiated outside of the pipeline. This way adding modules, through object references, gets more transparent. The following snippet does exactly the same as the one above.

```python
from magda.pipeline import SequentialPipeline

module_a = SomeModule('module_a')
module_b = SomeOtherModule('module_b')

builder = SequentialPipeline()
builder.add_module(module_a)
builder.add_module(module_b.depends_on(module_b))

runtime = builder.build(context)
```

</TabItem>
<TabItem value="config">

``` yaml
modules:
  - name: module_a
    type: some-module
  - name: module_b
    type: some-other-module
    depends_on:
      - module_a
```

The config file is then read, creating a pipeline instance (assuming modules have been registered in the `ModuleFactory` either manually or via the `@register` decorator). 

``` python
runtime = ConfigReader.read(config_file_path, ModuleFactory)
```

It's worth noting that the `ConfigReader.read` method outputs an already built pipeline. It means that first the pipeline is created, modules are added, some additional options and parameters are set, and at the end the `pipeline.build` method is invoked. 
It's important to know how it works as during pipeline building the `bootstrap` method in every module is called, which is described in detail [here](../modules/module.md).

</TabItem>
</Tabs>

### Running the pipeline
The *Pipeline* can work in one of two modes: run or process, which are described in detail [here](../modules/stateful.md). In brief, the run mode sequentially calls the `run` method in every `Module.Runtime` from the *Graph* (which precedes an aggregation module) and the `aggregate` method from the `Module.Aggregate`. All `Module.Runtime` following `Module.Aggregate` are ommited.

During the process mode firstly `process` methods from the aggregation modules are called, which return stored data. Then the `run` method from every runtime module (that succeeds the aggregation module) is invoked. All `Module.Runtime` preceding `Module.Aggregate` are ommited.

:::caution Notice
Every invocation of the `pipeline.process` method clears aggregation modules inner state.
:::

``` python
pipeline.run(request)  # during both `run` and `process` modes requests can be passed to the pipeline
pipeline.run(request)
pipeline.process()     # aggregation module inner state contains 2 elements
pipeline.run(request)
pipeline.process()     # aggregation module inner state contains only 1 element
```

## Blueprints

In the following section, instead of very specific implementations, general blueprints for creating a `SequentialPipeline` are shown. These are working examples with abstracted naming and logic, which can be easily expanded on with concrete details.
The examples will be also briefly commented on how they work and what should be an expected outcome of each one.

### A stateless pipeline based only on `Module.Runtime`

Consider the following pipeline:

<RuntimeSvg className="diagram" width="50%" />

The code is divided into six distinct sections.

1) **Class definitions**<br/>
This is where all classes are defined. Notice that, since the classes are added manually to the pipeline, neither the `@register` decorator nor `ModuleFactory` was used.

<details>
<summary>See code snippet</summary>

```python
from magda.module import Module
from magda.decorators import accept, finalize, expose
from magda.pipeline import SequentialPipeline

@finalize
class ModuleA(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module a output'

@finalize
class ModuleB(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module b output'

@accept(ModuleA, ModuleB)
@finalize
class ModuleC(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module c output'

@accept(ModuleC)
@expose()
@finalize
class ModuleD(Module.Runtime):
    def run(self, *args, **kwargs):
        return self.parameters['important_parameter']

@accept(ModuleC)
@finalize
class ModuleE(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module e output'

@accept(ModuleC, ModuleE)
@expose('module_f_expose_name')
@finalize
class ModuleF(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module f output'
```

</details>

2) **Module initialization**<br/>
Here every module is initialized and a name is set through the constructor. This name is used for `@exposed` classes that don't have an explicit expose name.

<details>
<summary>See code snippet</summary>

```python
module_a = ModuleA('module_a')
module_b = ModuleB('module_b')
module_c = ModuleC('module_c')
module_d = ModuleD('module_d')
module_e = ModuleE('module_e')
module_f = ModuleF('module_f')
```

</details>

3) **Dependency and parameters definition**<br/>
Here all dependencies and additional module parameters should be configured. Thanks to module initialization in the 2nd step, now **it is possible to directly reference modules inside the `depends_on` function.**

<details>
<summary>See code snippet</summary>

```python
module_c.depends_on(module_a).depends_on(module_b)
module_d.depends_on(module_c)
module_e.depends_on(module_c)
module_f.depends_on(module_c).depends_on(module_e)

module_d.set_parameters({'important_parameter': 'ModuleD important output'})
```

</details>

4) **Adding modules to a pipeline**<br/>
Here every initiated module is added to the pipeline. If the class didn't inherit from either `Module.Runtime` or `Module.Aggregate` an error is raised.

<details>
<summary>See code snippet</summary>

```python
builder = SequentialPipeline()
builder.add_module(module_a)
builder.add_module(module_b)
builder.add_module(module_c)
builder.add_module(module_d)
builder.add_module(module_e)
builder.add_module(module_f)
```

</details>

5) **Pipeline build**<br/>
During pipeline `build`, firstly, a Graph is built and validated for cycles and disjoint graphs. If the validation passes then all previously added modules are bootstrapped. If there is any `context` or `shared_parameters` available they can be added as optional pipeline `build` parameters.

```python
runtime = builder.build()
```

6) **Pipeline running**<br/>
When a pipeline is built it is ready to be run. As stated before, there are two ways to execute a pipeline. Here, since there is no `Module.Aggregate` inside a pipeline only the `run` option is applicable. The `run` can also accept a `request` parameter that could be used throughout the pipeline. Since `ModuleD` and `ModuleF` are exposed their results will be available as the pipeline results after a completed `run` function.<br/>
`ModuleD`'s result is available under the name it was initialized ("module_d"), but because there was a name passed to `@expose` when defining `ModuleF` class it was used instead ("module_f_expose_name"). Also, `ModuleD` makes use of passed module parameters (via `self.parameters`), which it just returns. All possible parameters and how to properly use them are explained in detail [here](../modules/module.md).

<details>
<summary>See code snippet</summary>

```python
result = runtime.run()
print(result)  # {'module_f_expose_name': 'module f output', 'module_d': 'ModuleD important output'}
```

</details>

### A stateful pipeline based on both `Module.Runtime` and `Module.Aggregate`
Below a blueprint for a pipeline that can hold a state in between runs is shown.
It has the same structure as the above one. Every step will be described but not as in depth.

Consider the following pipeline:

<AggregateSvg className="diagram" width="50%" />

The code is divided into six distinct sections.

1) **Class definitions**<br/>
All modules besides is defined as `Module.Runtime`, besides `ModuleF` which acts as `Module.Aggregate`.

:::caution Notice
The definition of `ModuleE` and `ModuleG` which have overridden run methods to also include `data` or `request` parameters. It can be done since these two parameters are always passed. Any other can be accessed through `args` or `kwargs`.
:::

<details>
<summary>See code snippet</summary>

```python
from magda.module import Module
from magda.decorators import accept, finalize, expose
from magda.pipeline import SequentialPipeline

@finalize
class ModuleA(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module a output'

@finalize
class ModuleB(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module b output'

@accept(ModuleA, ModuleB)
@finalize
class ModuleC(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module c output'

@accept(ModuleC)
@finalize
class ModuleD(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'module d output'

@accept(ModuleD)
@expose()
@finalize
class ModuleE(Module.Runtime):
    def run(self, request, *args, **kwargs):
        return request

@accept(ModuleC)
@finalize
class ModuleF(Module.Aggregate):
    pass

@accept(ModuleF)
@expose()
@finalize
class ModuleG(Module.Runtime):
    def run(self, data, *args, **kwargs):
        return data
```

</details>

2) **Module initialization**<br/>
All modules have their appropriate name set. Even though `ModuleF` isn't `Module.Runtime` it has the same initialization.

<details>
<summary>See code snippet</summary>

```python
module_a = ModuleA('module_a')
module_b = ModuleB('module_b')
module_c = ModuleC('module_c')
module_d = ModuleD('module_d')
module_e = ModuleE('module_e')
module_f = ModuleF('module_f')
module_g = ModuleG('module_g')
```

</details>

3) **Dependency and parameters definition**<br/>
As before, in order to create a graph all modules have to be connected.

<details>
<summary>See code snippet</summary>

```python
module_c.depends_on(module_a).depends_on(module_b)
module_d.depends_on(module_c)
module_e.depends_on(module_d)
module_f.depends_on(module_c)
module_g.depends_on(module_f)    
```

</details>

4) **Adding modules to a pipeline**<br/>
Here every initiated module is added to the pipeline. If the class didn't inherit from either `Module.Runtime` or `Module.Aggregate` an error is raised.

<details>
<summary>See code snippet</summary>

```python
builder = SequentialPipeline()
builder.add_module(module_a)
builder.add_module(module_b)
builder.add_module(module_c)
builder.add_module(module_d)
builder.add_module(module_e)
builder.add_module(module_f)
builder.add_module(module_g)
```

</details>

5) **Pipeline build**<br/>
The `context` parameter is a dictionary that can hold both primitive types or references as it's values.

```python
runtime = builder.build(context={'context_1': 'context'})
```

6) **Pipeline running**<br/>
The pipeline is run in two states. Firstly the `run` is used twice to execute two requests. The pipeline has two ending branches. One that has exposed result via `ModuleE` and one via `ModuleG`. During both passes only the modules marked as yellow blocks have their `run` method executed. The request is also added to the internal state of `ModuleF` but no further modules are run.<br/>
Afterwards, in order to retrieve and work with stored data, `process` mode is used. It executes `ModuleG` marked as a purple block after getting the collected results from `ModuleF`. The result is a `ResultSet` object containing all previously stored data, about which can be read more [here](../modules/module.md).

:::info Remember
During `process` mode the other branch wasn't executed.
:::

<details>
<summary>See code snippet</summary>

```python
result_run_1 = runtime.run('request_1')
result_run_2 = runtime.run('request_2')
result_process = runtime.process()
print(result_run_1)    # {'module_e': 'request_1'}
print(result_run_2)    # {'module_e': 'request_2'}
print(result_process)  # {'module_g': <magda.module.results.ResultSet>}
```

</details>
