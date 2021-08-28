---
title: Parallel
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import FlowSvg from '../assets/parallel/flow.svg';
import SimpleSvg from '../assets/parallel/simple.svg';
import SimpleTimelineSvg from '../assets/parallel/simple-timeline.svg';
import ResourcesSvg from '../assets/parallel/resources.svg';
import ResourcesTimelineSvg from '../assets/parallel/resources-timeline.svg';


# Parallel Pipeline

The second type of pipeline (besides the *sequential pipeline*) is a *parallel pipeline*. Its main goal is optimization by setting up some *parts* of the flow to be processed in parallel.


## Big picture

*Parallel pipeline* is a composition of *groups*. Similar to the modules in the sequential pipeline, the groups are linked together, creating a *directed acyclic graph*.
What's more, the sequential pipeline can be transformed into *parallel* one by grouping together its modules. Then, these blocks can be processed at the same time.

Parallel processing is done via [ray library](https://github.com/ray-project/ray). It is responsible for cluster management, resource provisioning and rest low-level operations.
*MAGDA* focuses on combining it in a proper execution order and exposing a user-friendly interface suited for modular processing.


### What is a Group?

*Group* is a block of actions (a group of modules) that must be processed together **by the same process**. Under the hood, a group is just a *graph* from the sequential pipeline, which invokes modules one-by-one. Its module dependencies are resolved in the same manner as in the sequential pipeline.

The true advantage of a *group* is visible from the bigger perspective. Modules within a group are processed sequentially, while groups **can be** processed in parallel (it depends on the flow described below) by different processes and even different nodes in a cluster.
Group can be identified as an [Actor](https://docs.ray.io/en/latest/actors.html), which claims hardware resources (CPU/GPU) and invokes its modules within the same node and thread.


### Parallel flow

*Groups* and *modules* have the same logic behind their starting conditions: *dependencies*. The parallel pipeline is a manager, which invokes every group (sequentially calls all modules within the group) as soon as all its dependencies are resolved.
Each group determines its dependencies (the *starting condition*). It is the list of all modules on which that group modules **depend on** and are **not within** the same group. In simple words: all links to the group's modules, which starts outside that group.
A group starts processing as soon as results from all preceding modules are available.

<FlowSvg className="diagram" width="25%" />

As seen above, groups *g1* and *g2* don't have any dependencies.
It means that their starting conditions are already fulfilled and they can immediately begin processing.
Group *g3* will begin processing as soon as results from modules *A* from *g1* and *C* from *g2* are available (despite that, the *B* module in the *g1* group can still be processing).


## Technical details

### Pipeline

The parallel pipeline can be built and used very similarly to the sequential pipeline.

``` python
from magda.pipeline.parallel import ParallelPipeline

# Building the parallel pipeline
builder = (
    ParallelPipeline()
        .add_module(...)
        .add_module(...)
        .add_module(...)
)
pipeline = builder.build(context)

# Usage - pipeline.run is coroutine so "await" is required
await pipeline.run(job)
```

The main difference is underneath. The parallel pipeline is based on *Actors* from the *ray* library. Therefore, the low-level operations are managed by the *ray cluster* and require **initialization at the very beginning of the application**:

``` python
magda.pipeline.parallel.init(*args, **kwargs)
```

This function wraps [ray.init](https://docs.ray.io/en/master/package-ref.html#ray-init) and passes all arguments to *ray*.

The main usage difference is that, the `ParallelPipeline.run` is an `asyncio.coroutine`. It means, that invoking the function is subject to asynchronous programming in [asyncio](https://docs.python.org/3/library/asyncio.html). It gives a huge variety of possibilities in jobs management, like delegating multiple jobs at once:

``` python
tasks = [
    asyncio.create_task(pipeline.run(job))
    for job in jobs_to_process
]
results = await asyncio.gather(*tasks)
```

This example creates jobs for the pipeline simultaneously. They will be processed in parallel, following the order and restriction (groups replicas and resources) set by the *parallel pipeline*.
After all jobs are finished, the output will be returned to the `results` as a `List`. The `results` order will be the same as jobs in the `tasks` i.e. `results[0]` corresponds to the `jobs_to_process[0]`, sencond result to the second job etc.

:::tip Pro Tip
Check [AsyncIO Documentation](https://docs.python.org/3/library/asyncio.html) for more operators and design patterns.
:::


### Group

``` python
magda.pipeline.parallel.ParallelPipeline.Group(
    name: str, *, replicas: int = 1, **rest_ray_options,
)
```

**Parameters:**
- **name** *(str)* - name of a group used in a config file or in code
- **replicas** *(int) (Optional)* - number of group (and modules) copies required by the *ParallelPool*

**Most common ray options:**
- **num_cpus** *(int) (Optional)* - number of CPU cores assigned to a group/actor
- **num_gpus** *(float) (Optional)* - the quantity of GPUs assigned to a group/actor
- **rest** *(kwargs)* - [ray.remote documentation](https://docs.ray.io/en/master/package-ref.html#ray-remote)

Each module in a *parallel pipeline* **must be assigned** to one of the groups.
Each group can be created in 2 ways:
- **manually**<br/>
*(definition added to the config/code)*<br/>
By passing additional arguments like resource requirements for *ray* or number of copies for the *ParallelPool*.

- **automatically**<br/>
*(definition skipped in the config/code but group referenced by at least one module)*<br/>
All parameters are set to default values.

These methods can be mixed within the single pipeline, i.e. one group is created manually and the rest are skipped (created automatically).

``` python
# Simple group
group1 = ParallelPipeline.Group('g1')

# Group with resource declaration
group2 = ParallelPipeline.Group('g2', num_gpus=1)

pipeline = (
    ParallelPipeline()
        .add_group(group1)
        .add_group(group2)
        # Other groups (to which modules are assigned to)
        #   will be created automatically as "simple groups" (like "g1")
        ...
)
```

The same can be achieved with a config file:

```yaml
modules: ...

groups:
  - name: g1
  - name: g2
    options:
      num_gpus: 1.0
```


## Examples

Each example uses the same module definition:

``` python title="example_module.py"
@register('example-module')
@accept(self=True)
@finalize()
class ExampleModule(Module.Runtime):
    def run(self, *args, **kwargs):
        time.sleep(1)  # Mimics some processing for 1 second
        return None
```


### Simple parallel pipeline

<SimpleSvg className="diagram" width="40%" />

#### Module processing timeline

<SimpleTimelineSvg className="diagram" width="90%" />

*Sequential pipeline* processes these 3 jobs in 15 seconds (3 jobs × 5 modules × 1 second each) while the *parallel* one interlaces jobs execution and finishes approximately after **8 seconds**.

<Tabs
  defaultValue="code"
  values={[
    {label: "Code-first", value: "code"},
    {label: "Config-first", value: "config"},
  ]}
>
<TabItem value="code">

``` python title="main.py"
from magda.pipeline.parallel import init, ParallelPipeline

# Initialize ray
init()

# Build pipeline
builder = ParallelPipeline()

# Group g1
builder.add_module(ExampleModule('A', group='g1'))
builder.add_module(
  ExampleModule('B', group='g1')
  .depends_on(builder.get_module('A'))
)

# Group g2
builder.add_module(ExampleModule('C', group='g2'))

# Group g3
builder.add_module(
  ExampleModule('D', group='g3')
  .depends_on(builder.get_module('B'))
  .depends_on(builder.get_module('C'))
)
builder.add_module(
  ExampleModule('E', group='g3')
  .depends_on(builder.get_module('D'))
)

# Build pipeline
pipeline = builder.build(context)

# Run 3 parallel jobs
result = await asyncio.gather(
  asyncio.create_task(pipeline.run('Job1')),
  asyncio.create_task(pipeline.run('Job2')),
  asyncio.create_task(pipeline.run('Job3')),
)
```

</TabItem>
<TabItem value="config">

```yml title="config.yml"
modules:
  - name: A
    type: example-module
    group: g1

  - name: B
    type: example-module
    group: g1
    depends_on:
      - A

  - name: C
    type: example-module
    group: g2

  - name: D
    type: example-module
    group: g3
    depends_on:
      - B
      - C

  - name: E
    type: example-module
    group: g3
    depends_on:
      - D
```

``` python title="main.py"
from magda import ConfigReader
from magda.module import ModuleFactory

pipeline = ConfigReader.read('./config.yml', ModuleFactory, context=context)
result = await pipeline.run(job)
```

</TabItem>
</Tabs>


### Resource claim

<ResourcesSvg className="diagram" width="20%" />

#### Module processing timeline

<ResourcesTimelineSvg className="diagram" width="50%" />

*Sequential pipeline* processes these 3 jobs in 9 seconds (3 jobs × 3 modules × 1 second each) while the *parallel* one interlaces jobs execution and finishes approximately after **4 seconds**.

<Tabs
  defaultValue="code"
  values={[
    {label: "Code-first", value: "code"},
    {label: "Config-first", value: "config"},
  ]}
>
<TabItem value="code">

```python title="main.py"
from magda.pipeline.parallel import init, ParallelPipeline

# Initialize ray
init()

# Build pipeline
builder = ParallelPipeline()

# Group g1
# highlight-start
builder.add_group(ParallelPipeline.Group('g1', num_gpus=1.0))
# highlight-end
builder.add_module(ExampleModule('A', group='g1'))

# Group g2
# highlight-start
builder.add_group(ParallelPipeline.Group('g2', num_cpus=4, num_gpus=0.5))
# highlight-end
builder.add_module(ExampleModule('B', group='g2'))

# Group g3
builder.add_group(ParallelPipeline.Group('g3')) # Optional line
builder.add_module(
  ExampleModule('C', group='g3')
  .depends_on(builder.get_module('A'))
  .depends_on(builder.get_module('B'))
)

# Build pipeline
pipeline = builder.build(context)

# Run 3 parallel jobs
result = await asyncio.gather(
  asyncio.create_task(pipeline.run('Job1')),
  asyncio.create_task(pipeline.run('Job2')),
  asyncio.create_task(pipeline.run('Job3')),
)
```

</TabItem>
<TabItem value="config">

```yml {20,23-24} title="config.yml"
modules:
  - name: A
    type: example-module
    group: g1

  - name: B
    type: example-module
    group: g2

  - name: C
    type: example-module
    group: g3
    depends_on:
      - A
      - B

groups:
  - name: g1
    options:
      num_gpus: 1.0
  - name: g2
    options:
      num_cpus: 4
      num_gpus: 0.5
```

``` python title="main.py"
from magda import ConfigReader
from magda.module import ModuleFactory

pipeline = ConfigReader.read('./config.yml', ModuleFactory, context=context)
result = await pipeline.run(job)
```

</TabItem>
</Tabs>
