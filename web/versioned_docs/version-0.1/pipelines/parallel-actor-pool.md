---
title: Actor Pool
sidebar_position: 3
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import IdeaSvg from '../assets/actor-pool/idea.svg';
import IdeaTimelineSvg from '../assets/actor-pool/idea-timeline.svg';
import TimelineSequentialSvg from '../assets/actor-pool/timeline-sequential.svg';
import TimelineParallelSvg from '../assets/actor-pool/timeline-parallel.svg';
import TimelineActorPoolSvg from '../assets/actor-pool/timeline-actor-pool.svg';
import Timeline4JobsSvg from '../assets/actor-pool/timeline-4-jobs.svg';


# Parallel Actor Pool

In some cases, the *parallel pipeline* is not enough for efficient processing. E.g. when one of the groups is significantly more demanding than others. The *parallel actor pool* was created to solve these kind of problems.


## Idea

The basic idea of a *parallel actor pool* is **replicating** groups (actors). This enables parallel processing of the same group for different jobs.
It doesn't violate the *parallel pipeline* constraints - each graph is processed sequentialy within a single request (job).
However, as it was mentioned earlier, the *parallel actor pool* replicates groups and graphs within them, which in turn increases the capacity of a pipeline at that certain fragment.

<IdeaSvg className="diagram" width="40%" />


#### Module processing timeline

<IdeaTimelineSvg className="diagram" width="90%" />

The idea is similar to the [ray.ActorPool](https://docs.ray.io/en/master/actors.html#actor-pool).


## Definition

*Actor pool* can be declared by providing `replicas` parameter (alongside other options like `num_gpus`) of a group:

<Tabs
  defaultValue="code"
  values={[
    {label: "Code-first", value: "code"},
    {label: "Config-first", value: "config"},
  ]}
>
<TabItem value="code">

``` python
group = ParallelPipeline.Group('g1', replicas=3, **rest_options)
```

</TabItem>
<TabItem value="config">

```yaml {4}
groups:
  - name: g1
    options:
      replicas: 3
      ...
```

</TabItem>
</Tabs>

On the logical layer, the pipeline will be the same regardless of the number of `replicas`. Each request will be processed **once** by the `g1` group.
However, on the physical layer, `g1` will be spawned **3 times**, allowing **the same logical group to process 3 different requests**.


## Limitations

1. Replication is done on the physical layer - **each physical copy requires its own resources!** If group `g1` requires 1 GPU, then `g1` replicated 4 times requires **4** GPUs.
2. Aggregation modules **cannot be placed** within a replicated group. It does not concern regular modules placed after the aggregation one.
3. An *Actor pool* can be useful **only** for solving pipeline bottlenecks. The time overhead of replicas management can even **slow down** processing in other situations!


## Example

The example is based on two modules definitions:

``` python title="example_modules.py"
class NullInterface(Module.Interface):
    pass

@register('example-module')
@accept(NullInterface)
@finalize()
class ExampleModule(Module.Runtime):
    def run(self, *args, **kwargs):
        time.sleep(1)  # mimics some processing for 1 second
        return NullInterface()

@register('long-running-module')
@accept(NullInterface)
@finalize()
class LongRunningModule(Module.Runtime):
    def run(self, *args, **kwargs):
        time.sleep(5)  # mimics some processing for 5 seconds
        return NullInterface()
```

Using these modules, a simple pipeline is created:

<IdeaSvg className="diagram" width="40%" />

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

# Groups
g2 = ParallelPipeline.Group('g2', replicas=3)

# Modules
a = ExampleModule('A', group='g1')
b = LongRunningModule('B', group='g2')
c = ExampleModule('C', group='g3')

# Build pipeline
pipeline = (
    ParallelPipeline()
    .add_group(g2)
    .add_module(a)
    .add_module(b.depends_on(a))
    .add_module(c.depends_on(b))
    .build(context)
)
```

</TabItem>
<TabItem value="config">

```yaml title="config.yml"
modules:
  - name: A
    type: example-module
    group: g1

  - name: B
    type: long-running-module
    group: g2
    depends_on:
      - A

  - name: C
    type: example-module
    group: g3
    depends_on:
      - B

groups:
  - name: g2
    options:
      replicas: 3
```

```py title="main.py"
from magda import ConfigReader
from magda.module import ModuleFactory

pipeline = ConfigReader.read('./config.yml', ModuleFactory, context=context)
```

</TabItem>
</Tabs>


#### Module processing timeline

<Tabs
  defaultValue="actorpool"
  values={[
    {label: "Actor Pool", value: "actorpool"},
    {label: "Parallel", value: "parallel"},
    {label: "Sequential", value: "sequential"},
  ]}
>
  <TabItem value="actorpool">
    <TimelineActorPoolSvg className="diagram" width="100%" />
  </TabItem>
  <TabItem value="parallel">
    <TimelineParallelSvg className="diagram" width="100%" />
  </TabItem>
  <TabItem value="sequential">
    <TimelineSequentialSvg className="diagram" width="100%" />
  </TabItem>
</Tabs>

The advantage of *actor pool* is visible in the comparison with other pipelines:

| Sequential | Parallel | Parralel Actor Pool |
|:----------:|:--------:|:-------------------:|
| 21 [s]     | 17 [s]   | 9 [s]               |


### Overload

:::info Remember
*Parallel actor pool* can simultaneously process as many different jobs as there are replicas.
:::

In the example above, requesting the 4th job (while having only 3 replicas of `g2`) will significantly slow down the pipeline. `g2` group can process the last job only after finishing any of the currently running jobs, which is shown below.

#### Module processing timeline

<Timeline4JobsSvg className="diagram" width="100%" />
