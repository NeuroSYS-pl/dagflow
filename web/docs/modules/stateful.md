---
title: Stateful Modules
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import FlowSvg from '../assets/stateful/stateful-stateless.svg';
import Example1Svg from '../assets/stateful/01.svg';
import Example2Svg from '../assets/stateful/02.svg';
import Example3Svg from '../assets/stateful/03.svg';


# Stateless vs. stateful processing

So far only regular stateless modules were described. Stateless because they don't store any data between requests. Even though they have their advantages, sometimes there is a need to remember some information after every `pipeline.run` excecution.
For instance, collecting statistics. This can't be achieved without storing results from previous runs. That's why there is a need for stateful modules which in *MAGDA* are called the aggregation modules. 


## Pipeline modes

The pipeline can work in one of two modes: **run** or **process**. 

### Auxiliary notions

Before discussing any more complex pipelines, let's define a few commonly used terms:

- **Regular module**  
  A module that inherits from `Module.Runtime`. It is stateless.  
  If it precedes an aggregation module it's marked as a yellow box on any diagram and is executed only during `pipeline.run`.  
  When it follows an aggregation module it's marked as a purple box. Then it's only executed during the `pipeline.process` and works on the collected data.

- **Aggregation module**  
  A module that inherits from `ModuleAggregate` and aggregates a preceding module's outputs. It is stateful. It's marked as a purple box on any diagram (additionally with *(Agg)*).


### Run mode

The run mode calls the `run` method in every regular module and passes an output of each module as an input to the next one.
Since regular modules are stateless there is no way to keep their results after each request. Hence the need for the aggregation modules. They are the ones that keep the state and aim to collect outputs of preceding modules.
The method that is responsible for adding data to an internal state in aggregation modules is `aggregate`.

```python
def aggregate(self, data: ResultSet):
    self.add_data(data)
    return self.state
```

What's important is that all regular modules that succeed aggregation modules (the purple ones) are not considered during the `run` mode at all. Aggregation modules do not pass their output further - it's always the last module that is invoked on a particular pipeline branch in this mode (as shown in [the diagram below](#exemplary-diagram)).


### Process mode

The `process` mode is used to process results aggregated during the `run` mode. All regular modules that precede the aggregation modules (the yellow ones) are omitted in this mode.  
The first modules to run are the aggregation modules which pass their full state (all of the previously aggregated results) to the following modules. To achieve that they invoke the `process` method.
It's worth noting that once called, this method **clears the aggregation module's inner state**. It means that invoking `pipeline.process` twice in a row doesn't make much sense as during the second call aggregation modules won't have any data to pass forward.

```python
def process(self, data: ResultSet):
    state_copy = self.state.copy()
    self.clear_state()
    return state_copy
```
Aggregation modules pass their state to the succeeding regular modules (the purple ones) which can then process or transform it, save to file, etc.


## Exemplary diagram

Diagram presents a simplified example of a pipeline consisting of both regular and aggregation modules.

<FlowSvg className="diagram" width="50%" />


## `ModuleAggregate` usage

The capabilities of aggregation modules are presented below with simple examples, which show multiple ways in which they can be used. But first, let's declare some common code.

### Common code

First, it's necessary to declare module classes. Supposing that, the pipeline consists of only two modules: one regular module and one aggregation module.
The regular module returns the value of its parameter `val` multiplied by the request - the value passed during `pipeline.run(request)` invocation.

```python
from magda.module import Module
from magda.decorators import finalize, accept, expose 

@expose()
@finalize
class RegularModule(Module.Runtime):
    def run(self, request, *args, **kwargs):
        return self.parameters['val'] * request

@expose()
@accept(RegularModule)
@finalize
class AggregationModule(Module.Aggregate):
    pass
```

The next step is to build a pipeline. For simplicity, a sequential pipeline which consists of only two modules is declared - a regular module that passes its output forward and an aggregate module that collects data.

<Tabs
  defaultValue="code"
  values={[
    {label: "Code-first", value: "code"},
    {label: "Config-first", value: "config"},
  ]}
>
<TabItem value="code">

```python
from magda.pipeline import SequentialPipeline

reg_mod = RegularModule('reg_mod').set_parameters({'val': 11})
agg_mod = AggregationModule('agg_mod').depends_on(reg_mod)

builder = SequentialPipeline()
builder.add_module(reg_mod)
builder.add_module(agg_mod)
pipeline = builder.build()
```

</TabItem>
<TabItem value="config">

```yaml
modules:
- name: reg_mod
    type: regular-module
    parameters: 
    val: 11
- name: agg_mod
    type: aggregation-module
    depends_on:
    - reg_mod
```

However, in order to use the config in code we would have to register previously declared modules in the `ModuleFactory` and then read the config like this:

```python 
from magda.config_reader import ConfigReader
from magda.module import ModuleFactory

ModuleFactory.register('regular-module', RegularModule)
ModuleFactory.register('aggregation-module', AggregationModule)
pipeline = ConfigReader.read(config_file_path, ModuleFactory)
```

</TabItem>
</Tabs>

The `reg_mod` has an additional parameter called `val`. It outputs its value multiplied by the request value which is passed as an input to the aggregation module.

The last step is running the pipeline. Running it twice in the run mode and once in the process mode should give a clear picture of what happens. 

```python
run1_output = pipeline.run(10)
run2_output = pipeline.run(20)
process_output = pipeline.process()

print(f"Process: {process_output}")
```

Now everything's set - how to declare modules and how to build and execute a pipeline.
In the examples below the only code fragment that changes is the declaration of the `AggregationModule` - we want to show how its modification results in different outputs.


### Small simplification

Before we run the first example let's look at the output of our aggregation module:

```python
Process: {'agg_mod': [<magda.module.results.ResultSet object at 0x7f2d3826dc70>, <magda.module.results.ResultSet object at 0x7f2d3826dac0>]}
```
It's not exactly what we expected. This form of output gives us no meaningful information. We can see that the aggregation module collected indeed two `ResultSet` but we're not able to verify their content or value. That's why (for clarity only) we're going to change just the output of our aggregation module:

```python 
class AggregationModule(Module.Aggregate):
    def aggregate(self, data: ResultSet, **kwargs):
        self.add_data(data.get(RegularModule))  # instead of self.add_data(data)
        return self.state
```
Instead of `data` we now add `data.get(RegularModule)` to the inner state - any output from the `RegularModule` that came as an input to the `AggregationModule`. Now the output looks much more clearly: 

```python
Process: {'agg_mod': [110, 220]}
```
On this account, we'll stick to this small "simplification" in all the following examples.


## Example 1 - the original implementation

The aggregation module collects its inputs in its inner state during run modes and passes the state further during process mode (clearing it). If we run the above code we'll get the output we just saw:

```python
Process: {'agg_mod': [110, 220]}
```

### Running the pipeline differently

Using this simple example we are dealing with right now, we can additionally show how the pipeline (and the modules) behaves when we call `pipeline.run` and `pipeline.process` multiple times. Let's assume just for a moment that we invoke this code fragment:

```python
run1_output = pipeline.run(10)
run2_output = pipeline.run(20)
process1_output = pipeline.process()
run3_output = pipeline.run(40)
process2_output = pipeline.process()

print(f"Run(10): {run1_output}")
print(f"Run(20): {run2_output}")
print(f"Process: {process1_output}")
print(f"Run(40): {run3_output}")
print(f"Process: {process2_output}")
```

We get the following output:
```python
Run(10): {'reg_mod': 110}
Run(20): {'reg_mod': 220}
Process: {'agg_mod': [110, 220]}
Run(40): {'reg_mod': 440}
Process: {'agg_mod': [440]}
```
As mentioned before, the aggregation module clears its state after every process mode. We can see that the second `pipeline.process` outputs a list with only one element (not three) - the output of the regular module which was obtained during the last `pipeline.run(40)`. This behavior is also visibly shown in the diagram below.

<Example1Svg className="diagram" width="50%" />


## Example 2 - overriding the `process` method

The aggregation module can not only serve to collect data and pass it further. It's only its basic implementation. It can also process its inputs. To achieve that we can override the `process` method:

```python
class AggregationModule(Module.Aggregate):
    def process(self, data: ResultSet, **kwargs):
        state_copy = self._current_state.copy()
        self.clear_state()
        return sum(state_copy)
```
The only change that was applied here is the return statement. We output not the list of objects but its sum. As simple as that:

```python
Process: {'agg_mod': 330}
```

We can also visualize this behavior using a diagram.

<Example2Svg className="diagram" width="50%" />


## Example 3 - overriding both `aggregate` and `process` methods

What if we would like to take full advantage of the aggregation module? We may not need to store all data within the module's inner state meaning that our `aggregate` method could behave like a reduce operation:

```python
class AggregationModule(Module.Aggregate):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._current_state = 0  # instead of []

    def aggregate(self, data: ResultSet, **kwargs):
        self._current_state += data.get(RegularModule)
        return self._current_state
```

<details>
  <summary>The code fragment above could be replaced with the one which doesn't need to override the aggregate method.</summary>

  We could override the `add_data` method leaving the original implementation of the `aggregate` method.

  ```python
  class AggregationModule(Module.Aggregate):
      def __init__(self, *args, **kwargs):
          super().__init__(*args, **kwargs)
          self._current_state = 0  # instead of []

      def aggregate(self, data: ResultSet, **kwargs):  # "original" implementation
          self.add_data(data.get(RegularModule))
          return self.state

      def add_data(self, value):
          self._current_state += value  # instead of self._current_state.append(value)
  ```
</details>

Now the `aggregate` method does not collect the data but constantly sums it. So the `process` method can perform yet another operation.
Because *MAGDA* supports the use of interfaces we may want to transform the aggregated sum to an object of a particular structure. Let's implement the simplest version of a class we'll call `ItemsSum`. Our `process` method might now looks like this:

```python
from magda.module import Module

class ItemsSum(Module.Interface):
    def __init__(self, val: int, *args, **kwargs):
        self.sum = val

    def __repr__(self):  # in order to display the value nicely
        return f"ItemsSum({self.sum})"

class AggregationModule(Module.Aggregate):
    
    (...)

    def process(self, data: ResultSet, **kwargs):
        state_copy = self._current_state
        self.clear_state()
        return ItemsSum(state_copy)

    def clear_state(self):
        self._current_state = 0  # instead of []
```

Our `AggregationModule` can now pass its output to some other module which would only accept an input that inherits from the `ItemsSum` class. The output now looks like this:

```python
Process: {'agg_mod': ItemsSum(330)}
```

We can visualize this example in a diagram as well.

<Example3Svg className="diagram" width="50%" />
