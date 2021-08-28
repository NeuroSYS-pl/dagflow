---
title: General outlook
sidebar_position: 1
---

import Phases from '../assets/module/phases.svg';


# General outlook

`Module` is the most important element of *MAGDA*. As mentioned in [overview](../home.md), this library is based on graphs so think of modules as nodes. These nodes should execute a single function. They are connected to other nodes by defining input modules, nodes that the current one depends on, and output modules, nodes that the current one sends its results to.

## Module types

There are two types of modules.
- `ModuleRuntime`,
- `ModuleAggregate`.

`ModuleRuntime` is designed to execute one function and pass it's results further.

`ModuleAggregate`'s purpose is to collect results generated from previous `ModuleRuntime`. Without it there would be no way of storing data between each graph traversal.

## Module definition

For a class to become a `Module` of a certain type it's as simple as inheriting from either `ModuleRuntime` or `ModuleAggregate`.

```python
class ModuleThatIsRuntime(Module.Runtime):
    pass

class ModuleThatIsAggregator(Module.Aggregate):
    pass
```

For convenience purposes the base class `Module` has hooks to few classes inside it.
- `ModuleRuntime` can be accessed through `Module.Runtime`,
- `ModuleAggregate` through `Module.Aggregate`,
- `ModuleInterface` through `Module.Interface`.

It's possible to also access result objects via:
- `Module.Result` - result object from a single `Module`
- `Module.ResultSet` - container for all `Module.Result`s

## Parameters

A Module has three ways of accessing data:
- **Context**<br/>designed to allow access to more complex structures and to hold references that you might want to initialize only once and use throughout a pipeline lifecycle, such as a DB connection,

- **Shared parameters**<br/>a dictionary that should contain common, shared data that is available and the same in every module,

- **Module parameters**<br/>a dictionary as well but every `Module` will have its own set of params available only to it.

```python
from magda.pipeline import SequentialPipeline
from magda.module import Module
from magda.decorators import finalize

@finalize
class ModuleExample(Module.Runtime):
    def run(self, data, request, *args, **kwargs):
        print(self.context) # {'DB': 'DB Connection'}
        print(self.parameters) # {'module_param': 'module_param_value'}
        print(self.shared_parameters) # {'shared_param': 'shared_param_value'}
        print(data) # <magda.module.results.ResultSet>
        print(request) # request variable
        return None

builder = SequentialPipeline()
builder.add_module(ModuleExample('module_example').set_parameters({'module_param': 'module_param_value'}))
runtime = builder.build({'DB': 'DB Connection'}, {'shared_param': 'shared_param_value'})
runtime.run('request variable')
```

*Context*, *shared parameters* and *module parameters* are accessible in every `Module` through `self.context`, `self.shared_parameters` and `self.parameters` respectively. They are all set during *pipeline* (so consequently during *module*) build.

Also, a `Module` has access to data from the previous module's output as well as request data through `*args` and `**kwargs`. Here `data`, and `request` has been incorporated into method definition through unpacking operators. These two parameters are always passed, that's why they can be written this way.

:::danger Notice
There is a `return None` at the end of a `run` method. It's necessary to always return something from the method, even if it's `None`.
:::

## Two states of a `Module`

Like pipelines, `Module` can be in one of two states. It can either be in a *builder* state, or in an *execution* state. The first one allows do define how a `Module` should behave, add parameters, define decorators. The latter one is accessed by calling the `build` method which results in an executable module. The result of building a `Module` is one of two objects `Module.Runtime` or `Module.Aggregate`, depending from which one the class inherited.

## Module bootstrapping

Modules has a special function `bootstrap` that acts just as a regular booting function. It cannot be invoked manually but is run at the very start **after a module is built** and is run **only once** per module. It's especially useful when initializing data or loading something from disk which is essential for the `Module` to work properly, such as stored Neural Network weights.

<Phases className="diagram" width="75%" />

``` python
@expose()
@finalize
class ModuleBootstrapedValues(Module.Runtime):
    def run(self, *args, **kwargs):
        return self.initial_value * 10

    def bootstrap(self):
        self.initial_value = 1

@expose()
@accept(ModuleBootstrapedValues)
@finalize
class ModuleBootstrapedWeights(Module.Runtime):
    def run(self, *args, **kwargs):
        return self.weights

    def bootstrap(self):
        self.weights = self.parameters['nn_weights']
        # some logic to load actual network

builder = SequentialPipeline()
builder.add_module(ModuleBootstrapedValues('module values'))
builder.add_module(
    ModuleBootstrapedWeights('module weights')
    .depends_on(builder.get_module('module values'))
    .set_parameters({'nn_weights': [2, 5, 8]})
)
runtime = builder.build()
print(runtime.run()) # {'module values': 10, 'module weights': [2, 5, 8]}
```

The first one, `ModuleBootstrapedValues`, has an additional `self.initial_value` set during bootstrapping, which is later used to calculate an output. The second `Module`, `ModuleBootstrapedWeights`, makes use of a parameter passed to it during building phase. Here it's just used to initialize fake `self.weights` but could normally be used to read or initialize proper NN weights.

## Module teardown

Next to `bootstrap` function, a module may also have implemented `teardown` function. It is run for each module when `Pipeline.close` method is called and is intended to handle some logic in the end of module's lifecycle. An exemplary usecase is closing a connection to database. 

``` python
@expose()
@finalize
class ModuleTeardown(Module.Runtime):
    def run(self, *args, **kwargs):
        return self.initial_value * 10

    def teardown(self):
        # i.e. close here a DB connection
```

## Decorators

Multiple decorators were designed to allow for quick definition of most important `Module` behaviors.

- `@finalize`
- `@accept`
- `@produce`
- `@expose`
- `@register`

Generally speaking, the order of decorators doesn't matter, but there is an exception, `@finalize` decorator is a special case.

### `@finalize`

This is the most important decorator. It's job is to take a class, which inherits from `Module.Runtime` or `Module.Aggregate` and convert it to a *builder* state, which can be further customized.

The code must comply with the following rules:
- `@finalize` does not take any explicit parameters and is invoked without parentheses `()`, since it works on and references a class directly,
- when defining multiple decorators, **`@finalize` must always be the last one**, when multiple decorators are defined above a class, **order of other ones does not matter**,
- `@finalize` is always required, the other ones are not.

### `@accept` and `@produce`

The role of `@accept` is to define the allowed input type for a given `Module`. Similarly, `@produce` specifies the resulting output type of a class. But while `@accept` can allow an input to be of type `Module` or `Module.Interface`, `@produce` can create results of type `Module.Interface`.

The main idea behind `@produce` and `@accept` is to allow for a common data interfaces. 

The `@produce` signature is as follows:
``` python
@produce(interface_class: Module.Interface)
```
and `@accept` signature:
``` python
@accept(*ancestors: Iterable[Module], self=False)
```

A `Module` can have multiple different ancestors. Also, the same type of `Module` can be chained multiple times and for this the optional parameter `self=True` ought to be used. For more on `@accept` and `@produce` parameters please refer to [Module workflow](./workflow.md) and to [Interfaces](./interfaces.md).

### `@expose`

If it's necessary to keep a given `Module`'s output to be accessed at the end of a pipeline `@expose` decorator should be used. Exposing a `Module` is a way of keeping it's output in a `ResultSet` under a given name. This can be especially useful for logging purposes or exporting data after each run from a certain point of interest in a graph.

The names must be unique throught the pipeline, and if the `name` parameter is not set the name given during the `Module` initialization will is used instead.

**Remember that even though there is no other module after the last one, in order for its result to show up after the pipeline the `Module` has to be exposed.**

``` python
@expose('important_module')
@finalize
class ExposedModuleNamed(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'ExposedModuleNamed output'

@expose()
@accept(ExposedModuleNamed)
@finalize
class ExposedModule(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'ExposedModule output'

builder = SequentialPipeline()
builder.add_module(ExposedModuleNamed('module_named'))
builder.add_module(ExposedModule('module_unnamed').depends_on(builder.get_module('module_named')))
runtime = builder.build()
print(runtime.run())  # {'important_module': 'ExposedModuleNamed output', 'module_unnamed': 'ExposedModule output'}
```

Both `Module`'s outputs will be available after the completed graph traversal, but `ExposedModuleNamed` output will be accessible under the name `important_module`, while `ExposedModule` output will be available under `module_unnamed`.

### `@register`

The `@register` decorator's job is to create a mapping between a given name (a string) and a class which can be then used by `ModuleFactory`.
Decorator `@register` is directly connected to `ModuleFactory`, which is used when building a pipeline via config files.

```python
@register('decorator-register')
@finalize
class ModuleRegisterDecorator(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'some value'

@finalize
class ModuleRegisterManual(Module.Runtime):
    def run(self, *args, **kwargs):
        return 'some more values'

ModuleFactory.register('manual-register', ModuleRegisterManual)

print(ModuleFactory.get('manual-register'))  # <class ModuleRegisterManual.Builder>
print(ModuleFactory.get('decorator-register'))  # <class ModuleRegisterDecorator.Builder>

ModuleFactory.unregister('decorator-register')

print(ModuleFactory.get('manual-register'))  # <class ModuleRegisterManual.Builder>
print(ModuleFactory.get('decorator-register'))  # KeyError: 'decorator-register'
```

As shown, when unregistering a `Module` other references are still present but when accessing `decorator-register` class it's no longer available and throws an error. Althought not mandatory, the use of kebab-case for `Module` registering is recommended. 

## Modules in config

Even though, in yaml files it's not feasible to create a `Module`, it's possible to specify a wanted module, via the `type` parameter.
In order to use a class as a `type` it needs to be registered with a `ModuleFactory`. 

`ModuleFactory` provides two methods - `register` and `unregister`. The first one can be called via both @register decorator or coded manually, the latter one only via code. The registered name points to an earlier predefined `Module`. 

This `type` will be then looked for in `ModuleFactory` and a class corresponding to the name will be dynamically built.
It's vital to pass a `ModuleFactory` class to a `ConfigReader.read` method for it to work properly.

For more information on how to work with config files please refer to the [configuration file documentation page](../configuration/config-file.md).

:::tip Pro Tip
If using a decorator approach it's enough to import all classes annotated with `@register`. The method will be run while importing. If using the manual approach it's necessary to register each `Module` individually with `ModuleFactory`, as shown above.
:::
