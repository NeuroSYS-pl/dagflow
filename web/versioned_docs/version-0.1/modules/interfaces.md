---
title: Interfaces
sidebar_position: 3
---

# Interfaces

*MAGDA* interfaces are not strict interfaces known from programming languages but classes encapsulating results that are passed between modules. They can be understood as a data contract. However, the main concept is still similar to real interfaces - it is a way to accept multiple predecessors of a module that returns a result of the same data type.

## Idea

An exemplary use case is when a given module is preceded by different modules having different logic in every experiment pipeline, but always expecting the same data type. A solution for that could be to assign a common interface for predecessors' results and accept such interface as input in the given module. See the example below:
 
<!-- <img src="images/interfaces/interfaces_example.png" alt="sequential_agg_pipeline" width="500"/> -->

`TeapotModule` accepts predecessors returning `TeaLeaves`. Alternative types of tea, that can be used in separate runtimes, have to produce results exactly of the same kind.

Interfaces can be defined as easily as:

```python
from magda.module.module import Module

# Common interface
class TeaLeaves(Module.Interface):
    ...

# Types of tea in alphabetical order
@register('black-tea')
@produce(TeaLeaves)
@finalize
class BlackTeaGetter(Module.Runtime):
    def run(self, *args, **kwargs):
        ...

@register('green-tea')
@produce(TeaLeaves)
@finalize
class GreenTeaGetter(Module.Runtime):
    def run(self, *args, **kwargs):
        ...

@register('white-tea')
@produce(TeaLeaves)
@finalize
class WhiteTeaGetter(Module.Runtime):
    def run(self, *args, **kwargs):
        ...

# Final module
@register('teapot')
@accept(TeaLeaves)
@finalize
class TeapotModule(Module.Runtime):
    def run(self, *args, **kwargs):
        ...
```

An interface has to inherit from the `Module.Interface` class. `TeapotModule` implements `Module.Runtime`, which can take as its input only modules that produce results coherent with the data type it accepts (in this case `TeaLeaves`). This has to be defined using the `@accept` decorator before the definition of `TeapotModule` and the `@produce` decorator in *tea getters*.

A following, YAML configuration file is consistent with the example above and is a valid pipeline structure:

```yaml
modules:
  - name: tea-getter
    type: green-tea

  - name: small-teapot
    type: teapot
    depends_on:
      - tea-getter
```

Modules: `tea-getter` is of type `GreenTeaGetter`, that produces a correct data interface (`TeaLeaves`), which is acceptable by `TeapotModule`. That is why it can be a valid predecessor of `small-teapot`.

## Decorator `@produce`

We use `@produce` decorator above the definition of a class in order to specify the interface of the module's output. The `@produce` decorator may accept only a single interface and the result returned in this module's `run` function always has to be of that interface. `@produce` will be described in detail in the section devoted to `Modules`. Below you can see a correct definition of a submodule. 

```python
@produce(CorrectDataInterface)
@finalize
class Submodule(Module.Runtime):
    def run(self, *args, **kwargs):
        important_string = 'This is important'
        # some logic happens here
        return CorrectDataInterface(important_string)
```

`Submodule` should produce a result of type `CorrectDataInterface`, which is done in the `run` function.

## Do all single values have to be encapsulated in interfaces?

When the `@produce` decorator is not defined, a module can return a value of any type, as shown below:

```python
@finalize
class Submodule(Module.Runtime):
    def run(self, *args, **kwargs):
        important_magic_integer = 7
        # some logic happens here
        return important_magic_integer
```

However, when `@produce` decorator was defined, even a value of primitive type has to be encapsulated in an interface. In such a case, this is the only valid data contract in *MAGDA*. Even if it is a float or an integer, it has to be enclosed within an object.

```python
@produce(CustomInteger)
@finalize
class Submodule(Module.Runtime):
    def run(self, *args, **kwargs):
        important_magic_integer = 7
        # some logic happens here
        return CustomInteger(important_magic_integer)
```

## Examples of incorrect usage

Please, have a look at the following examples that are **incorrect**. 

**A module cannot produce multiple interfaces:**

```python
@produce(CorrectInterface, AdditionalCorrectInterface) # Wrong: should be just one argument
@finalize
class ModuleIncorrectSample(Module.Runtime):
    ...
```


**A module cannot produce not an interface (i.e. another module):**

```python
@finalize
class CorrectAnotherModule(Module.Runtime):
    ...

@produce(CorrectAnotherModule) # Wrong: should be a class inheriting from ModuleInterface
@finalize
class ModuleSample(Module.Runtime):
    ...
```

**A module cannot produce an interface that is not inheriting from `ModuleInterface`:**

```python
class IncorrectInterface(ABC):
    ...

@produce(IncorrectInterface) # Wrong: should be a class inheriting from ModuleInterface
@finalize
class ModuleIncorrectSample(Module.Runtime):
    ...
```


**A module cannot produce null or empty  interface:**

```python
@produce(None)  # Wrong: should be an interface
@finalize
class ModuleIncorrectSample(Module.Runtime):
    ...

@produce()  # Wrong: should be skipped
@finalize
class ModuleIncorrectAnotherSample(Module.Runtime):
    ...
```


## A correct advanced example

Below you can find a more complicated, correct example for an imaginary pipeline of making a *Caffè Americano*. For details and questions related to accessing results from concrete modules by their interfaces, refer to [Modules](./module.md).

```python
from magda.decorators import register, finalize, accept, produce, expose
from magda.module import Module

class Liquid(Module.Interface):
    def __init__(self, liquid_type: str, volume: str):
        self.liquid_type = liquid_type
        self.volume = volume

@register('coffee-machine-dispenser')
@produce(Liquid)
@finalize
class CoffeeMachineDispenser(Module.Runtime):
    def run(self, data, request, *args, **kwargs):
        coffee_type = self.shared_parameters['coffee']
        liquid_type = self.parameters['liquid']
        # Returns agreed (@produce) interface
        return Liquid(
            liquid_type=liquid_type,
            volume=self.get_volume(coffee_type, request),
        )

@register('mug')
@accept(Liquid)
@finalize
class Mug(Module.Runtime):
    def run(self, data, *args, **kwargs):
        parts = data.of(Liquid)  # the results of modules producing liquids
        fluid_mechanics = self.context['fluid-mechanics']
        content = fluid_mechanics.mix(parts)
        return content
```

```yaml
modules:
  - name: coffee-dispenser
    type: coffee-machine-dispenser
    parameters:
      liquid: coffee

  - name: water-dispenser
    type: coffee-machine-dispenser
    parameters:
      liquid: water

  - name: coding-mug
    type: mug
    expose: caffe-americano
    depends_on:
      - coffee-dispenser
      - water-dispenser

shared_parameters:
  coffee: americano
```
