---
title: Workflow
sidebar_position: 2
---

# Workflow

This section describes how to build dependencies between modules and how to access results from their ancestors. 

## Idea

The main idea of building a modules dependency graph (*Directed Acyclic Graph*) is to specify the predecessors of a given module either from code or from a configuration file. Modules are obliged to have a proper input specified in the `@accept` decorator, which can take a class name or a data interface to be returned (described in detail on the page [Interfaces](./interfaces.md)). 

This page covers three key aspects:
 
1. How to define acceptable predecessors.
2. How to assign a concrete predecessor from code or from config.
3. How to access the predecessors' results in code of the given module.
 

## How to define acceptable predecessors - decorator `@accept`

We use the `@accept` decorator above the definition of the module class in order to specify the list of accepted predecessors of the module. The decorator can take the name of another Module class, the module itself or an interface of accepted data type that the predecessor may return. Single or multiple entities might be passed to the decorator. 

Below you can see the correct ways of specifying the predecessors of a module: 

**A module can accept another module:**

```python
@finalize
class SamplePredecessorModule(Module.Runtime):
    def run(self, *args, **kwargs):
        # some predecessors logic happens here
        ...

@accept(SamplePredecessorModule)
@finalize
class ModuleSample(Module.Runtime):
    def run(self, *args, **kwargs):
        # some logic happens here
        ...
```

**A module can accept a module of the same type:**

In this case `self=True` has to be included in the decorator

```python
@accept(self=True)
@finalize
class ModuleSample(Module.Runtime):
    def run(self, *args, **kwargs):
        # some logic happens here
        ...
```

**A module can accept a data interface:**

```python
from magda.module import Module

class CorrectDataInterface(Module.Interface):
    pass

@accept(CorrectDataInterface)
@finalize
class ModuleSample(Module.Runtime):
    def run(self, *args, **kwargs):
        # some logic happens here
        ...
```

**A module can accept multiple modules and interfaces at once:**
```python
@finalize
class SampleAnotherPredecessorModule(Module.Runtime):
    def run(self, *args, **kwargs):
        # some another predecessors logic happens here
        ...

@accept(SamplePredecessorModule, SampleAnotherPredecessorModule, CorrectDataInterface)
@finalize
class AnotherModuleSample(Module.Runtime):
    def run(self, *args, **kwargs):
        # some logic happens here
        ...
```

On the other hand, below you can see examples with entities that cannot be passed to the `@accept` decorator.

**A module cannot accept an interface other than `Module.Interface`:**
```python
class IncorrectInterface(ABC):
    pass

@accept(IncorrectInterface) # Wrong: interface other than MAGDA is passed
@finalize
class  ModuleSample(Module.Runtime):
    def run(self, *args, **kwargs):
        # some logic happens here
        ...
```

**A module cannot accept a `None` value**
```python
@accept(None) # Wrong: None value cannot be passed to @accept decorator
@finalize
class  ModuleSample(Module.Runtime):
    def run(self, *args, **kwargs):
        # some logic happens here
        ...
```

Having specified accepted inputs of particular modules, the next step is to define the structure of the pipeline by creating dependencies of concrete instances. Keep in mind, that the pipeline's graph will not be created when dependencies of wrong types are assigned to the modules. This restriction cannot be omitted in any way. 


## How to define dependencies

*TODO*


##  How to access results returned by ancestor modules

In the simplest case, the output from a specific module can be accessed in the `run` function of another module via the `data` parameter that is of the `ResultSet` type. `ResultSet` has the following methods, for accessing the results, implemented:

- `get` - returns a single, plain result (without metadata) from a specified module. It raises an Exception when the identifier passed to the method refers to many results,
- `has` - indicates if results from specified modules are available,
- `of` - lists plain results (without metadata) from specified modules.

```python
type Identifier = Union[Module.Runtime, Module.Builder, Module.Interface]

ResultSet.get(identifier: Identifier): Module.Result
ResultSet.has(identifier: Identifier): bool
ResultSet.of(identifier: Identifier): List[Module.Result]
```

Results are accessible by **module class names or by interfaces**: 

```python
# TODO: Example
```


Moreover, it is also possible to access a result by the **tag given in the @expose decorator**:

```python
# TODO: Example
```


Although it is **not a recommended** solution, a module's result can be also accessed by **module registered name**. A module shouldn't rely on specific names of modules. It is highly recommended to use either interfaces, modules' type, or exposed names. However, in very rare cases this functionality might be useful:

```python
# TODO: Example
```
