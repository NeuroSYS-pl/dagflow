---
title: Config File
sidebar_position: 1
---

import Example1Svg from '../assets/config-file/01.svg';
import Example2Svg from '../assets/config-file/02.svg';
import Example3Svg from '../assets/config-file/03.svg';
import Example4Svg from '../assets/config-file/04.svg';


# Configuration File

When the modules are implemented, a pipeline can be created in one of two ways: from code or using a configuration file (called *config*). This page documents pipeline creation using the second way.

## What it is
Config is a YAML file that enables the definition of a pipeline. It accepts some predefined properties: module dependencies and parameters but also pipeline shared parameters and group options.

## How to use
Having a configuration file in YAML, pipeline creation is as simple as:

```python
pipeline = ConfigReader.read(yaml_config: str, module_factory: Type[ModuleFactory])
```

The `read` method requires `ModuleFactory` in order to map previously registered module types to Python classes. It returns `BasePipeline.Runtime` instance (already built `BasePipeline`) ready to `run` or `process`.

### Config example
Below is a schema of a configuration file based on a simple example. It presents all possible options and parameters that can be configured within the file although most of them aren't required to create a pipeline. More concrete examples are described in [the last section](#examples-of-configuration-files) and all properties are listed in [the next section](#config-options).

```yaml title="config.yml"
modules:                   # list of modules
  - name: simple_mod       # module name
    type: simple-module    # registered Python type name
    group: group_1         # group name
    depends_on:            # list of module names
      - other_mod   
    parameters:            # map of module parameters
      simple: true

shared_parameters:         # map of shared parameters
  threshold: 0.75

groups:                    # additional groups options
  - name: group_1          # group name
    options:               # map of group options
      replicas: 3
```


## Config options
Every config has to have [`modules`](#modules) property. Other properties - [`shared_parameters`](#shared_parameters) and [`groups`](#groups) - are optional. 

### `modules`
This property is the only one that's mandatory. In general, it's used to determine dependencies between modules but also allows to set modules parameters and groups (in the case of a parallel pipeline).

Option | Required   | Note
------ | :--------: | ----
`name` | &#x2713; | It's the name of the module. There isn't any naming convention but the name specified in this property must be consistent with the module's name in a `depends_on` property of another module.
`type` | &#x2713; | It's the string under which the class has been registered in the `ModuleFactory`.
`group` | &#x2717; / &#x2713; | It's the name of a group, which the module belongs to. This property is **required only in a parallel pipeline**. If at least one module has a defined `group` property, the created pipeline is treated as a parallel pipeline.
`depends_on` | &#x2717; | It's a list of module names that the module depends on. Dependencies between all modules defined in a config determine their final order.
`parameters` | &#x2717; | It's a dictionary of parameter names and values that are given to the module as its arguments.


### `shared_parameters`
It's a dictionary of some additional parameters that are passed to each module. A similar role is played by a context however the main difference is that the shared parameters can store only simple types. Context usually stores objects or references. 
Shared parameters can be thought of as regular module parameters that can be accessed by and are the same for every module in a pipeline.

Shared parameters can be passed to a `ConfigReader` in one of two ways: as a property in a config YAML file or as an argument in the `read` method. Unlike context, which can be passed only as an argument (as it can store complex types).

```python
pipeline = ConfigReader.read(config_file_path, ModuleFactory,
                             context=my_context, shared_parameters=my_params)
```

It's worth noticing that shared parameters declared in a config YAML file always overwrite shared parameters passed as an argument to the `ConfigReader.read` method. That's why shared parameters should be provided in one of these ways, **but not both**.

The usage of the `shared_parameters` property in config is as simple as it can be - it stores key-value pairs of any simple type. The `shared_parameters` dictionary from a YAML file is read into Python as a `dict`.


### `groups`
The `groups` option isn't mandatory however if it is already in use then its properties are required. It sets additional options in the groups mentioned in the preceding `modules` property.

Option | Required   | Note
------ | :--------: | ----
`name` | &#x2713; | It's the name of a group. Must be consistent with module group names declared in the `modules` property.
`options` | &#x2713; | It's a dictionary of option names and values that are set in [ray](https://docs.ray.io/en/latest/advanced.html#dynamic-remote-parameters). The list of all valid options can be found in [ray documentation](https://docs.ray.io/en/latest/package-ref.html#ray-remote). <br/> One additional option is the `replicas` property - it determines the size of the actor pool (like in [the example below](#parallelpool-with-aggregation-module)).


## Examples of configuration files

### Sequential pipeline

<Example1Svg className="diagram" width="30%" />

```yaml title="sequential.config.yml"
modules:
  - name: mod_a
    type: dough-kneading-module
  - name: mod_b
    type: ingredients-preparing-module
    parameters:
      double_cheese: true
      salami_slices: 30
  - name: mod_c
    type: pizza-forming-module
    depends_on: 
      - mod_a
      - mod_b
  - name: mod_d
    type: pizza-baking-module
    depends_on:
      - mod_c
    parameters:
      temperature: 220

shared_parameters:
  recipe_difficulty_level: 5
```

Every module must consist of at least two parameters: `name` and `type`. In the example above, `dough-kneading-module` has only these two required properties. The second one contains some additional parameters: `double_cheese` and `salami_slices`. Both modules' outputs are inputs to the third module as the `pizza-forming-module` *depends on* `mod_a` and `mod_b`. Similarly, `mod_d` depends on `mod_c` and also has one additional parameter.  
There are also some `shared_parameters` added which are accessible from every module.


### Sequential pipeline with aggregation module

<Example2Svg className="diagram" width="30%" />

```yaml title="aggregated.config.yml"
modules:
  - name: mod_a
    type: tea-oxidation-module
  - name: mod_b
    type: tea-drying-module
    depends_on: 
      - mod_a
    parameters:
      days: 5
  - name: mod_c
    type: tea-firing-module
    depends_on:
      - mod_b
  - name: mod_d
    type: leaves-collecting-module
    depends_on: 
      - mod_a
  - name: mod_e
    type: leaves-analyzing-module
    depends_on: 
      - mod_d
```

In this example `mod_a` output is an input to the two consecutive modules. Modules `mod_a`, `mod_b` and `mod_c` (yellow ones) are marked as *regular* modules as they run during the `Pipeline.run` phase. In turn, `mod_d` and `mod_e` (purple modules) run during the `Pipeline.process` mode. The module `mod_d` is an aggregation module - it's stateful so multiple runs of the `Pipeline.run` only aggregate `mod_a` outputs. Only during `Pipeline.process` it runs and gives its output as an input to `mod_e`.

### Parallel pipeline with aggregation module

<Example3Svg className="diagram" width="60%" />

```yaml title="parallel.config.yml"
modules:
  - name: mod_a
    type: cocoa-harvesting-module
    group: group_1
  - name: mod_b
    type: cocoa-fermentation-module
    group: group_1
    depends_on: 
      - mod_a
  - name: mod_c
    type: cocoa-drying-module
    group: group_2
    depends_on: 
      - mod_b
    parameters:
      days: 7
  - name: mod_d
    type: cocoa-roasting-module
    group: group_2
    depends_on: 
    -  mod_c
  - name: mod_e
    type: chocolate-bagging-module
    group: group_1
    depends_on: 
      - mod_a
  - name: mod_f
    type: chocolate-transporting-module
    group: group_1
    depends_on: 
      - mod_e
  - name: mod_g
    type: chocolate-analyzing-module
    group: group_3
    depends_on: 
      - mod_f

groups:
  - name: group_2
    options:
      num_gpus: 2
```

The creation of a parallel pipeline requires an additional mandatory property: `group`. That's the main difference. Just like a sequential pipeline, a parallel one can have `shared_parameters`, module `parameters` property, and can consist of both regular and aggregation modules. 
But in a parallel pipeline, the `group` parameter is mandatory. For `group_2` there is an additional property defined:  `num_gpus`. However, it's not mandatory to define a group in the `group` section - `group_1` and `group_3` are initialized with default values.

### ParallelPool with aggregation module

<Example4Svg className="diagram" width="60%" />

```yaml title="parallel-actor-pool.config.yml"
modules:
  - name: mod_a
    type: coffee-drying-module
    group: group_1
  - name: mod_b
    type: coffee-milling-module
    group: group_1
    depends_on: 
      - mod_a
    parameters:
      grind_size: medium-coarse
  - name: mod_c
    type: coffee-roasting-module
    group: group_1
    depends_on: 
      - mod_b
  - name: mod_d
    type: coffee-grinding-module
    group: group_2
    depends_on: 
      - mod_c
  - name: mod_e
    type: coffee-brewing-module
    group: group_2
    depends_on: 
      - mod_d
  - name: mod_f
    type: coffee-storing-module
    group: group_1
    depends_on: 
      - mod_a
  - name: mod_g
    type: coffee-packing-module
    group: group_3
    depends_on: 
      - mod_f
    parameters:
      bag_size: medium
  - name: mod_h
    type: coffee-distributing-module
    group: group_3
    depends_on: 
      - mod_g

shared_parameters:
  coffee_type: liberica
  origin: Philippines

groups:
  - name: group_1
    options:
      max_calls: 2
      num_cpus: 1
  - name: group_3
    options:
      replicas: 3
```

This example uses all kinds of acceptable properties - `shared_parameters`, `groups`, and module `parameters`. However, the main difference is the `replicas` option in `groups`. `group_3` is replicated three times within the created parallel pool. `group_2` could be replicated as well but not `group_1` as it contains an aggregation module that cannot be duplicated.
