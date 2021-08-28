---
id: home
title: What is MAGDA?
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import SequentialStateless from './assets/home/sequential-stateless.svg';
import SequentialStateful from './assets/home/sequential-stateful.svg';
import ParallelStateless from './assets/home/parallel-stateless.svg';
import ParallelStateful from './assets/home/parallel-stateful.svg';
import ActorPoolStateless from './assets/home/actor-pool-stateless.svg';
import ActorPoolStateful from './assets/home/actor-pool-stateful.svg';


# What is MAGDA?

*MAGDA* is a Python library intended for assembling a stream-like architecture of an application following functional programming principles, by using predefined interfaces and classes. *MAGDA* stands for Modular and Asynchronous Graphs with Directed and Acyclic edges, and is the backbone of the library. The library works best when the code can be split into independent operations with clearly defined input and outputs (modules). The main idea is to use *MAGDA* to process code in a stream-like flow. Think of this as nodes with input/output as edges in directed graphs.

*MAGDA* supports following operations:
- building an application pipeline from a configuration file and from code,
- asynchronous processing,
- dividing modules into groups for parallel pipeline,
- aggregation of partial results.

*MAGDA* can be applied almost anywhere, but is especially well-suited for BigData parallel processing and ML pipelines intended for carrying out multiple, repeatable experiments.


## Exemplary use case

<!-- <img src="images/overview/usecase.png" alt="sequential_agg_pipeline" /> -->
| TODO: Diagram |
| :---: |

The diagram above presents a simple application based on data flow, where business logic can be divided into modules fired in a cascade manner. The purpose of this exemplary application is to collect data portions from multiple sources, merge them somehow, prepare a request to multiple databases, aggregate results and count statistics based on data and on the result of the request.

The pipeline is intended to be launched multiple times, with each run devoted to a separate data portion. In order to accelerate performance, some logical parts can be processed in parallel. Moreover, statistics are supposed to be updated for each data portion and finally retrieved, at the end of processing. Modules can also take parameters specific only to them. There is also a pool of parameters shared between multiple logical parts.
 
All those functionalities can be simply achieved by enclosing the application logic in interfaces provided by *MAGDA* and setting up an appropriate configuration file.


## Directed Acyclic Graph 

A *DAG*, Directed Acyclic Graph, is a directed graph with no cycles, meaning that each edge is directed from one vertex to another and there is no such a sequence of edges starting and ending in the same vertex. The *MAGDA* library  uses topological ordering of DAG to process modules in an optimal sequence either in purely sequential, as well as parallel use cases. All that has to be done is to define the ancestors of all modules in the configuration file.

**It is also worth mentioning, that modules are obliged to create one, single graph. No disconnected subparts are allowed.**

## General architecture

### Module

`Module` is an abstract class that defines how the logic of an application component works. Business logic is defined by creating an object upon the basic `Module` class. Conceptually, with some exceptions, the module encapsulates the logic in `run` method, that takes the output from the module's ancestors and can make use of the *Context* (i.e. database connectors,  DAO objects, reusable components), module related *Parameters* or so called *Shared Parameters*. All those functionalities will be described in detail further in the documentation. 

Each module defines its ancestors creating a dependency relationship. A module may have a single ancestor (one-to-one relationship), multiple ancestors (many-to-one) and can be an ancestor for multiple modules (one-to-many) but only under one condition. As dependencies can be represented only as the aforementioned *DAG*, modules cannot be connected circularly.

### Pipeline

*Pipeline*, which consists of a set of modules, is a manager for creating and controlling the flow of the application. Underneath, it consists of a *builder* and *runtime* classes, described in detail on the page about sequential pipelines. A pipeline is made up of connected modules, each representing a separate part of systems logic. A pipeline can be either implemented by attaching modules programmatically or be built from the configuration file.

### Config

*Config* (or a configuration file) is a YAML file, being a draft of each pipeline. It describes how *Modules* are linked together, what parameters they take, assigns their names and interfaces, and defines if they should be processed together in a group in case of a parallel pipeline. In a config file, it is also possible to define shared parameters and group parameters for parallel use cases.


## Pipeline scenarios  

Each of the pipeline scenarios enclosed below has a separate section in this documentation, devoted to describing it in detail.

<Tabs
  defaultValue="stateless-sequential"
  values={[
    {label: "Stateless Sequential", value: "stateless-sequential"},
    {label: "Stateless Parallel", value: "stateless-parallel"},
    {label: "Stateless Parallel Actor Pool", value: "stateless-actor-pool"},
    {label: "Stateful Sequential", value: "stateful-sequential"},
    {label: "Stateful Parallel", value: "stateful-parallel"},
    {label: "Stateful Parallel Actor Pool", value: "stateful-actor-pool"},
  ]}
>
  <TabItem value="stateless-sequential">
    <SequentialStateless className="diagram" width="100%" />
  </TabItem>
  <TabItem value="stateless-parallel">
    <ParallelStateless className="diagram" width="100%" />
  </TabItem>
  <TabItem value="stateless-actor-pool">
    <ActorPoolStateless className="diagram" width="100%" />
  </TabItem>
  <TabItem value="stateful-sequential">
    <SequentialStateful className="diagram" width="100%" />
  </TabItem>
  <TabItem value="stateful-parallel">
    <ParallelStateful className="diagram" width="100%" />
  </TabItem>
  <TabItem value="stateful-actor-pool">
    <ActorPoolStateful className="diagram" width="100%" />
  </TabItem>
</Tabs>

### Sequential, Parallel, and ParallelPool pipelines
 
Pipelines can be run in multiple manners. They can be purely sequential with no parallelization, meaning that only one module is processed at the same time. Modules can be also combined into groups that are processed simultaneously. In such a case, modules within the group are processed sequentially. Moreover, in order to speed up the performance, some groups can be multiplied and used as parallel pool, so as to process many requests by the same logical group at once.

### Stateless and stateful modules
 
Modules can be either stateless or stateful. Stateless modules are runtime modules, invoked with every request. Stateful modules are aggregators (i.e. data statistics counters from the example above) tasked with storing results between requests. **An Aggregator cannot be multiplied in a parallel pool.**
