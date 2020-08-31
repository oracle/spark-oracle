# Spark_On_Oracle

- Currently, data lakes comprising Oracle Data Warehouse and Apache Spark have these characteristics:
  - They have **separate data catalogs,** even if they access the same data in an object store.
  - Applications built entirely on Spark have to **compensate for gaps in data management.**
  - Applications that federate across Spark and Oracle usually suffer from
    **inefficient data movement.**
  - Operating Spark clusters are expensive because they lack administration tooling
    and they have gaps in data management. **Therefore, price-performance advantages of Spark are overstated.**

![current deployments](https://github.com/oracle/spark-oracle/wiki/uploads/currentDeploymentDrawbacks.png)

This project fixes those issues:
- It provides a single catalog: Oracle Data Dictionary.
- Oracle is responsible for data management, including:
  - Consistency
  - Isolation
  - Security
  - Storage layout
  - Data lifecycle
  - Data in an object store managed by Oracle as external tables
- It provides support for a full Spark programming model.
- **Spark on Oracle** has these  characteristics:
  - Full pushdown on SQL workloads: Query, DML on all tables, DDL for external tables.
  - Push SQL operations of other workloads.
  - Surface Oracle capabilities like machine learning and streaming in the Spark programming model.
  - Co-processor on Oracle instances to run certain kinds of Scala code. Co-processors are isolated and limited and therefore are easy to manage.
- Enable simpler, smaller Spark clusters.

![spark on oracle](https://github.com/oracle/spark-oracle/wiki/uploads/spark-on-oracle.png)

**Feature summary:**
- Catalog integration. (See [this page](https://github.com/oracle/spark-oracle/wiki/Oracle-Catalog).)
- Significant support for SQL pushdown, to the extent that more than 95 (of 99) [TPCDS queries](https://github.com/oracle/spark-oracle/wiki/TPCDS-Queries)
  are completely pushed to Oracle instance. (See [Operator](https://github.com/oracle/spark-oracle/wiki/Operator-Translation) and [Expression](https://github.com/oracle/spark-oracle/wiki/Expression-Translation) translation pages.)
- Deployable as a Spark extension jar for Spark 3 environments.
- [Language integration beyond SQL](https://github.com/oracle/spark-oracle/wiki/Language-Integration)
  and [DML](https://github.com/oracle/spark-oracle/wiki/Write-Path-Flow) support.

See [Project Wiki](https://github.com/oracle/spark-oracle/wiki/home) for complete documentation.


## Installation

Spark on Oracle can be deployed on any Spark 3.1 or above environment.
See the [Quick Start Guide](https://github.com/oracle/spark-oracle/wiki/Quick-Start-Guide).

## Documentation

See the [wiki](https://github.com/oracle/spark-oracle/wiki/home).


## Examples

The [demo script](https://github.com/oracle/spark-oracle/wiki/Demo) walks you
through the features of the library.

## Help

Please file Github issues.

## Contributing

<!-- If your project has specific contribution requirements, update the
    CONTRIBUTING.md file to ensure those requirements are clearly explained. -->

This project welcomes contributions from the community. Before submitting a pull
request, please [review our contribution guide](./CONTRIBUTING.md).

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security
vulnerability disclosure process.

## License

<!-- The correct copyright notice format for both documentation and software
    is "Copyright (c) [year,] year Oracle and/or its affiliates."
    You must include the year the content was first released (on any platform) and
    the most recent year in which it was revised. -->

Copyright (c) 2022 Oracle and/or its affiliates.

<!-- Replace this statement if your project is not licensed under the UPL -->

Released under the Universal Permissive License v1.0 as shown at
<https://oss.oracle.com/licenses/upl/>.
