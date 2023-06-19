# LERN data-products

Data products is a collection of scala scripts which are used to generate reports, updating data in the redis and migration of data.

The code in this repository is licensed under MIT License unless otherwise noted. Please see the [LICENSE](https://github.com/project-sunbird/sunbird-lms-service/blob/master/LICENSE) file for details.

## System Requirements

### Prerequisites

- Java 11
- Scala 2.12
- Spark 3.1.3 
- Latest Maven

### Data provider dependencies
Following data providers will be required for running the job with spark-submit mode.
- Cassandra
- Postgres
- Druid
- Redis
- Elasticsearch
- Content search API
- Org search API

### Setup of dependency libraries for data-products
Build the dependency libraries in local machine
#### <u>sunbird-analytics-core</u>
Analytics job driver and analytics framework is used to trigger the job in job manager

```
### Steps to build ###

# Clone the repo
git clone git@github.com:Sunbird-Obsrv/sunbird-analytics-core.git

# checkout to the respective release branch
git checkout release-5.1.1

# build the project
mvn clean install -DskipTests
```

#### <u>sunbird-core-dataproducts</u>

Batch-models module is used from this library handling the execution of job

```
### Steps to build ###

# Clone the repo
git clone git@github.com:Sunbird-Obsrv/sunbird-core-dataproducts.git

# checkout to the respective release branch
git checkout release-5.1.1

# build the project
mvn clean install -DskipTests
```

***Note***: The above dependency libraries has to be built from the respective release branch for the data-products.

## Setup of data-products in local

Each data-product is an independent spark job which used for generating reports and data migrations. So each data-product having different sets of data provider dependencies. Data-provider for each job is listed in the below reference link.

[Reference Link](https://project-sunbird.atlassian.net/wiki/spaces/UM/pages/3135471624/Migration+of+Data+Products+in+Sunbird-LERN#%F0%9F%A7%AE-Data-product-list)

The data-products can be tested locally with the testcases.

**Steps to build the project**

```
# Clone the repo
git clone git@github.com:Sunbird-Lern/data-products.git

# checkout to the respective release branch
git checkout release-5.3.0

# change the directory to project directory
cd lern-data-products 

# build the project
mvn clean install -DskipTests
```

**Steps to run the testcase**
```
mvn -Dsuites={{classname with package path}} test

# Example:
# mvn -Dsuites=org.sunbird.lms.exhaust.TestProgressExhaustJob test
```

**Note**: While testcase execution, report files will be generated and verified and deleted immediately after the testcase is completed. Check for the file path from the  testcase for manual verification.
<br>
We suggest running the testcases in debug mode using IDE for debugging.

For running the data-products testcase, we are using following data sources in embedded mode
- cassandra
- postgres
- redis

Data sources shema used in testcases are below
<br>
https://github.com/Sunbird-Lern/data-products/blob/release-5.3.0/lern-data-products/src/main/resources/data.cql
<br>
https://github.com/Sunbird-Lern/data-products/blob/release-5.3.0/lern-data-products/src/test/scala/org/sunbird/core/util/EmbeddedPostgres.scala

And the API requests are mocked inside the testcase with mockwebserver library.

## Run Data-products in server

Data-products in server runs in spark-submit mode. Installation and execution guide can be found from the below link

https://lern.sunbird.org/use/developer-installation/data-products