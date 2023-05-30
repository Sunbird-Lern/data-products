# LERN data-products

Data products is a collection of scala scripts which are used to generate reports, updating data in the redis and migration of data.

The code in this repository is licensed under MIT License unless otherwise noted. Please see the [LICENSE](https://github.com/project-sunbird/sunbird-lms-service/blob/master/LICENSE) file for details.

## System Requirements

### Prerequisites

- Java 11
- Scala 2.12
- Spark 3.1.3 
- Latest Maven

### Dependency libraries for data-products
- `sunbird-analytics-core`

  https://github.com/project-sunbird/sunbird-analytics-core

  Analytics job driver and analytics framework is used to trigger the job in job manager

- `sunbird-core-dataproducts`

  https://github.com/project-sunbird/sunbird-core-dataproducts

  Batch-models module is used from this

***Note***: The above dependency libraries has to be built from the respective release branch for the data-products. Use the
below command for building the dependencies.
```
mvn clean install -DskipTests
```

## Setup of data-products

Each data-product is an independent job which used for generating reports and data migrations. Since that each data-product having different sets of data provider dependencies. Data-provider for each job is listed in the below reference link.

[Reference Link](https://project-sunbird.atlassian.net/wiki/spaces/UM/pages/3135471624/Migration+of+Data+Products+in+Sunbird-LERN#%F0%9F%A7%AE-Data-product-list)

The data-products can be tested locally with the testcases.

***Note***: Use below command for running specific testcases from command line shell.

```
mvn -Dsuites={{classname with package path}} test

# Example:
# mvn -Dsuites=org.sunbird.lms.exhaust.TestProgressExhaustJob test
```
