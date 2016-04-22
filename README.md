# ohdsi-era-constructor-spark

This project is a reimagination of the [ohdsi-era-constructor](https://github.com/OHDSI/Era-Constructor) tool. Instead of doing all of the data processing, retrieval, and storage inside a single Postgres database, this project performs the same operations using Apache Spark and CSV files. 

For smaller datasets (50-100k records processed), this project preforms about 10-20% worse than the original implementation however for datasets in the millions, it can perform at minimum 10-20% than the original implementation if not up to 1000% percent.

## Installation

1. `git clone https://github.com/amchang/ohdsi-era-constructor-spark.git`
2. `./sbt/sbt run`
    * This step will take around 10-15 minutes based on your internet connection and system
    
### Requirements

* JDK > 8
* Persistent internet connection

## Example


// setup postgres

// setup the data model
git@github.com:OHDSI/CommonDataModel.git
git@github.com:OHDSI/ETL-CMS.git

// remember the config
//sql

## Program Options

Here are a whole list of options which can be passed to the program.

* `-Dohdsi.vocab.location`
    * The location of 
* `-Dohdsi.vocab.delimiter`

## Data

There are two main options to get data to run the program (assuming you do not generate it yourself).

1.
2.

## Test

This project currently is 100% (outside of the main driver application) unit tested.

To run unit tests, use the following command.

`./sbt/sbt test`

## Notes

There are several deviations or variances from the [original](https://github.com/OHDSI/Era-Constructor) implementation of this project makes that may be worthy to take note of.

1. Bug fixes
    * The results created by the [original](https://github.com/OHDSI/Era-Constructor) era-constructor project had several obvious errors that were so patched in this project.
2. Spec Reinterpretation
    * The era's created by [era-constructor](https://github.com/OHDSI/Era-Constructor) project are an interpretation of the DRUG_ERA, DOSE_ERA, and CONDITION_ERA of the [CDM v5 Data Model](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:standardized_derived_elements). This project also inteprets those same specifications however differs in some minor ways leading to differences in results between comparing the original project to this project.

## Future Work

* Write integration tests with real data
* Publish library code as a JAR to Maven for library only reuse
* Consider using a dockerized environment for development
* Use an external Spark server for data processing

## Reference Paper and Video

If interested, please email Adrian Chang <adrian.chang@yahoo.com> for links for a corresponding video presentation and written paper explaining the entire project.