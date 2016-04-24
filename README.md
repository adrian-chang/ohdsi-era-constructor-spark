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
* More than 8GB of RAM (configurable, however very very recommended)

## Example

```
$ ./sbt/sbt -Dohdsi.vocab.location=/Users/achang/Downloads/vocab/ -Dohdsi.data.location=/Users/achang/Downloads/cms-synpuf-1000-cdmv5-version-1-0-1/ run
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=256M; support was removed in 8.0
[info] Loading project definition from /Users/achang/Development/ohdsi-project/project
[info] Set current project to ohdsi-project (in build file:/Users/achang/Development/ohdsi-project/)
[info] Compiling 1 Scala source to /Users/achang/Development/ohdsi-project/target/scala-2.11/classes...
[info] Running com.github.amchang.ohdsi.Main
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
16/04/22 00:19:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
16/04/22 00:19:13 INFO Remoting: Starting remoting
16/04/22 00:19:13 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkDriverActorSystem@192.168.31.214:54046]
conditionEra elapsed time: 2.508 s
doseEra elapsed time: 1.112 s
drugEraNonStockpile elapsed time: 1.06 s
drugEraStockpile elapsed time: 1.065 s
writing condition era at /tmp/condition_era_1461309564685
writing dose era at /tmp/dose_era_1461309576706
writing drug era non stockpile at /tmp/drug_era_non_stockpile_1461309576888
writing drug era stockpile at /tmp/drug_era_stockpile_1461309576994
[success] Total time: 31 s, completed Apr 22, 2016 12:19:37 AM

```

## Program Options

Here are a whole list of options which can be passed to the program. HDFS file paths are supported.

* `-Dohdsi.vocab.location`
    * The location of the [vocabulary files](http://www.ohdsi.org/web/athena/). Full V5 download is required to run this program. Option is required.
* `-Dohdsi.vocab.delimiter`
    * CSV delimiter of the vocabulary files. Default is `\t`.     
* `-Dohdsi.vocab.conceptAncestor`
    * CSV File containing concept ancestor data. Default is `CONCEPT_ANCESTOR.csv`.
* `-Dohdsi.vocab.concept`
    * CSV File containing concept data. Default is `CONCEPT.csv`.
* `-Dohdsi.vocab.conceptAncestor`
    * CSV File containing concept ancestor data. Default is `CONCEPT.csv`.
* `-Dohdsi.data.location`
    * The location of the OMOP CDM v5 data to process. Option is required.
* `-Dohdsi.data.conditionOccurrence`
    * CSV File containing condition iccurrence data. Default is `CDM_CONDITION_OCCURRENCE.csv`.
* `-Dohdsi.data.conditionOccurrence`
    * CSV File containing drug exposure data. Default is `CDM_DRUG_EXPOSURE.csv`.
* `-Dohdsi.cache.enabled`
    * Do we want to cache calculations for multiple runs? Default is `true`.
* `-Dohdsi.cache.location`
    * Where do we want to cache calculations. Default is `/tmp` (don't add trailing slash).
* `-Dohdsi.csv.enabled`
    * Do we want to save results? Default is `true`.
* `-Dohdsi.csv.location`
    * Where do we want to save results? Default is `/tmp/` (make sure to add trailing slash).
* `-Dohdsi.dateFormat`
    * The date format of the data. Default is `yyyyMMdd`.

## Data

There are two main options to get data to run the program (assuming you do not generate it yourself).

1. Small set of [hand made data](http://forums.ohdsi.org/t/1k-sample-of-simulated-cms-synpuf-data-in-cdmv5-format-available-for-download/728).
2. Use the [etl-cms](https://github.com/OHDSI/ETL-CMS/tree/unm-improvements/python_etl) project to generate a large set of data.

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