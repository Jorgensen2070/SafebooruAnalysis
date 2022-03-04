# Safebooru-Analysis-Component

This component is responsible for analyzing the downloaded imagedata from safebooru. There are 2 main methods in this
component:

### Initial-Analysis

This method is used for doing the initial calculations of all the desired information. Please note that this method
takes quite some time to execute.

### Update-Analysis

This method is used for updating the desired information. Please note that this method should only be executed after the
initial analysis was executed once because it uses some data already calculated by the initial analysis to reduce the
time. This method can be used for updating the information once a day after the newest images were downloaded.

## Build

To create an executable JAR bundled with all dependencies the plugin sbt-assembly is used. To execute the assembly
execute the "assembly" sbt task. In order to define from which main method the JAR will be built the corresponding lines
in the plugins.sbt file need to be uncommented/commented.

## Execution

The created JARs can be executed by executing the following command on the command line

For Executing the Initial-Anylsis

``` 
 java -jar InitialAnalysis.jar
``` 

For Executing the Update-Download

``` 
 java -jar UpdateAnalysis.jar
``` 

Please note that Java 11 should be used for executing the created jars.

The execution of those JARs also causes a .txt file to be written which contains information regarding the current
download and can be used for checking if the download was successful.

## Environment-Variables

This component requires the following environment variable which should be set before executing:

| Name | Description | Example |
|:---:|:---:| :---:|
| USER_DATABASE_NAME  | Name of the database of MongoDB where the users are saved in | admin                                  |
| DATA_DATABASE_NAME  | Name of the database of MongoDB where the data is saved in   | danbooruData                           |
| SAFEANA_MONGO | The standard connection string used to access the MongoDB | mongodb://username:password@host:port/ |
