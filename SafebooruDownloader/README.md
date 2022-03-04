# Safebooru-Download-Component

This component is responsible for downloading the imagedata from safebooru. After the data is downloaded the data is
prepared and saved into the MongoDB. There are 3 main methods in this component:

### Initial-Download

This method is used for downloading the data of all the images on safebooru.

### Update-Download

This method is used for downloading the data of all images uploaded within the last 31 days on safebooru. This method
can be used for updating the imagedata once a day.

### Test-Download

This method can be used for testing one execution of the download loop. This method was used for fine-tuning the
parameters for the download loop.

## Build

To create an executable JAR bundled with all dependencies the plugin sbt-assembly is used. To execute the assembly
execute the "assembly" sbt task. In order to define from which main method the JAR will be built the corresponding lines
in the plugins.sbt file need to be uncommented/commented.

## Execution

The created JARs can be executed by executing the following command on the command line

For Executing the Initial-Download

``` 
 java -jar InitialDownload.jar
``` 

For Executing the Update-Download

``` 
 java -jar UpdateDownload.jar
``` 

Please note that Java 11 should be used for executing the created jars.

The execution of those JARs also causes a .txt file to be written which contains information regarding the current
download and can be used for checking if the download was successful.

## Environment-Variables

This component requires the following environment variables which should be set before executing:

| Name | Description | Example |
|:---:|:---:| :---:|
| USER_DATABASE_NAME | Name of the database of MongoDB where the users are saved in | admin |
| DATA_DATABASE_NAME | Name of the database of MongoDB where the data is saved in | danbooruData |
| DANBOORU_USERNAME| username of the danbooru/safebooru account | user1 |
| DANBOORU_API_KEY| api-key of the danbooru/safebooru account | a1b2c3d4e5f6g7h8i9j1k1l2 |
| DATABASE_USER| name of the user of the MongoDB | databaseUser |
| DATABASE_PASSWORD | password of the given user of the MongoDB | securePassword |
| DATABASE_CONNECTION | server address of the MongoDB | example.com:27017 |

## MongoDB