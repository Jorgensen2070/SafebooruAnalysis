# Safebooru-Akka-HTTP-Component

This component is responsible for serving the calculated data from the MongoDB to the React-Component. This component
consists of one main method which initialized the Akka-Service and enables the access to the data via the given URLs.

## Build

To create an executable JAR bundled with all dependencies the plugin sbt-assembly is used. To execute the assembly execute the "assembly" sbt task.

## Execution

The created JARs can be executed by executing the following command on the command line

``` 
 java -jar SafebooruAkka-assembly-0.1.jar
```

Please note that it is not recommended to execute the command as shown above. Due to the fact that some data is cached
it is recommended to assign more RAM to the process. This can be done with the following command where 8 GB of RAM are
assigned.

``` 
 java -Xms8g -jar UpdateDownload.jar
``` 

With 8 GB of RAM there is enough space to currently fit all the calculated data. The RAM requirement can be lowered by
manually adjusting the data that is cached throughout the code.

The execution of this jar should be done with Java 11.

At the beginning of the execution of the JAR first the data that will be cached is loaded by the system. Due to this the
entire startup phase may take some minutes until the service becomes fully operational.

Once the service is operational the routes that serve the data can be called.

## Environment-Variables

This component requires the following environment variable which should be set before executing:

| Name | Description | Example |
|:---:|:---:| :---:|
| USER_DATABASE_NAME  | Name of the database of MongoDB where the users are saved in | admin |
| DATA_DATABASE_NAME  | Name of the database of MongoDB where the data is saved in   | danbooruData|
| SAFEANA_MONGO | The standard connection string used to access the MongoDB | mongodb://username:password@host:port/ |

## Routes

The following sections gives an overview of all the routes that this component provides. Only a short description is
given here. To find more detailed information regarding the data please refer to the comments in the code regarding the
specific routes.

### Character-Routes

General Character Routes:

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/character/list`| `None` | List of all chartacters sorted by the size in descending order|
|`/character/total`| `None` | Data for a piechart regarding image counts for the characters |
|`/character/year`| `None` | Image count per year for the top 9 characters |
|`/character/month`| `None` |  Image count per month for the top 9 characters |
|`/character/day`| `None` | Image count per day for the top 9 characters within the last 31 days |
|`/character/characterPairing`| `None` | Data for a force-directed-graph from the top 100 characters and their connections |
|`/character/totalBoxplot`| `None` | 5 values for a boxplot regarding the image counts per character |

Individual Character Routes:

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/character/total/{character}`| `character`: `String`| Count of occurrences of the given character|
|`/character/year/{character}`|  `character`: `String` |  Image count per year for the given character  |
|`/character/month/{character}`|  `character`: `String` |  Image count per month for the  given character |
|`/character/day/{character}`|  `character`: `String` | Image count per day for the  given character within the last 31 days |
|`/character/mainCopyright/{character}`| `character`: `String` | Main copyright of the given character |
|`/character/copyright/{character}`|  `character`: `String`| List of copyrights that the given character was tagged with together with a count of occurrences |
|`/character/copyrightPie/{character}`|  `character`: `String`|  Data for a piechart regarding copyrights that the given character was tagged with |
|`/character/characterPairing/{character}`|  `character`: `String`| List of character that the given character was tagged with together with a count of occurrences |
|`/character/characterPairingPie/{character}`|  `character`: `String`|  Data for a piechart regarding character that the given character was tagged with  |
|`/character/tags/{character}`|  `character`: `String`|  List of tags that the given character was tagged with together with a count of occurrences |
|`/character/tagsRadar/{character}`|  `character`: `String`|  Data for a radar-chart regarding the top 10 tags that were given to the given character |

### Copyright-Routes

General Copyright Routes:

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/copyright/list`| `None` | List of all copyright sorted by the size in descending order|
|`/copyright/total`| `None` | Data for a piechart regarding image counts for the copyrights |
|`/copyright/year`| `None` | Image count per year for the top 9 copyrights |
|`/copyright/month`| `None` |  Image count per month for the top 9 copyrights |
|`/copyright/day`| `None` | Image count per day for the top 9 copyrights within the last 31 days |
|`/copyright/copyrightPairing`| `None` | Data for a force-directed-graph from the top 100 copyrights and their connections |
|`/copyright/totalBoxplot`| `None` | 5 values for a boxplot regarding the image counts per copyright |
|`/copyright/characterPerCopyright`| `None` | Data for a piechart regarding characters per maincopyright |
|`/copyright/characterPerCopyrightBoxplot`| `None` | 5 values for a boxplot regarding the characters per maincopyright  |

The last 2 routes are not used by the React-component but were left in.

Individual Copyright Routes:

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/copyright/total/{copyright}`| `copyright`: `String`| Count of occurrences of the given copyright|
|`/copyright/year/{copyright}`|  `copyright`: `String` |  Image count per year for the given copyright  |
|`/copyright/month/{copyright}`|  `copyright`: `String` |  Image count per month for the  given copyright |
|`/copyright/day/{copyright}`|  `copyright`: `String` | Image count per day for the  given copyright within the last 31 days |
|`/copyright/implications/{copyright}`|  `copyright`: `String` | Information regarding implications either from this copyright to another copyright or from another copyright to this copyright |
|`/copyright/copyrightPairing/{copyright}`|  `copyright`: `String` | List of copyrights that the given copyright was tagged with together with a count of occurrences |
|`/copyright/copyrightPairingPie/{copyright}`|  `copyright`: `String`|  Data for a piechart regarding copyrights that the given copyrights was tagged with for the copyrights  |
|`/copyright/mainCharacters/{copyright}`|  `copyright`: `String`| List of all characters that have the given copyright or implicated copyright as their maincopyright together with a count of occurrences |
|`/copyright/mainCharactersPie/{copyright}`|  `copyright`: `String`| Data for a piechart regarding characters that have the given copyright or the implicated copyright as their maincopyright |
|`/copyright/allCharacters/{copyright}`|  `copyright`: `String`| List of all characters that were tagged together with the given copyright together with a count of occurrences |
|`/copyright/allCharactersPie/{copyright}`|  `copyright`: `String`| Data for a piechart regarding characters that have the given copyright was tagged together with |
|`/copyright/topCharactersYear/{copyright}`|  `copyright`: `String`|  Image count per year for the top 8 characters that were tagged with together with the copyright |
|`/copyright/topCharactersMonth/{copyright}`|  `copyright`: `String`|  Image count per month for the top 8 characters that were tagged with together with the copyright |
|`/copyright/topCharactersDay/{copyright}`|  `copyright`: `String`|  Image count per day for the top 8 characters that were tagged with together with the copyright for the last 31 days |
|`/copyright/characterCombinations/{copyright}`|  `copyright`: `String`| Data for a force-directed-graph from the top 100 characters and their connections that have the given copyright as their maincopyright|

### General-Tag-Routes

General Tag Routes:

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/generalTag/list`| `None` | List of all general tags sorted by the size in descending order|
|`/generalTag/total`| `None` | Data for a piechart regarding image counts for the general tags |
|`/generalTag/year`| `None` | Image count per year for the top 9 general tags |
|`/generalTag/month`| `None` |  Image count per month for the top 9 general tags  |
|`/generalTag/day`| `None` | Image count per day for the top 9 general tags  within the last 31 days |
|`/generalTag/totalBoxplot`| `None` | 5 values for a boxplot regarding the image counts per general tag |

Individual General Tag Routes:

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/generalTag/total/{generalTag}`| `{generalTag}`: `String` |Count of occurrences of the given tag |
|`/generalTag/year/{generalTag}`| `{generalTag}` : `String`| Image count per year for the given tag  |
|`/generalTag/month/{generalTag}`| `{generalTag}` : `String`|  Image count per month for the for the given tag  |
|`/generalTag/day/{generalTag}`| `{generalTag}` : `String`| Image count per day for the given tag for the last 31 days |

### Tag-Group-Routes

Available tag-group-names:

+ headwearAndHeadgear
+ shirtsAndTopwear
+ pantsAndBottomwear
+ legsAndFootwear
+ shoesAndFootwear
+ uniformsAndCostumes
+ swimsuitsAndBodysuits
+ traditionalClothing

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/tagGroups/{tagGroupName}/total`| `{tagGroupName}`: `String` | Data for a piechart regarding the constellation of the given tag group |
|`/tagGroups/{tagGroupName}/year`| `{tagGroupName}` : `String`| Image count per year for the top 8 entries of the given tag group  |
|`/tagGroups/{tagGroupName}/month`| `{tagGroupName}` : `String`|  Image count per month for the top 8 entries of the given tag group |

### General-Information-Routes

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/generalInformation/avgUpScore/total/{tag}`| `{tag}`: `String` | Overall average score given to the given tag |
|`/generalInformation/avgUpScore/year/{tag}`| `{tag}` : `String`| Average score per year given to the given tag |
|`/generalInformation/avgUpScore/month/{tag}`| `{tag}` : `String`|  Average score per month given to the given tag |

### Latest-Image-Routes

| Route                               |Parameter | Returned Information            |
| :------------------------------------|:----------------- |:--------------------- |
|`/latestImages/{tag}`| `{tag}`: `String` | URLs to the 5 latest images uploaded for the given tag|

