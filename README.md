# Safebooru-Analysis

This is the code created as part of my bachelor thesis in which an application was created that gathers, processes, and analyses the metadata from a selected non-linear imageboard (safebooru). Here you can find the 4 main components and their code in the respective folders.

- Safebooru-Download-Component (SafebooruDownloader)
- Safebooru-Analysis-Component (SafebooruSpark)
- Safebooru-Akka-HTTP-Component (SafebooruAkka)
- Safebooru-React-Component (safebooru-app)

Detailed instructions regarding the installation and the execution of the different components can be found in the READMEs of the different components.

# Overview of the components

In this section a short overview over the different components will be given

## Safebooru-Download-Component

This component is responsible for downloading the image data from safebooru, processing it and saving it into the MongoDB.

## Safebooru-Analysis-Component

This component is responsible for extracting the information from the downloaded image data and calculating the desired informations. The processed information is then saved into the MongoDB.

## Safebooru-Akka-HTTP-Component

This component is responsible for serving the calculated information via Akka-HTTP.

## Safebooru-React-Component

This component is used to create the frontend as a react-application where the information is visualized.

# Prerequisites

The different components require a running MongoDB and the installation of Java, NodeJS, npm and pm2.

The different components also require some environment variables. Following is a list of environment variables that should be set. In each component you can also find a component specific list of environment variables in case you would like to only execute a single component.

|        Name         | Description                                                  | Example                                |
| :-----------------: | ------------------------------------------------------------ | -------------------------------------- |
|  DANBOORU_USERNAME  | username of the danbooru/safebooru account                   | user1                                  |
|  DANBOORU_API_KEY   | api-key of the danbooru/safebooru account                    | a1b2c3d4e5f6g7h8i9j1k1l2               |
| USER_DATABASE_NAME  | name of the database of MongoDB where the users are saved in | admin                                  |
| DATA_DATABASE_NAME  | name of the database of MongoDB where the data is saved in   | danbooruData                           |
|    DATABASE_USER    | name of the user of the MongoDB                              | databaseUser                           |
|  DATABASE_PASSWORD  | password of the given user of the MongoDB                    | securePassword                         |
| DATABASE_CONNECTION | server address of the MongoDB                                | example.com:27017                      |
|    SAFEANA_MONGO    | the standard connection string used to access the MongoDB    | mongodb://username:password@host:port/ |

In order to run all components and the MongoDB it is recommended to use around 16 GB of RAM and 30 GB of disk space. The RAM requirement can be lowered by reducing the amount of data that is cached in the Akka-HTTP-Component. Please not that due to the constantly growing amount of images the amoung of disk space may also need to be increased due to the constantly gowing amount of data. Currently there is a reserve between around 7 GB of disk space when assigning 30 GB of disk space. Therefore this problem should not occur for some time but it is something to be aware of.

# Example Configuration of Cron

Here you can find an example configuration from cron which is used on the VM where the components are deployed.

## Crontab of the LocalUser:

The crontab of the LocalUser is used to download the new image data, updating the calculated data, starting the akka-service and restarting the serve of the application via pm2.

- Downloading the newest images

5 23 \* \* \* /usr/bin/java -jar /home/local/download/UpdateDownload.jar

- Update the image data

15 23 \* \* \* /usr/bin/java -jar /home/local/spark/UpdateAnalysis.jar

- Stopping the current akka-service before the image data is updated in order to free the RAM needed for the updating.

0 23 \* \* \* kill $(ps aux | grep '[S]afebooruAkka-assembly-0.1.jar' | awk '{print $2}')

- Restarting the akka-service after the image data was updated

45 0 \* \* \* /usr/bin/java -Xms8g -jar /home/local/SafebooruAkka-assembly-0.1.jar

- Daily restart of pm2

0 23 \* \* \* pm2 serve /home/local/safebooru-app/build 3000 --spa

## Crontab of the SuperUser:

The crontab of the SuperUser is used to restart the MongoDB and reapply the firewall settings.

0 23 \* \* \* ./home/local/firewall.sh

1 23 \* \* \* chown -R mongodb:mongodb /var/lib/mongodb

2 23 \* \* \* chown mongodb:mongodb /tmp/mongodb-27017.sock

3 23 \* \* \* service mongod restart
