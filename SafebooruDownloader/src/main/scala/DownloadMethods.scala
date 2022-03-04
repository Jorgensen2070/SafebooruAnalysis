import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.mongodb.scala.{MongoClient, MongoClientSettings, MongoCollection, MongoCredential, MongoDatabase, ServerAddress}
import org.mongodb.scala.connection.ClusterSettings

import java.net.{HttpURLConnection, URL}
import java.io.FileWriter
import java.time.LocalDateTime
import java.util.regex.Pattern


object DownloadMethods {
  //Chars that will be removed from the response
  val remove: Set[Char] = "{[]}".toSet

  val maxPage = 500
  val batchSize = 50
  val tableName = "allImageData"
  val implicationTableName = "implications"
  val userDatabaseName:String =  sys.env("USER_DATABASE_NAME")
  val dataDatabaseName:String =  sys.env("DATA_DATABASE_NAME")

  val danbooruUsername: String = sys.env("DANBOORU_USERNAME")
  val danbooruApi_key: String = sys.env("DANBOORU_API_KEY")

  val databseUser: String = sys.env("DATABASE_USER")
  val databasePassword: String = sys.env("DATABASE_PASSWORD")
  val databaseConnection: String = sys.env("DATABASE_CONNECTION")

  val databaseCredential: MongoCredential = MongoCredential.createCredential(databseUser, userDatabaseName, databasePassword.toCharArray)
  val clusterSettings: ClusterSettings = ClusterSettings.builder().hosts(java.util.Arrays.asList(new ServerAddress(databaseConnection))).build()
  val settings: MongoClientSettings = MongoClientSettings.builder()
    .applyToClusterSettings((b: ClusterSettings.Builder) => b.applySettings(clusterSettings)).credential(databaseCredential).codecRegistry(MongoClient.DEFAULT_CODEC_REGISTRY).build()

  val mongoClient: MongoClient = MongoClient(settings)
  val database: MongoDatabase = mongoClient.getDatabase(dataDatabaseName)


  case class AllDoneDownloading() extends Exception {}

  case class RetryCountExceeded() extends Exception {}

  case class MaxPageReachedDownloadingImages(tag: String, date: String) extends Exception {}

  case class MaxPageReachedDownloadingCopyright() extends Exception {}


  /**
   * This method starts a loop for downloading the impplications between the copyright tags and calls a method to save them into the database
   *
   * @param fw   - the filewriter that is used to write to the log file
   */
  def generateImplicationsList(fw: FileWriter): Unit = {
    fw.append("\n Started creating the Implications List")
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/" + "DanbooruTestData.implications")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    val sc = sparkSession.sparkContext
    var finData = sc.emptyRDD[Document]

    try {
      for (page <- 1 to maxPage) yield {
        var reqDone = false
        var resp = ""
        var retryCount = 0
        while (!reqDone) {
          try {
            resp = getRequestImplicationCopyrightList(page, danbooruUsername, danbooruApi_key)

            //The response [] means no more data for the specified tag was found.
            if (resp.equals("[]")) {
              fw.append("\n Ended downloading all Implications at page " + page + " with " + finData.count() + " entries")
              throw AllDoneDownloading()
            }
            //The response regarding the database timing out can occur when new data is fetched on the server side.
            //The default retry count is set to 10 (usually an answer comes earlier but it is set so high to improve stability)
            else if (resp.contains("The database timed out running your query")) {
              if (retryCount > 10) {
                fw.append("\n " + resp)
                fw.append("\n RetryCount exceeded for obtaining the Implications on page " + page)
                throw RetryCountExceeded()
              }
              retryCount += 1
            } else {
              reqDone = true
            }
          } catch {
            case _: java.io.IOException =>
              retryCount += 1
            case x: Any => throw x
          }
        }


        val prepared = resp.split("\\},\\{")
          .map(elem => ("{" + (elem.filterNot(remove)) + "}")
            .replace("\"id\"", "\"_id\""))
        val rdd = sc.parallelize(prepared)
        val parsed = rdd.map(elem => Document.parse(elem))

        // the now created documents are put together via union into a bigger batch before being written into the db
        finData = finData.union(parsed)

        if (page == maxPage) {
          fw.append("\n Reached MaxPage while creating the impplications List. ")
          fw.append("\n This error should normally not occur please try to set the maxPage higher - eg. 500")
        }
      }
    }

    catch {
      case _: AllDoneDownloading => //Expected Outcome
        saveImplicationsListToDatabaseAndExtractFilterList(finData)

      case _: RetryCountExceeded => //Outcome which occurs when the
        saveImplicationsListToDatabaseAndExtractFilterList(finData)

      //Outcome which can occur when more images are available but the max page for the current account level is reached
      //The oldest date for an image is returned so that a new search from the given date can be started
      case _: MaxPageReachedDownloadingCopyright =>
       saveImplicationsListToDatabaseAndExtractFilterList(finData)

      case x: Throwable =>
        fw.write("\n An exception occured while downloading the Implications !")
        fw.write("\n" + x)
        fw.write("\n Leaving Method")
    }
  }

  /**
   * This method takes the rdd containing all implications from the request in document form
   * Then it transforms them into a map of the form (main_series(_id):String , implications List[String])
   * After that this map is written into the implicationTable in the database
   *
   * @param prepared RDD[Document] rdd containing the data regarding the implications as Documents
   */
  def saveImplicationsListToDatabaseAndExtractFilterList(prepared: RDD[Document]): Unit = {
    val extracted = prepared
      .map(x => (x.get("consequent_name").asInstanceOf[String], x.get("antecedent_name").asInstanceOf[String]))
      .groupBy(_._1.replaceAll("\"", "\'")).mapValues(x => x.map(_._2.replaceAll("\"", "\'")))
    // ^ Thank you Touhou for gracing us sth with the name 'dateless_bar_"old_adam"' (reason for the replaceAll)


    // Here double implications are removed which can occur in the list
    // (a, List(b,c)) (b,List(d,e)) => (a,List(b,c,d,e)
    val lookUp = extracted.mapValues(_.toList).collectAsMap() //This map need to be collected in order to perform the lookUp operations
    val cleaned = extracted.map(orig => (orig._1, orig._2 ++ orig._2.flatMap(impli =>
      lookUp.getOrElse(impli, List[String]())))) //The connected Elements or an empty List is returned
      .filter(fil => !lookUp.map(_._2.contains(fil._1)).toList.contains(true)) //Here the original entries that were merged into the others are removed

    val docs = cleaned.map(elem => Document.parse("{\"_id\": \"" + elem._1 + "\" ,\"implications\": " + elem._2.toArray.mkString("[\"", "\", \"", "\"]") + "}"))

    val collection: MongoCollection[Document] = database.getCollection(implicationTableName)
    val fin = docs.collect()
    collection.insertMany(fin).head()

  }

  /**
   * This method starts a loop for downloading the metadata for a given tag
   * In most cases the loop is directly exited but it can repeat if
   * more images are available than the user can access with his current level
   *
   * @param searchTag    - the tag from which the metadata should be obtained
   * @param downloadType - a string that determines what type of download it is
   *                     -> update -> This means that the data is being updated (requires a given date)
   *                     -> downloadAll -> This means that all data is being fetched
   *                     -> NoValue -> This means that only data from a specific tag is fetched
   * @param fileWriter   - the filewriter that is used to write to the log file
   */
  def downloadLoopSingleTag(searchTag: String, downloadType: String = null, fileWriter: FileWriter): Unit = {
    var startdate: Option[String] = None

    if (downloadType.equals("update")) {
      val timeStamp = LocalDateTime.now().minusDays(31)
      startdate = Option(timeStamp.getYear + "-" + timeStamp.getMonthValue + "-" + timeStamp.getDayOfMonth)
    }

    do {
      if (startdate.isDefined) {
        startdate = downloadImagesToTag(searchTag, fileWriter, startdate.get, Option(downloadType))
      } else {
        startdate = downloadImagesToTag(searchTag, fileWriter, null, Option(downloadType))
      }
    }
    while (startdate.isDefined)
  }

  /**
   * This method exits to call the downloadImagesToTag method once
   * This method was used to test the processing speed with different batch sizes
   *
   * @param fileWriter  the filewriter that is used to write to the log file
   */
  def testMethodForDownload(fileWriter: FileWriter):Unit = {
    downloadImagesToTag("#", fileWriter, null, Option("downloadAll"))
  }

  /** This Method
   *
   * @param searchTag    - the tag from which the metadata should be obtained
   * @param fw           - the filewriter that is used to write to the log file
   * @param date         - optional dateString if the download should be started from a given date
   * @param downloadType - Option[String] This string can be set to specify the download type:
   *                     -> update -> This means that the data is being updated (requires a given date)
   *                     -> downloadAll -> This means that all data is being fetched
   *                     -> NoValue -> This means that only data from a specific tag is fetched
   * @return Option[String] with a dateString if there are more images are available or no more images are available
   */
  def downloadImagesToTag(searchTag: String, fw: FileWriter, date: String = null, downloadType: Option[String]): Option[String] = {
    fw.append("\n Started downloading " + searchTag + " with downloadType " + downloadType.get)
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/" + "DanbooruTestData")
      .config("spark.testing.memory", 2147480000)
      .getOrCreate()

    val sc = sparkSession.sparkContext
    var finData = sc.emptyRDD[Document]
    var dataCount: Long = 0
    var lastDate: String = null

    try {
      var startIntervalTime = System.currentTimeMillis()

      for (page <- 1 to maxPage) yield {
        var reqDone = false
        var resp = ""
        var retryCount = 0

        while (!reqDone) {
          try {
            resp = customTagGetRequest(searchTag, page, Option(date), downloadType, danbooruUsername, danbooruApi_key)

            //The response [] means no more data for the specified tag was found.
            if (resp.equals("[]")) {
              fw.append("\n Ended downloading " + searchTag + " at page " + page + " with " + (dataCount + finData.count()) + " entries")
              throw AllDoneDownloading()
            }
            //The response regarding the database timing out can occur when new data is fetched on the server side.
            //The default retry count is set to 10 (usually an answer comes earlier but it is set so high to improve stability)
            else if (resp.contains("The database timed out running your query")) {
              if (retryCount > 10) {
                fw.append("\n Response: " + resp)
                fw.append("\n RetryCount exceeded with " + searchTag + " at page " + page)
                throw RetryCountExceeded()
              }
              retryCount += 1
            } else {
              reqDone = true
            }
          } catch {
            case x: java.io.IOException =>
              x.printStackTrace()
              fw.append("\n Response: " + resp)
              retryCount += 1
              if (retryCount > 10) {
                fw.append("\n RetryCount exceeded with " + searchTag + " at page " + page)
                throw RetryCountExceeded()
              }
            case x: Any => throw x
          }
        }

        // The raw String in the body isnt correctly formatted so that when transformed into an rdd the Lists are incorrect
        // Therefore the string needs to be adjusted
        // To reduce the impact of this operation this step is parallel via '.par'
        val prepared = resp.split("\\},\\{").par
          .map(elem => ("{" + (elem.filterNot(remove)) + "}")
            .replace("\"id\"", "\"_id\"")
          ).filter(_.contains("\"_id\""))

        val rdd = sc.parallelize(prepared.seq)


        val parsed = rdd.map(elem => Document.parse(elem)).map(
          elem => {
            elem.replace("source", elem.getOrDefault("source", "").asInstanceOf[String]) //.split(" ").toList)
            elem.replace("tag_string", elem.getOrDefault("tag_string", "").asInstanceOf[String].split(" ").toList)
            elem.replace("pool_string", elem.getOrDefault("pool_string", "").asInstanceOf[String])
            elem.replace("tag_string_general", elem.getOrDefault("tag_string_general", "").asInstanceOf[String].split(" ").toList)
            elem.replace("tag_string_character", elem.getOrDefault("tag_string_character", "").asInstanceOf[String].split(" ").toList)
            elem.replace("tag_string_copyright", elem.getOrDefault("tag_string_copyright", "").asInstanceOf[String].split(" ").toList)
            elem.replace("tag_string_artist", elem.getOrDefault("tag_string_artist", "").asInstanceOf[String].split(" ").toList)
            elem.replace("tag_string_meta", elem.getOrDefault("tag_string_meta", "").asInstanceOf[String].split(" ").toList)

            // There are tag names which include quotation marks (") e.g - "sakura"_hime
            // Those quotation marks are replaced with ' e.g. - 'sakura'_hime  so that they can be written into JSONs
            // (this needs to be done otherwise an error occurs when trying to parse those fields into a json when writing them into the database)
            elem.replace("tag_string", elem.get("tag_string").asInstanceOf[List[String]].map(_.replaceAll("\"", "'")))
            elem.replace("tag_string_general", elem.get("tag_string_general").asInstanceOf[List[String]].map(_.replaceAll("\"", "'")))
            elem.replace("tag_string_character", elem.get("tag_string_character").asInstanceOf[List[String]].map(_.replaceAll("\"", "'")))
            elem.replace("tag_string_copyright", elem.get("tag_string_copyright").asInstanceOf[List[String]].map(_.replaceAll("\"", "'")))
            elem.replace("tag_string_artist", elem.get("tag_string_artist").asInstanceOf[List[String]].map(_.replaceAll("\"", "'")))

            elem
          }
        )

        // the now created documents are put together via union into a bigger batch before being written into the db
        finData = finData.union(parsed)

        if (page == maxPage) {
          val lastDate = parsed.map(_.get("created_at").asInstanceOf[String].split("T").head).min
          fw.append("\n Reached MaxPage for " + searchTag + " on date " + lastDate + " with " + (dataCount + finData.count()) + " images")
          val endIntervalTime = System.currentTimeMillis()
          val durationSeconds = (endIntervalTime - startIntervalTime) / 1000.toDouble
          fw.append("\n Images/per Second: " + (finData.count() / durationSeconds) )
          fw.append("\n A new loop will be started from the given date")
          throw MaxPageReachedDownloadingImages(searchTag, lastDate)
        }

        // If the given BatchIntervall is reached the batch is written to the db
        if (page % batchSize == 0) {
          dataCount += finData.count()
          val collection: MongoCollection[Document] = database.getCollection(tableName)
          val fin = finData.collect()
          val h = collection.insertMany(fin).head()
          val endIntervalTime = System.currentTimeMillis()
          val durationSeconds = (endIntervalTime - startIntervalTime) / 1000.toDouble
          //fw.append("\n Duration Intervall (" + (page - batchSize) + " - " + page + ") : " + durationSeconds + "s | " + (finData.count() / durationSeconds) + " Images/per Second")
          finData = sc.emptyRDD[Document]
          startIntervalTime = System.currentTimeMillis()
        }
      }
    }


    catch {
      case _: AllDoneDownloading => //Expected Outcome
        val collection: MongoCollection[Document] = database.getCollection(tableName)
        val fin = finData.collect()

        if (fin.nonEmpty) {
          collection.insertMany(fin).head()
        }

      case _: RetryCountExceeded =>
        //Outcome which occurs when the retryCount is exceeded. The already obtained data is written to the database and a message was written into the log
        val collection: MongoCollection[Document] = database.getCollection(tableName)
        val fin = finData.collect()

        //In the case the the last page is reached on the first site of a new batch this batch is empty so it cant be written into the database
        if (fin.nonEmpty) {
          collection.insertMany(fin).head()
        }



      //Outcome which can occur when more images are available but the max page for the current account level is reached
      //The oldest date for an image is returned so that a new search from the given date can be started
      case x: MaxPageReachedDownloadingImages => val collection: MongoCollection[Document] = database.getCollection(tableName)
        val fin = finData.collect()
        if (fin.nonEmpty) {
          collection.insertMany(fin).head()
        }

        lastDate = x.date

      //Outome if any other error occurs the message is then logged and the method is leavt
      case x: Throwable =>
        fw.write("\n An exception occured while downloading " + searchTag + " !")
        fw.write("\n" + x)
        fw.write("\n Leaving Method")

    }
    Option(lastDate)
  }

  /**
   * This method performs a get request to obtain the metadata of up to 200 images for a given tag
   *
   * @param searchTag    - the tag for which the data should be obtained
   * @param page         - the page (from the searchtag) from which the data should be obtained
   * @param date         - Option[String] this can be set to start from a given date
   * @param downloadType - Option[String] This string can be set to specify the download type:
   *                     -> update -> This means that the data is being updated (requires a given date)
   *                     -> downloadAll -> This means that all data is being fetched
   *                     -> NoValue -> This means that only data from a specific tag is fetched
   * @param username     the username of the account
   * @param api_key      the api key corresponding to the account
   * @return the data as a string (the string still needs to be modified so that the lists are correctly represented)
   */
  def customTagGetRequest(searchTag: String, page: Int, date: Option[String], downloadType: Option[String], username: String, api_key: String): String = {
    val baseUrl = "https://safebooru.donmai.us/posts.json"
    val limit = 200.toString

    val url = {
      downloadType match {
        case Some("update") =>
          baseUrl + "?tags=" +
            "+date%3A%3E%3D" + date.get +
            "&limit=" + limit +
            "&page=" + page +
            "&login=" + username +
            "&api_key=" + api_key

        case Some("downloadAll") =>
          date match {
            case Some(value) => baseUrl + "?tags=" +
              "+date%3A%3C%3D" + value +
              "&limit=" + limit +
              "&page=" + page +
              "&login=" + username +
              "&api_key=" + api_key
            case None => baseUrl + "?tags=" +
              "&limit=" + limit +
              "&page=" + page +
              "&login=" + username +
              "&api_key=" + api_key
          }
        case None => date match {
          case Some(value) => baseUrl + "?tags=" + searchTag
            .replaceAll("\"", "%22")
            .replaceAll("#", "%23")
            .replaceAll("@", "%40")
            .replaceAll(Pattern.quote("+"), "%2B")
            .replaceAll("&", "%26") +
            "+date%3A%3C%3D" + value +
            "&limit=" + limit +
            "&page=" + page +
            "&login=" + username +
            "&api_key=" + api_key
          case None => baseUrl + "?tags=" + searchTag
            .replaceAll("\"", "%22")
            .replaceAll("#", "%23")
            .replaceAll("@", "%40")
            .replaceAll(Pattern.quote("+"), "%2B")
            .replaceAll("&", "%26") +
            "&limit=" + limit +
            "&page=" + page +
            "&login=" + username +
            "&api_key=" + api_key
        }
      }
    }

    println(url)
    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(7500) // 7.5s
    connection.setReadTimeout(10000) // 10s
    connection.setRequestMethod("GET")
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream)("UTF-8").mkString
    if (inputStream != null) inputStream.close()
    content
  }

  /**
   * This method performs a get request to obtain a list of implications
   *
   * @param page         - the page (from the searchtag) from which the data should be obtained
   * @param username     the username of the account
   * @param api_key      the api key corresponding to the account
   * @return the data as a string
   */
  def getRequestImplicationCopyrightList(page: Int, username: String, api_key: String): String = {
    val baseUrl = "https://danbooru.donmai.us/tag_implications.json?commit=Search&search[antecedent_tag][category]=3&search[consequent_tag][category]=3&search[status]=Active"
    val limit = 200.toString

    val url = baseUrl + "&limit=" + limit +
      "&page=" + page +
      "&login=" + username +
      "&api_key=" + api_key

    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(7500) // 7.5s
    connection.setReadTimeout(10000) // 10s
    connection.setRequestMethod("GET")
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream)("UTF-8").mkString
    if (inputStream != null) inputStream.close()
    content
  }
}
