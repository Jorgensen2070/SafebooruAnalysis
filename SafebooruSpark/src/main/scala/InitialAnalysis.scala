import Analysis.{countCharacter, countCopyright, countTag, getAvgUpScore, getCharacterCombinations, getCharacterCopyrightCombinations, getCharacterGeneralTagCombinations, getCharacterMainCopyrightCombinations, getCopyrightCombinations, getLatestImages, getSoucesAll}
import Utilities.{getCharacterCopyrightCombinationsRDD, getCharacters, getCopyrights, getImplicationsMap, getTimestamp}
import UtilitiesAnalysis.{groupAvgMonthValueByYear, groupAvgValueByMonth, groupAvgYearValueByTotal, groupCombinationCountByMonth, groupCombinationCountByTotal, groupCombinationCountByYear, groupDayCountByMonth, groupMonthCountBYear, groupYearCountByTotal}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions, toSparkContextFunctions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import java.io.FileWriter
import java.time.LocalDateTime


object InitialAnalysis {
  val connectionString: String = sys.env("SAFEANA_MONGO") + sys.env("DATA_DATABASE_NAME") + "."

  val authString: String = "?authSource="+ sys.env("USER_DATABASE_NAME")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("MongoSparkConnector").
      setMaster("local[*]") //This line can be uncommented so that only one core is used. The * causes spark to use all available cores.
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
      .set("spark.mongodb.input.uri", connectionString + "allImageData" + authString)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val rdd = MongoSpark.load(sc).rdd

    val fw = new FileWriter("Log.txt", true)

    val startTimeMillis = System.currentTimeMillis()

    fw.append("\n Started SafebooruSpark\n ")

    latestImages(rdd, fw, true)
    avgUpScore(rdd, fw, true)
    countCopyrightTotal(rdd, fw, true)
    countGenerlTagsTotal(rdd, fw, true)
    countCharacterTotal(rdd, fw, true)
    generateCharacterCombinations(rdd, fw, true)
    generateCopyrightCombinations(rdd, fw, true)
    generateCharacterCopyrightCombinations(rdd, fw, true)
    generateCharacterGeneralTagCombinations(rdd, fw, true)

    val copyrightToImplicationMap = getImplicationsMap(sc, "implications")
    val implicationToCopyrightMap = copyrightToImplicationMap.flatMap(entry => entry._2.map(implication => implication -> entry._1))
    val characterTotalRDD = sc.loadFromMongoDB(ReadConfig(Map("uri" -> (connectionString + "characterTotal" + authString)))).rdd
    val characterCopyrightCombinations = getCharacterCopyrightCombinationsRDD(sc, "characterCopyrightCombinationsDetailed")
    generateCharacterMainCopyrightCombinations(characterTotalRDD, characterCopyrightCombinations, implicationToCopyrightMap, fw, true)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    fw.append("\n Ended SafebooruSpark Application:")
    fw.append("\n Duration: " + durationSeconds + "\n ")
    fw.close()
  }

  /**
   * This method takes the given RDD uses the getLatestImages method with it and saves the results into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def latestImages(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procLatestImages = getLatestImages(rdd)

    if (writeToDB) {
      val latesteImages = procLatestImages.map(entry =>
        Document.parse(
          "{_id: {" +
            "tag: \"" + entry._1 +
            "\"},images:  " + entry._2.map(elem => "{id : " + elem._1 + ",url: \"" + elem._2 + "\" }").toArray.mkString("[", ", ", "]") + "}"))

      latesteImages.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "latestImages" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished getLatestImages method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the getSoucesAll method with it
   * The results are then further reduced with the groupDayCountByMonth, groupMonthCountBYear and groupYearCountByTotal methods
   * The results of all of them are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def countSourcesAll(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procSources = getSoucesAll(rdd)

    if (writeToDB) {
      val docsTotal = procSources.map(entry =>
        Document.parse(
          "{_id: {" +
            "tag: \"" + entry._1._1 +
            "\", source: \"" + entry._1._2 +
            "\"},count: " + entry._2 + "}")
      )
      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "sourcesTotal" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished countSourcesAll method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the getAvgUpScore method with it
   * The results are then further reduced with the groupAvgValueByMonth, groupAvgMonthValueByYear and groupAvgYearValueByTotal methods
   * The results of all of them are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def avgUpScore(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val proc = getAvgUpScore(rdd)

    val procMonth = groupAvgValueByMonth(proc)

    val procYear = groupAvgMonthValueByYear(procMonth)

    val procTotal = groupAvgYearValueByTotal(procYear)

    if (writeToDB) {
      val docsTotal = procTotal.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1 +
          "\"},summedUpScore: " + entry._2._1 + ",summedCount: " + entry._2._2 + "}"))

      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "avgUpScoreTotal" + authString))))

      val docsYear = procYear.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},summedUpScore: " + entry._2._1 + ",summedCount: " + entry._2._2 + "}"))

      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "avgUpScoreByYear" + authString))))

      val docsMonth = procMonth.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          "},summedUpScore: " + entry._2._1 + ",summedCount: " + entry._2._2 + "}"))

      docsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "avgUpScoreByMonth" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished avgUpScore method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the countCopyright method with it
   * The results are then further reduced with the groupDayCountByMonth, groupMonthCountBYear and groupYearCountByTotal methods
   * The results of all of them are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def countCopyrightTotal(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procDay = countCopyright(rdd)

    val procMonth = groupDayCountByMonth(procDay)

    val procYear = groupMonthCountBYear(procMonth)

    val procTotal = groupYearCountByTotal(procYear)

    if (writeToDB) {
      val docsTotal = procTotal.map(entry => Document.parse(
        "{_id: {" +
          "copyright: \"" + entry._1 +
          "\"},count: " + entry._2 + "}"))

      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightTotal" + authString))))

      val docsYear = procYear.map(entry => Document.parse(
        "{_id: {" +
          "copyright: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},count: " + entry._2 + "}"))

      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightByYear" + authString))))

      val docsMonth = procMonth.map(entry => Document.parse(
        "{_id: {" +
          "copyright: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          "},count: " + entry._2 + "}"))

      docsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightByMonth" + authString))))

      val docsDay = procDay.map(entry => Document.parse(
        "{_id: {" +
          "copyright: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          ",day: " + entry._1._2._3 +
          "},count: " + entry._2 + "}"))

      docsDay.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightByDay" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished countCopyrightTotal method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the countCharacter method with it
   * The results are then further reduced with the groupDayCountByMonth, groupMonthCountBYear and groupYearCountByTotal methods
   * The results of all of them are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def countCharacterTotal(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procDay = countCharacter(rdd)

    val procMonth = groupDayCountByMonth(procDay)

    val procYear = groupMonthCountBYear(procMonth)

    val procTotal = groupYearCountByTotal(procYear)

    if (writeToDB) {
      val docsTotal = procTotal.map(entry => Document.parse(
        "{_id: {" +
          "character: \"" + entry._1 +
          "\"},count: " + entry._2 + "}"))

      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterTotal" + authString))))

      val docsYear = procYear.map(entry => Document.parse(
        "{_id: {" +
          "character: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},count: " + entry._2 + "}"))

      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterByYear" + authString))))

      val docsMonth = procMonth.map(entry => Document.parse(
        "{_id: {" +
          "character: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          "},count: " + entry._2 + "}"))

      docsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterByMonth" + authString))))

      val docsDay = procDay.map(entry => Document.parse(
        "{_id: {" +
          "character: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          ",day: " + entry._1._2._3 +
          "},count: " + entry._2 + "}"))

      docsDay.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterByDay" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished countCharacterTotal method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the countTag method with it
   * The results are then further reduced with the groupDayCountByMonth, groupMonthCountBYear and groupYearCountByTotal methods
   * The results of all of them are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def countGenerlTagsTotal(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procDay = countTag(rdd)

    val procMonth = groupDayCountByMonth(procDay)

    val procYear = groupMonthCountBYear(procMonth)

    val procTotal = groupYearCountByTotal(procYear)

    if (writeToDB) {
      val docsTotal = procTotal.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1 +
          "\"},count: " + entry._2 + "}"))

      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "generalTagTotal" + authString))))

      val docsYear = procYear.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},count: " + entry._2 + "}"))

      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "generalTagByYear" + authString))))

      val docsMonth = procMonth.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          "},count: " + entry._2 + "}"))

      docsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "generalTagByMonth" + authString))))

      val docsDay = procDay.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          ",day: " + entry._1._2._3 +
          "},count: " + entry._2 + "}"))

      docsDay.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "generalTagByDay" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished countGenerlTags method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the getCharacterCombinations method with it
   * The results are then further reduced with the groupCombinationCountByMonth, groupCombinationCountByYear and groupCombinationCountByTotal methods
   * The results of groupCombinationCountByMonth, groupCombinationCountByYear and groupCombinationCountByTotal are then are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def generateCharacterCombinations(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val proc = getCharacterCombinations(rdd)

    val procMonths = groupCombinationCountByMonth(proc)

    val procYears = groupCombinationCountByYear(procMonths)

    val procTotal = groupCombinationCountByTotal(procYears)

    if (writeToDB) {

      val docsProcessedCharacterCombinationsMonth = procMonths.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2._1 +
          ", month: " + elem._1._2._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterPairingsMonth" + authString))))

      val docsProcessedCharacterCombinationsYear = procYears.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterPairingsYear" + authString))))


      val docsProcessedCharacterCombinations = procTotal.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1 + "\", target: \"" + elem._1._2 + "\"},val: " + elem._2 + "}"))
      docsProcessedCharacterCombinations.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterPairings" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished generateCharacterCombinations method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the getCopyrightCombinations method with it
   * The results are then further reduced with the groupCombinationCountByMonth, groupCombinationCountByYear and groupCombinationCountByTotal methods
   * The results of groupCombinationCountByMonth, groupCombinationCountByYear and groupCombinationCountByTotal are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def generateCopyrightCombinations(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val proc = getCopyrightCombinations(rdd)

    val procMonths = groupCombinationCountByMonth(proc)

    val procYears = groupCombinationCountByYear(procMonths)

    val procTotal = groupCombinationCountByTotal(procYears)

    if (writeToDB) {

      val docsProcessedCharacterCombinationsMonth = procMonths.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2._1 +
          ", month: " + elem._1._2._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightPairingsMonth" + authString))))

      val docsProcessedCharacterCombinationsYear = procYears.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightPairingsYear" + authString))))


      val docsProcessedCharacterCombinations = procTotal.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1 + "\", target: \"" + elem._1._2 + "\"},val: " + elem._2 + "}"))
      docsProcessedCharacterCombinations.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightPairings" + authString))))
    }


    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished generateCopyrightCombinations method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and first extracts documents with months that occurred within the last 31 days
   * The data that occurred within the given months are separated and processed with getCharacterCopyrightCombinations in order to save them into the database
   * The data that occurred outside of the given months are processed with getCharacterCopyrightCombinations and grouped by month
   * After that the data in the given months are also grouped by month and unioned with the other data
   * Finally the unioned data is processed with groupCombinationCountByYear and groupCombinationCountByTotal
   * At the end the data grouped by day,month, year and total is written to the database
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def generateCharacterCopyrightCombinations(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()


    val startDate = LocalDateTime.now()
    val endDate = startDate.minusDays(31)

    val filterDate =
      if (startDate.getYear != endDate.getYear) {
        List((startDate.getYear, startDate.getMonthValue), (endDate.getYear, endDate.getMonthValue))
      } else {
        (for (i <- endDate.getMonthValue to startDate.getMonthValue) yield {
          (startDate.getYear, i)
        }).toList
      }

    val filteredToNotRequiredMonths = rdd.filter(entry => !filterDate.contains((getTimestamp(entry).getYear, getTimestamp(entry).getMonthValue)))

    val filteredToRequiredMonths = rdd.filter(entry => filterDate.contains((getTimestamp(entry).getYear, getTimestamp(entry).getMonthValue)))

    val procMonths = groupCombinationCountByMonth(
      getCharacterCopyrightCombinations(
        filteredToNotRequiredMonths.filter(ent => getCharacters(ent).nonEmpty && getCopyrights(ent).nonEmpty)))


    val procDays = getCharacterCopyrightCombinations(filteredToRequiredMonths.filter(ent => getCharacters(ent).nonEmpty && getCopyrights(ent).nonEmpty))

    val procMonthsFromDays = groupCombinationCountByMonth(procDays)

    val unionedMonths = procMonths.union(procMonthsFromDays)

    val procYear = groupCombinationCountByYear(unionedMonths)

    val procTotal = groupCombinationCountByTotal(procYear)

    if (writeToDB) {
      val docsProcessedCombinationsYears = procYear.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1._1 + "\", copyright: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCombinationsYears.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsYearDetailed" + authString))))


      val docsProcessedCombinationsMonths = unionedMonths.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1._1 + "\", copyright: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2._1 +
          ", month: " + elem._1._2._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCombinationsMonths.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsMonthDetailed" + authString))))

      val docsProcessedCombinationsDays = procDays.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1._1 + "\", copyright: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2._1 +
          ", month: " + elem._1._2._2 +
          ", day: " + elem._1._2._3 +
          "}, val: " + elem._2 + "}"))

      docsProcessedCombinationsDays.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsDaysDetailed" + authString))))

      val docsProcessedCharacterCopyrightCombinationsTotal = procTotal.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1 + "\", copyright: \"" + elem._1._2 + "\"" +
          "},val: " + elem._2 + "}"))
      docsProcessedCharacterCopyrightCombinationsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsDetailed" + authString))))
    }


    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished generateCharacterCopyrightCombinations Detailed method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /** This method takes the given RDD and uses the getCharacterGeneralTagCombinations with it
   * The results are then saved into the database if the corresponding boolean is true
   *
   * @param rdd       - rdd containing all the documents
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def generateCharacterGeneralTagCombinations(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procTotal = getCharacterGeneralTagCombinations(rdd)

    if (writeToDB) {
      val docsProcessedCharacterCopyrightCombinationsTotal = procTotal.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1 + "\", tag: \"" + elem._1._2 + "\"" +
          "},val: " + elem._2 + "}"))
      docsProcessedCharacterCopyrightCombinationsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterGeneralTagCombinations" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished generateCharacterGeneralTagCombinations method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /**
   * This method generates the given RDDs and collections and uses the getCharacterMainCopyrightCombinations with it
   * The results are then saved in the following form in the mongoDB
   *
   * - Char -> MainCopyright
   * - Copyright -> List(Chars that have this copyright as their mainCopyright)
   *
   * @param characterTotalRDD              - the RDD that contains all characters together with a count of their total occurrences
   * @param characterCopyrightCombinations - RDD[((String, String), Int)],
   *                                       - RDD that contains the combination between characters and copyrights and the count of their total occurrences
   * @param implicationToCopyrightMap      - scala.collection.Map[String, String]
   *                                       - A map that contains copyrights that originate from another copyright
   *                                         (has the form (copyright, originalCopyright)
   * @param fw                             - filewriter used for writing the log
   * @param writeToDB                      boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def generateCharacterMainCopyrightCombinations(characterTotalRDD: RDD[Document],
                                                 characterCopyrightCombinations: RDD[((String, String), Int)],
                                                 implicationToCopyrightMap: scala.collection.Map[String, String],
                                                 fw: FileWriter,
                                                 writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    //This rdd needs to be collected
    //If it is not collected an error is thrown because you cant perform an action on an rdd while in the map of another rdd
    val proc = getCharacterMainCopyrightCombinations(characterTotalRDD, characterCopyrightCombinations, implicationToCopyrightMap)

    val copyrightToCharacterMap = proc.groupBy(_._2).mapValues(_.map(_._1).toList)

    if (writeToDB) {
      val docsProcessedCharacterCopyrightCombinations = proc.map(
        elem => {
          Document.parse("{_id: {character: \"" + elem._1 + "\"},copyright: \"" + elem._2 + "\"}")
        })
      docsProcessedCharacterCopyrightCombinations.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterMainCopyrightCombination" + authString))))

      val docsMainCopyToCharMap = copyrightToCharacterMap.map(elem =>
        Document.parse("{_id: {copyright: \"" + elem._1 + "\"},character: " + elem._2.mkString("[\"", "\", \"", "\"]") + "}"))
      docsMainCopyToCharMap.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "mainCopyrightToCharacterMap" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished generateCharacterMainCopyrightCombinations method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }
}
