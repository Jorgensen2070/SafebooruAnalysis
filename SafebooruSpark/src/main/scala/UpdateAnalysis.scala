import Analysis.{countCharacter, countCopyright, countTag, getAvgUpScore, getCharacterCombinations, getCharacterGeneralTagCombinations, getCharacterMainCopyrightCombinations, getCopyrightCombinations, getLatestImages}
import Utilities.{getCharacterCopyrightCombinationsList, getCharacterCopyrightCombinationsRDD, getCharacters, getCopyrights, getImplicationsMap, getTime, getTimestamp}
import UtilitiesAnalysis.{groupAvgMonthValueByYear, groupAvgValueByMonth, groupAvgYearValueByTotal, groupCombinationCountByMonth, groupCombinationCountByTotal, groupCombinationCountByYear, groupDayCountByMonth, groupMonthCountBYear, groupYearCountByTotal}
import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions, toSparkContextFunctions}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.collection.JavaConverters._
import java.io.FileWriter
import java.time.LocalDateTime
import java.util

object UpdateAnalysis {
  val connectionString: String = sys.env("SAFEANA_MONGO") + sys.env("DATA_DATABASE_NAME") + "."

  val authString: String = "?authSource="+ sys.env("USER_DATABASE_NAME")

  def getRDD(collectionName: String, sparkContext: SparkContext): RDD[Document] = {
    sparkContext.loadFromMongoDB(ReadConfig(Map("uri" -> (connectionString + collectionName + authString)))).rdd
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]") //This line can be uncommented so that only one core is used. The * causes spark to use all available cores.
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", connectionString + "allImageData" + authString)
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = MongoSpark.load(sc).rdd
    val fw = new FileWriter("UpdateSparkLog.txt", true)

    val startTimeMillis = System.currentTimeMillis()
    val todaysDate = LocalDateTime.now().toString
    fw.append("\n Started SafebooruSpark Update")
    fw.append("\n Current date " + todaysDate)
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

    val filteredToRequiredYears = rdd.filter(entry => filterDate.map(_._1).contains(getTimestamp(entry).getYear))
    val filteredToRequiredMonths = filteredToRequiredYears.filter(entry => filterDate.contains((getTimestamp(entry).getYear, getTimestamp(entry).getMonthValue))).cache()

    val oldLatestImages = getRDD("latestImages", sc)

    updateLatestImages(filteredToRequiredMonths, oldLatestImages, fw, true)
    updateAvgUpScore(sc, filteredToRequiredMonths, filterDate.map(_._1), fw, true)

    updateCountCopyrightTotal(sc, filteredToRequiredMonths, filterDate.map(_._1), fw, true)
    updateCountCharacterTotal(sc, filteredToRequiredMonths, filterDate.map(_._1), fw, true)
    updateCountGeneralTagsTotal(sc, filteredToRequiredMonths, filterDate.map(_._1), fw, true)

    updateCharacterCombinations(sc, filteredToRequiredMonths, filterDate.map(_._1), fw, true)
    updateCopyrightCombinations(sc, filteredToRequiredMonths, filterDate.map(_._1), fw, true)
    updateCharacterCopyrightCombinationsDetailed(sc, filteredToRequiredMonths, filterDate.map(_._1), fw, true)
    updateCharacterGeneralTagCombinations(rdd, fw, true)

    val copyrightToImplicationMap = getImplicationsMap(sc, "implications")
    val implicationToCopyrightMap = copyrightToImplicationMap.flatMap(entry => entry._2.map(implication => implication -> entry._1))
    val characterTotalRDD = sc.loadFromMongoDB(ReadConfig(Map("uri" -> (connectionString + "characterTotal" + authString)))).rdd
    val characterCopyrightCombinations = getCharacterCopyrightCombinationsRDD(sc, "characterCopyrightCombinationsDetailed")
    updateCharacterMainCopyrightCombinations(characterTotalRDD, characterCopyrightCombinations, implicationToCopyrightMap, fw, true)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    fw.append("\n Ended SafebooruSpark Update Application:")
    fw.append("\n Duration: " + durationSeconds + "\n ")
    fw.close()
  }

  /**
   * This method updates the data regarding the latest images per tag
   *
   * @param rdd                - rdd containing the imagedata, this should be filtered to the data that was updated
   * @param oldLatestImagesRDD - rdd containing the current data in the mongoDB regarding the latest images
   * @param fw                 - filewriter used for writing the log
   * @param writeToDB          boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateLatestImages(rdd: RDD[Document], oldLatestImagesRDD: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val newImages = getLatestImages(rdd)

    val oldImages = oldLatestImagesRDD.map(elem => {
      (
        elem.get("_id").asInstanceOf[Document].get("tag").asInstanceOf[String],
        elem.get("images").asInstanceOf[util.ArrayList[Document]].asScala.toList.map(ent => (ent.get("id").asInstanceOf[Int], ent.get("url").asInstanceOf[String])))
    })

    val latesteImages = newImages.join(oldImages).mapValues(elem => {
      (elem._1 ++ elem._2).distinct.sortBy(-_._1).take(5)
    })

    if (writeToDB) {
      val latestImageDocs = latesteImages.map(entry =>
        Document.parse(
          "{_id: {" +
            "tag: \"" + entry._1 +
            "\"},images:  " + entry._2.map(elem => "{id : " + elem._1 + ",url: \"" + elem._2 + "\" }").toArray.mkString("[", ", ", "]") + "}"))

      latestImageDocs.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "latestImages" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateLatestImages method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /**
   * This method updates the avgUpScore values per tag
   *
   * @param sc         - the current sparkContext, this is used to obtain other RDDs within the method
   * @param rdd        - RDD containing the imagedata, this should be filtered to the data that was updated
   * @param filterDate - List[Int] - of the years that were present in the timeframe of the data that was updated (e.g List[2021,2022])
   * @param fw         - filewriter used for writing the log
   * @param writeToDB  boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateAvgUpScore(sc: SparkContext, rdd: RDD[Document], filterDate: List[Int], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val proc = getAvgUpScore(rdd)
    val procMonth = groupAvgValueByMonth(proc)

    if (writeToDB) {
      val docsMonth = procMonth.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2._1 +
          ",month: " + entry._1._2._2 +
          "},summedUpScore: " + entry._2._1 + ",summedCount: " + entry._2._2 + "}"))
      docsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "avgUpScoreByMonth" + authString))))
    }

    val monthRDD = getRDD("avgUpScoreByMonth", sc)
      .filter(elem => filterDate.contains(elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]))
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("tag").toString.replace("\\", "\\\\"),
          (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
            elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
        ), (elem.get("summedUpScore").asInstanceOf[Int], elem.get("summedCount").asInstanceOf[Int])))

    val yearRDD = groupAvgMonthValueByYear(monthRDD)

    if (writeToDB) {
      val docsYear = yearRDD.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},summedUpScore: " + entry._2._1 + ",summedCount: " + entry._2._2 + "}"))
      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "avgUpScoreByYear" + authString))))
    }

    val updatedYearRDD = getRDD("avgUpScoreByYear", sc)
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("tag").toString.replace("\\", "\\\\"),
          elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        ), (elem.get("summedUpScore").asInstanceOf[Int], elem.get("summedCount").asInstanceOf[Int])))

    val totalRdd = groupAvgYearValueByTotal(updatedYearRDD)

    if (writeToDB) {
      val docsTotal = totalRdd.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1 +
          "\"},summedUpScore: " + entry._2._1 + ",summedCount: " + entry._2._2 + "}"))
      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "avgUpScoreTotal" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateAvgUpScore method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /**
   * This method updates the counts regarding the copyrights
   *
   * @param sc         - the current sparkContext, this is used to obtain other RDDs within the method
   * @param rdd        - RDD containing the imagedata, this should be filtered to the data that was updated
   * @param filterDate - List[Int] - of the years that were present in the timeframe of the data that was updated (e.g List[2021,2022])
   * @param fw         - filewriter used for writing the log
   * @param writeToDB  boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateCountCopyrightTotal(sc: SparkContext, rdd: RDD[Document], filterDate: List[Int], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procDay = countCopyright(rdd)

    val procMonth = groupDayCountByMonth(procDay)

    if (writeToDB) {
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

    //Here only the year value for the affected years is recalculated
    val monthRdd = getRDD("copyrightByMonth", sc)
      .filter(elem => filterDate.contains(elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]))
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
          (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
            elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
        ), elem.get("count").asInstanceOf[Int]))

    val yearRdd = groupMonthCountBYear(monthRdd)

    if (writeToDB) {
      val docsYear = yearRdd.map(entry => Document.parse(
        "{_id: {" +
          "copyright: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},count: " + entry._2 + "}"))

      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightByYear" + authString))))
    }

    val updatedYearRDD = getRDD("copyrightByYear", sc)
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
          elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        ), elem.get("count").asInstanceOf[Int]))

    val totalRdd = groupYearCountByTotal(updatedYearRDD)

    if (writeToDB) {

      val docsTotal = totalRdd.map(entry => Document.parse(
        "{_id: {" +
          "copyright: \"" + entry._1 +
          "\"},count: " + entry._2 + "}"))

      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightTotal" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateCountCopyrightTotal method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /**
   * This method updates the counts regarding the characters
   *
   * @param sc         - the current sparkContext, this is used to obtain other RDDs within the method
   * @param rdd        - RDD containing the imagedata, this should be filtered to the data that was updated
   * @param filterDate - List[Int] - of the years that were present in the timeframe of the data that was updated (e.g List[2021,2022])
   * @param fw         - filewriter used for writing the log
   * @param writeToDB  boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateCountCharacterTotal(sc: SparkContext, rdd: RDD[Document], filterDate: List[Int], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procDay = countCharacter(rdd)

    val procMonth = groupDayCountByMonth(procDay)

    if (writeToDB) {
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

    //Here only the year value for the affected years is recalculated
    val monthRdd = getRDD("characterByMonth", sc)
      .filter(elem => filterDate.contains(elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]))
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("character").toString,
          (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
            elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
        ), elem.get("count").asInstanceOf[Int]))

    val yearRdd = groupMonthCountBYear(monthRdd)

    if (writeToDB) {
      val docsYear = yearRdd.map(entry => Document.parse(
        "{_id: {" +
          "character: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},count: " + entry._2 + "}"))

      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterByYear" + authString))))
    }

    val updatedYearRDD = getRDD("characterByYear", sc)
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("character").toString,
          elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        ), elem.get("count").asInstanceOf[Int]))

    val totalRdd = groupYearCountByTotal(updatedYearRDD)

    if (writeToDB) {
      val docsTotal = totalRdd.map(entry => Document.parse(
        "{_id: {" +
          "character: \"" + entry._1 +
          "\"},count: " + entry._2 + "}"))

      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterTotal" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateCountCharacterTotal method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")

  }

  /**
   * This method updates the counts regarding the generalTagws
   *
   * @param sc         - the current sparkContext, this is used to obtain other RDDs within the method
   * @param rdd        - RDD containing the imagedata, this should be filtered to the data that was updated
   * @param filterDate - List[Int] - of the years that were present in the timeframe of the data that was updated (e.g List[2021,2022])
   * @param fw         - filewriter used for writing the log
   * @param writeToDB  boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateCountGeneralTagsTotal(sc: SparkContext, rdd: RDD[Document], filterDate: List[Int], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procDay = countTag(rdd)

    val procMonth = groupDayCountByMonth(procDay)

    if (writeToDB) {
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

    val monthRdd = getRDD("generalTagByMonth", sc)
      .filter(elem => filterDate.contains(elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]))
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("tag").toString.replace("\\", "\\\\"),
          (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
            elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
        ), elem.get("count").asInstanceOf[Int]))

    val yearRdd = groupMonthCountBYear(monthRdd)

    if (writeToDB) {
      val docsYear = yearRdd.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1._1 +
          "\",year: " + entry._1._2 +
          "},count: " + entry._2 + "}"))

      docsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "generalTagByYear" + authString))))
    }

    val updatedYearRDD = getRDD("generalTagByYear", sc)
      .map(elem => (
        (elem.get("_id").asInstanceOf[Document].get("tag").toString.replace("\\", "\\\\"),
          elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        ), elem.get("count").asInstanceOf[Int]))

    val totalRdd = groupYearCountByTotal(updatedYearRDD)

    if (writeToDB) {
      val docsTotal = totalRdd.map(entry => Document.parse(
        "{_id: {" +
          "tag: \"" + entry._1 +
          "\"},count: " + entry._2 + "}"))

      docsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "generalTagTotal" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateCountGeneralTagsTotal method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /**
   * This method updates the combinations between characters
   *
   * @param sc         - the current sparkContext, this is used to obtain other RDDs within the method
   * @param rdd        - RDD containing the imagedata, this should be filtered to the data that was updated
   * @param filterDate - List[Int] - of the years that were present in the timeframe of the data that was updated (e.g List[2021,2022])
   * @param fw         - filewriter used for writing the log
   * @param writeToDB  boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateCharacterCombinations(sc: SparkContext, rdd: RDD[Document], filterDate: List[Int], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val proc = getCharacterCombinations(rdd)
    val procMonths = groupCombinationCountByMonth(proc)

    if (writeToDB) {
      val docsProcessedCharacterCombinationsMonth = procMonths.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2._1 +
          ", month: " + elem._1._2._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterPairingsMonth" + authString))))
    }

    val monthRdd = getRDD("characterPairingsMonth", sc)
      .filter(elem => filterDate.contains(elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]))
      .map(elem => (
        ((elem.get("_id").asInstanceOf[Document].get("source").toString,
          elem.get("_id").asInstanceOf[Document].get("target").toString
        ),
          (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
            elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
        ), elem.get("val").asInstanceOf[Int]))

    val yearRdd = groupCombinationCountByYear(monthRdd)

    if (writeToDB) {
      val docsProcessedCharacterCombinationsYear = yearRdd.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterPairingsYear" + authString))))
    }

    val updatedYearRDD = getRDD("characterPairingsYear", sc)
      .map(elem => (
        ((elem.get("_id").asInstanceOf[Document].get("source").toString,
          elem.get("_id").asInstanceOf[Document].get("target").toString
        ),
          elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        ), elem.get("val").asInstanceOf[Int]))

    val totalRdd = groupCombinationCountByTotal(updatedYearRDD) //.filter(_._2 > 5)

    if (writeToDB) {
      val docsProcessedCharacterCombinations = totalRdd.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1 + "\", target: \"" + elem._1._2 + "\"},val: " + elem._2 + "}"))
      docsProcessedCharacterCombinations.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterPairings" + authString))))
    }


    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateCharacterCombinations method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /**
   * This method updates the combinations between copyrights
   *
   * @param sc         - the current sparkContext, this is used to obtain other RDDs within the method
   * @param rdd        - RDD containing the imagedata, this should be filtered to the data that was updated
   * @param filterDate - List[Int] - of the years that were present in the timeframe of the data that was updated (e.g List[2021,2022])
   * @param fw         - filewriter used for writing the log
   * @param writeToDB  boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateCopyrightCombinations(sc: SparkContext, rdd: RDD[Document], filterDate: List[Int], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val proc = getCopyrightCombinations(rdd)
    val procMonths = groupCombinationCountByMonth(proc)

    if (writeToDB) {
      val docsProcessedCharacterCombinationsMonth = procMonths.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2._1 +
          ", month: " + elem._1._2._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsMonth.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightPairingsMonth" + authString))))
    }

    val monthRdd = getRDD("copyrightPairingsMonth", sc)
      .filter(elem => filterDate.contains(elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]))
      .map(elem => (
        ((elem.get("_id").asInstanceOf[Document].get("source").toString,
          elem.get("_id").asInstanceOf[Document].get("target").toString
        ),
          (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
            elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
        ), elem.get("val").asInstanceOf[Int]))

    val yearRdd = groupCombinationCountByYear(monthRdd)

    if (writeToDB) {
      val docsProcessedCharacterCombinationsYear = yearRdd.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1._1 + "\", target: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCharacterCombinationsYear.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightPairingsYear" + authString))))
    }

    val updatedYearRDD = getRDD("copyrightPairingsYear", sc)
      .map(elem => (
        ((elem.get("_id").asInstanceOf[Document].get("source").toString,
          elem.get("_id").asInstanceOf[Document].get("target").toString
        ),
          elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        ), elem.get("val").asInstanceOf[Int]))

    val totalRdd = groupCombinationCountByTotal(updatedYearRDD) //.filter(_._2 > 5)

    if (writeToDB) {
      val docsProcessedCharacterCombinations = totalRdd.map(
        elem => Document.parse("{_id: {source: \"" + elem._1._1 + "\", target: \"" + elem._1._2 + "\"},val: " + elem._2 + "}"))
      docsProcessedCharacterCombinations.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "copyrightPairings" + authString))))
    }


    val endTimeSection = System.currentTimeMillis()
    val sectionDurationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateCopyrightCombinations method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }

  /**
   * This method updates the combinations between characters and copyrights
   *
   * @param sc         - the current sparkContext, this is used to obtain other RDDs within the method
   * @param rdd        - RDD containing the imagedata, this should be filtered to the data that was updated
   * @param filterDate - List[Int] - of the years that were present in the timeframe of the data that was updated (e.g List[2021,2022])
   * @param fw         - filewriter used for writing the log
   * @param writeToDB  boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateCharacterCopyrightCombinationsDetailed(sc: SparkContext, rdd: RDD[Document], filterDate: List[Int], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procDays = rdd.filter(ent => getCharacters(ent).nonEmpty && getCopyrights(ent).nonEmpty)
      .flatMap(image => getCharacterCopyrightCombinationsList(image).map(pair => (pair, getTime(image), image.get("_id"))))
      .groupBy(x => (x._1, x._2._1, x._2._2, x._2._3)).mapValues(_.size)

    val procMonthsFromDays = procDays.groupBy(x => (x._1._1, x._1._2, x._1._3)).mapValues(_.map(_._2).sum)

    if (writeToDB) {
      val docsProcessedCombinationsMonths = procMonthsFromDays.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1._1 + "\", copyright: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          ", month: " + elem._1._3 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCombinationsMonths.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsMonthDetailed" + authString))))

      val docsProcessedCombinationsDays = procDays.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1._1 + "\", copyright: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          ", month: " + elem._1._3 +
          ", day: " + elem._1._4 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCombinationsDays.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsDaysDetailed" + authString))))
    }

    val monthRdd = getRDD("characterCopyrightCombinationsMonthDetailed", sc)
      .filter(elem => filterDate.contains(elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]))
      .map(elem => ((
        (elem.get("_id").asInstanceOf[Document].get("character").toString,
          elem.get("_id").asInstanceOf[Document].get("copyright").toString),
        elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int],
      ), elem.get("val").asInstanceOf[Int]))

    val procYear = monthRdd.groupBy(x => (x._1._1, x._1._2)).mapValues(_.map(_._2).sum)

    if (writeToDB) {
      val docsProcessedCombinationsYears = procYear.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1._1 + "\", copyright: \"" + elem._1._1._2 + "\"" +
          ", year: " + elem._1._2 +
          "}, val: " + elem._2 + "}"))
      docsProcessedCombinationsYears.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsYearDetailed" + authString))))
    }

    val yearRdd = getRDD("characterCopyrightCombinationsYearDetailed", sc)
      .map(elem => ((
        (elem.get("_id").asInstanceOf[Document].get("character").toString,
          elem.get("_id").asInstanceOf[Document].get("copyright").toString.replace("\\", "\\\\")),
        elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
      ), elem.get("val").asInstanceOf[Int]))

    val procTotal = yearRdd.groupBy(_._1._1).mapValues(_.map(_._2).sum) //.filter(_._2 > 5)

    if (writeToDB) {
      val docsProcessedCharacterCopyrightCombinationsTotal = procTotal.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1 + "\", copyright: \"" + elem._1._2 + "\"},val: " + elem._2 + "}"))
      docsProcessedCharacterCopyrightCombinationsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterCopyrightCombinationsDetailed" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val durationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateCharacterCopyrightCombinationsDetailed method:")
    fw.append("\n   Duration: " + durationSeconds + "\n ")
  }

  /**
   * This method regenerates the data regarding connections between characters and generalTags
   * It takes the  the given RDD and uses the getCharacterGeneralTagCombinations method with it
   * The result of this are then saved in the database
   *
   * @param rdd       - rdd containing all imageData (this SHOULD NOT be filtered!)
   * @param fw        - filewriter used for writing the log
   * @param writeToDB boolean
   *                  - yes - data will be written to the database
   *                  - no - data will not be written to the database
   */
  def updateCharacterGeneralTagCombinations(rdd: RDD[Document], fw: FileWriter, writeToDB: Boolean = false): Unit = {
    val startTimeSection = System.currentTimeMillis()

    val procTotal = getCharacterGeneralTagCombinations(rdd)

    if (writeToDB) {
      val docsProcessedCharacterCopyrightCombinationsTotal = procTotal.map(
        elem => Document.parse("{_id: {character: \"" + elem._1._1 + "\", tag: \"" + elem._1._2 + "\"" +
          "},val: " + elem._2 + "}"))
      docsProcessedCharacterCopyrightCombinationsTotal.saveToMongoDB(WriteConfig(Map("uri" -> (connectionString + "characterGeneralTagCombinations" + authString))))
    }

    val endTimeSection = System.currentTimeMillis()
    val durationSeconds = (endTimeSection - startTimeSection) / 1000
    fw.append("\n   Finished updateCharacterGeneralTagCombinations method:")
    fw.append("\n   Duration: " + durationSeconds + "\n ")
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
  def updateCharacterMainCopyrightCombinations(characterTotalRDD: RDD[Document],
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
    fw.append("\n   Finished updateCharacterMainCopyrightCombinations method:")
    fw.append("\n   Duration: " + sectionDurationSeconds + "\n ")
  }
}
