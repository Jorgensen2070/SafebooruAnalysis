import CacheData.cacheData
import CharacterRoutes.getCharacterRoutes
import CopyrightRoutes.getCopyrightRoutes
import GeneralInformationRoutes.getGeneralInformationRoutes
import GeneralTagRoutes.getGeneralTagRoutes
import LatestImages.getLatestImagesRoute
import TagGroupRoutes.getTagGroupRoutes
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.toSparkContextFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

import java.time.LocalDate
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor
import scala.language.postfixOps

object AkkaMain extends CORSHandler {
  // Here the top lists are defined in the case of the copy and the tag list the top entries are selected
  // in the case of the character list the top chars are selected with the restriction of one char per copyright
  val copyList: Array[String] = Array("touhou", "original", "kantai_collection", "fate_(series)", "idolmaster", "pokemon", "vocaloid", "azur_lane", "hololive")
  val charList: Array[String] = Array("hatsune_miku", "hakurei_reimu", "artoria_pendragon_(fate)", "akemi_homura", "admiral_(kancolle)", "souryuu_asuka_langley", "akiyama_mio", "fate_testarossa", "suzumiya_haruhi")
  val tagList: Array[String] = Array("1girl", "solo", "long_hair", "breasts", "blush", "looking_at_viewer", "smile", "short_hair", "open_mouth")

  val conf: SparkConf = new SparkConf().
    setAppName("MongoSparkConnector").
    setMaster("local[*]") // the * means every available core will be used
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "4g")

  val sc = new SparkContext(conf)

  val connectionString: String = sys.env("SAFEANA_MONGO") + sys.env("DATA_DATABASE_NAME") + "."

  val authString: String = "?authSource="+ sys.env("USER_DATABASE_NAME")

  /**
   * This method returns returns a collection specified by the given name as an RDD
   *
   * @param collectionName - name of the collection that should be returned
   * @param sparkContext   - the current sparkContext
   * @return - the collection as an RDD
   */
  def getRDD(collectionName: String, sparkContext: SparkContext): RDD[Document] = {
    sparkContext.loadFromMongoDB(ReadConfig(Map("uri" -> (connectionString + collectionName + authString)))).rdd
  }

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "my-system")
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext: ExecutionContextExecutor = system.executionContext

    val endDate = LocalDate.now()
    val beginDate = LocalDate.now().minusDays(31)
    val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList


    val implicationToCopyrightMap = getRDD("implications", sc).flatMap(
      elem => (elem.get("implications").asInstanceOf[java.util.ArrayList[String]].asScala.map(ent => (ent -> elem.get("_id").asInstanceOf[String])))
    ).collectAsMap().toMap
    val implicationsRDD = getRDD("implications", sc).map(elem => (elem.get("_id").asInstanceOf[String], elem.get("implications").asInstanceOf[java.util.ArrayList[String]].asScala.toList))
    val copyrightArray = getRDD("implications", sc).flatMap(elem => elem.get("implications").asInstanceOf[java.util.ArrayList[String]].asScala).collect()

    // ---------- CopyrightRDDs ----------

    val copyrightByYear = getRDD("copyrightByYear", sc)
    val copyrightByMonth = getRDD("copyrightByMonth", sc)
    val copyrightByDay = getRDD("copyrightByDay", sc)
    val copyrightTotal = getRDD("copyrightTotal", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
        elem.get("count").asInstanceOf[Int]
      ))

    // ---------- CharacterRDDs ----------

    val characterTotal = getRDD("characterTotal", sc)
      .map(elem => (elem.get("_id").asInstanceOf[Document].get("character").toString,
        elem.get("count").asInstanceOf[Int]))
    val characterByYear = getRDD("characterByYear", sc)
    val characterByMonth = getRDD("characterByMonth", sc)
    val characterByDay = getRDD("characterByDay", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("character").toString,
        ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
          elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int],
          elem.get("_id").asInstanceOf[Document].get("day").asInstanceOf[Int]),
          elem.get("count").asInstanceOf[Int])
      )).filter(x => dateList.contains(x._2._1)).cache()

    // ---------- tagRDDs ----------

    val generlTagTotal = getRDD("generalTagTotal", sc)
    val generlTagByYear = getRDD("generalTagByYear", sc)
    val generlTagByMonth = getRDD("generalTagByMonth", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("tag").toString,
        ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
          elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
          elem.get("count").asInstanceOf[Int])
      )) //.cache()
    val generlTagByDay = getRDD("generalTagByDay", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("tag").toString,
        ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
          elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int],
          elem.get("_id").asInstanceOf[Document].get("day").asInstanceOf[Int]),
          elem.get("count").asInstanceOf[Int])
      )).filter(x => dateList.contains(x._2._1)).cache()

    // ---------- avgUpScoreRDDs ----------

    val avgUpScoreTotal = getRDD("avgUpScoreTotal", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("tag").toString, elem.get("summedUpScore").asInstanceOf[Int].toDouble / elem.get("summedCount").asInstanceOf[Int])
    ).cache()

    val avgUpScoreYear = getRDD("avgUpScoreByYear", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("tag").toString, (
        elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]
        , elem.get("summedUpScore").asInstanceOf[Int].toDouble / elem.get("summedCount").asInstanceOf[Int]))
    ).cache()

    val avgUpScoreMonth = getRDD("avgUpScoreByMonth", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("tag").toString, (
        (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
          elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int])
        , elem.get("summedUpScore").asInstanceOf[Int].toDouble / elem.get("summedCount").asInstanceOf[Int]))
    ).cache()

    // ---------- otherRDDS ----------

    val mainCopyrightToCharacterRDD = getRDD("mainCopyrightToCharacterMap", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("copyright").toString, elem.get("character").asInstanceOf[java.util.ArrayList[String]].asScala.toList))
    val characterToMainCopyright = getRDD("characterMainCopyrightCombination", sc).map(
      elem => (elem.get("_id").asInstanceOf[Document].get("character").toString, elem.get("copyright").toString)
    ).collectAsMap().toMap

    val latestImageRDD = getRDD("latestImages", sc)

    // -------------------- Section for Combination Collections which are cached in order to improve access times --------------------
    // Depending on the availability of RAM some caches should be removed in order to not cause out of memory exceptions/constant refetches

    val copyrightPairings = getRDD("copyrightPairings", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("source").toString,
        elem.get("_id").asInstanceOf[Document].get("target").toString,
        elem.get("val").asInstanceOf[Int]
      )).cache()

    val characterCombinationRDD = getRDD("characterPairings", sc).map(elem =>
      (elem.get("_id").asInstanceOf[Document].get("source").toString,
        elem.get("_id").asInstanceOf[Document].get("target").toString,
        elem.get("val").asInstanceOf[Int]
      )).cache()

    val characterCopyrightCombinationRDD = getRDD("characterCopyrightCombinationsDetailed", sc)
      .map(elem => ((elem.get("_id").asInstanceOf[Document].get("character").toString, elem.get("_id").asInstanceOf[Document].get("copyright").toString), elem.get("val").asInstanceOf[Int]))
      .cache()

    val characterCopyrightCombinationsYearRDD = getRDD("characterCopyrightCombinationsYearDetailed", sc).map(elem =>
      (((elem.get("_id").asInstanceOf[Document].get("character").toString,
        elem.get("_id").asInstanceOf[Document].get("copyright").toString),
        elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int]),
        elem.get("val").asInstanceOf[Int]
      )).cache()

    val characterCopyrightCombinationsMonthRDD = getRDD("characterCopyrightCombinationsMonthDetailed", sc).map(elem =>
      (((elem.get("_id").asInstanceOf[Document].get("character").toString,
        elem.get("_id").asInstanceOf[Document].get("copyright").toString),
        elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
        elem.get("val").asInstanceOf[Int]
      )).cache()

    val characterCopyrightCombinationsDayRDD = getRDD("characterCopyrightCombinationsDaysDetailed", sc).map(elem =>
      (((elem.get("_id").asInstanceOf[Document].get("character").toString,
        elem.get("_id").asInstanceOf[Document].get("copyright").toString),
        elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
        elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int],
        elem.get("_id").asInstanceOf[Document].get("day").asInstanceOf[Int]),
        elem.get("val").asInstanceOf[Int]
      )).filter(x => dateList.contains((x._1._2, x._1._3, x._1._4))) //.cache()

    val characterGeneralTagCombinationsRDD = getRDD("characterGeneralTagCombinations", sc).map(elem =>
      ((elem.get("_id").asInstanceOf[Document].get("character").toString,
        elem.get("_id").asInstanceOf[Document].get("tag").toString),
        elem.get("val").asInstanceOf[Int]))
      .cache()

    // Here all the data is loaded into the cache
    cacheData(
      characterByDay,
      generlTagByMonth,
      generlTagByDay,
      copyrightPairings,
      characterCombinationRDD,
      characterCopyrightCombinationRDD,
      characterCopyrightCombinationsYearRDD,
      characterCopyrightCombinationsMonthRDD,
      characterCopyrightCombinationsDayRDD,
      characterGeneralTagCombinationsRDD,
      avgUpScoreYear,
      avgUpScoreMonth,
    )

    val routes = {
      concat(
        getCopyrightRoutes("copyright",
          copyrightArray,
          implicationToCopyrightMap,
          implicationsRDD,
          copyrightByYear,
          copyrightByMonth,
          copyrightByDay,
          copyrightTotal,
          copyrightPairings,
          characterTotal,
          mainCopyrightToCharacterRDD,
          characterToMainCopyright,
          characterCopyrightCombinationRDD,
          characterCopyrightCombinationsYearRDD,
          characterCopyrightCombinationsMonthRDD,
          characterCopyrightCombinationsDayRDD,
          characterCombinationRDD),

        getCharacterRoutes("character",
          characterByYear,
          characterByMonth,
          characterByDay,
          characterTotal,
          characterToMainCopyright,
          characterCombinationRDD,
          implicationsRDD,
          characterCopyrightCombinationRDD,
          characterGeneralTagCombinationsRDD),
        getGeneralTagRoutes("generalTag",
          generlTagByYear, generlTagByMonth, generlTagByDay, generlTagTotal),
        getTagGroupRoutes("tagGroups",
          generlTagByYear, generlTagByMonth, generlTagTotal),

        getLatestImagesRoute("latestImages",
          latestImageRDD),

        getGeneralInformationRoutes("generalInformation", avgUpScoreTotal, avgUpScoreYear, avgUpScoreMonth),

        // This route exists to end current jobs
        // It can be called to end the current jobs of the sparkContext in case the calculation takes to long
        /// Another use case is if new tasks will be given to the context in order to immediately start with the new ones
        // This route however may cause some problems if the website is used by multiple users due to the possibility of one user endings the tasks of another user
        path("endCurrentJobs") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, {
              try {
                sc.cancelAllJobs()
                "Jobs were cancelled successfully!"
              } catch {
                case _: Throwable => "Jobs were not cancelled successfully!"
              }
            })
            ))
          }
        },
      )
    }

    Http().newServerAt("0.0.0.0", 8080).bind(routes)
  }
}
