
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

class AnalysisTest extends AnyFunSuite with BeforeAndAfterAll {
  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _
  var multipleImageRDD: RDD[Document] = _

  val multipleImageDataName = "multipleImages.json"

  override protected def beforeAll(): Unit = {

    val conf = new SparkConf().
      setAppName("MongoSparkConnector").
      setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext


    val multipleImages = scala.io.Source.fromFile("src/test/resources/" + multipleImageDataName).mkString.replace("[{", "").replace("}]", "")
      .split("},\\{").map(elem => "{" + elem + "}")
    multipleImageRDD = sc.parallelize(multipleImages).map(elem => Document.parse(elem))
  }

  //n this test only the values for a few characters will be checked to prove the concept
  test("getLatestImages Test") {
    val latestImages = Analysis.getLatestImages(multipleImageRDD).filter(x => List("hanyuu", "furude_rika", "frederica_bernkastel").contains(x._1))
    val latestImagesArray = Array(
      ("hanyuu", List(
        (4453735, "https://cdn.donmai.us/sample/ca/57/sample-ca57f3e1fccf8bc260e151ade70473fb.jpg"),
        (2063626, "https://cdn.donmai.us/sample/b1/8a/sample-b18a4baf3c69baed1ce0a620fbd28524.jpg"),
        (1502616, "https://cdn.donmai.us/original/20/df/20dfe50d0dbc5d7b16d2628f8959dca7.jpg"),
        (1472187, "https://cdn.donmai.us/original/fd/9a/fd9aa8eac7bbe497ff3680529cc61b83.jpg"),
        (592736, "https://cdn.donmai.us/sample/fb/a7/sample-fba719ca59a88e92f881759adb89f49b.jpg"))),
      ("furude_rika",
        List(
          (2063626, "https://cdn.donmai.us/sample/b1/8a/sample-b18a4baf3c69baed1ce0a620fbd28524.jpg"),
          (1502616, "https://cdn.donmai.us/original/20/df/20dfe50d0dbc5d7b16d2628f8959dca7.jpg"),
          (1472187, "https://cdn.donmai.us/original/fd/9a/fd9aa8eac7bbe497ff3680529cc61b83.jpg"),
          (592736, "https://cdn.donmai.us/sample/fb/a7/sample-fba719ca59a88e92f881759adb89f49b.jpg"),
          (354623, "https://cdn.donmai.us/original/e8/ba/e8ba0d386715eae5fff7599ab9fd5b6c.jpg"))),
      ("frederica_bernkastel",
        List((1472187, "https://cdn.donmai.us/original/fd/9a/fd9aa8eac7bbe497ff3680529cc61b83.jpg")))
    )

    assert(latestImages.collect().toSet == latestImagesArray.toSet)
  }

  test("Initialization Test") {
    assert(multipleImageRDD.count() === 10)
  }

  //This test also covers the the values between each computation this will not be done for the countCharacter and countTag tests
  test("countCopyright Test") {
    val copyrightsByDay = Analysis.countCopyright(multipleImageRDD)
    val copyrightsByDayArray = Array(
      (("higurashi_no_naku_koro_ni", (2006, 9, 29)), 1),
      (("higurashi_no_naku_koro_ni", (2007, 10, 1)), 1),
      (("higurashi_no_naku_koro_ni", (2008, 8, 18)), 1),
      (("higurashi_no_naku_koro_ni", (2008, 9, 30)), 1),
      (("higurashi_no_naku_koro_ni", (2008, 11, 24)), 1),
      (("higurashi_no_naku_koro_ni", (2010, 1, 6)), 1),
      (("higurashi_no_naku_koro_ni", (2013, 7, 27)), 1),
      (("higurashi_no_naku_koro_ni", (2013, 9, 9)), 1),
      (("higurashi_no_naku_koro_ni", (2015, 7, 9)), 1),
      (("higurashi_no_naku_koro_ni", (2021, 4, 6)), 1),
      (("kantai_collection", (2013, 9, 9)), 1),
      (("touhou", (2007, 10, 1)), 1),
      (("umineko_no_naku_koro_ni", (2008, 9, 30)), 1),
      (("umineko_no_naku_koro_ni", (2013, 7, 27)), 1),
      (("umineko_no_naku_koro_ni", (2021, 4, 6)), 1)
    )

    assert(copyrightsByDay.collect().toSet.equals(copyrightsByDayArray.toSet))

    val copyrightsByMonth = UtilitiesAnalysis.groupDayCountByMonth(copyrightsByDay)
    val copyrightsByMonthArray = Array(
      (("higurashi_no_naku_koro_ni", (2006, 9)), 1),
      (("higurashi_no_naku_koro_ni", (2007, 10)), 1),
      (("higurashi_no_naku_koro_ni", (2008, 8)), 1),
      (("higurashi_no_naku_koro_ni", (2008, 9)), 1),
      (("higurashi_no_naku_koro_ni", (2008, 11)), 1),
      (("higurashi_no_naku_koro_ni", (2010, 1)), 1),
      (("higurashi_no_naku_koro_ni", (2013, 7)), 1),
      (("higurashi_no_naku_koro_ni", (2013, 9)), 1),
      (("higurashi_no_naku_koro_ni", (2015, 7)), 1),
      (("higurashi_no_naku_koro_ni", (2021, 4)), 1),
      (("kantai_collection", (2013, 9)), 1),
      (("touhou", (2007, 10)), 1),
      (("umineko_no_naku_koro_ni", (2008, 9)), 1),
      (("umineko_no_naku_koro_ni", (2013, 7)), 1),
      (("umineko_no_naku_koro_ni", (2021, 4)), 1)
    )

    assert(copyrightsByMonth.collect().toSet.equals(copyrightsByMonthArray.toSet))

    val copyrightsByYear = UtilitiesAnalysis.groupMonthCountBYear(copyrightsByMonth)

    val copyrightsByYearArray = Array(
      (("higurashi_no_naku_koro_ni", 2006), 1),
      (("higurashi_no_naku_koro_ni", 2007), 1),
      (("higurashi_no_naku_koro_ni", 2008), 3),
      (("higurashi_no_naku_koro_ni", 2010), 1),
      (("higurashi_no_naku_koro_ni", 2013), 2),
      (("higurashi_no_naku_koro_ni", 2015), 1),
      (("higurashi_no_naku_koro_ni", 2021), 1),
      (("kantai_collection", 2013), 1),
      (("touhou", 2007), 1),
      (("umineko_no_naku_koro_ni", 2008), 1),
      (("umineko_no_naku_koro_ni", 2013), 1),
      (("umineko_no_naku_koro_ni", 2021), 1)
    )

    assert(copyrightsByYear.collect().toSet.equals(copyrightsByYearArray.toSet))

    val copyrightsTotal = UtilitiesAnalysis.groupYearCountByTotal(copyrightsByYear)
    val copyrightsTotalArray = Array(
      ("higurashi_no_naku_koro_ni", 10),
      ("touhou", 1),
      ("kantai_collection", 1),
      ("umineko_no_naku_koro_ni", 3))

    assert(copyrightsTotal.collect().toSet.equals(copyrightsTotalArray.toSet))
  }

  test("countCharacter Test") {
    val charactersByDay = Analysis.countCharacter(multipleImageRDD)

    val charactersByMonth = UtilitiesAnalysis.groupDayCountByMonth(charactersByDay)

    val charactersByYear = UtilitiesAnalysis.groupMonthCountBYear(charactersByMonth)

    val charactersTotal = UtilitiesAnalysis.groupYearCountByTotal(charactersByYear)
    val charactersTotalArray = Array(
      ("beatrice_(umineko)", 1),
      ("featherine_augustus_aurora", 1),
      ("frederica_bernkastel", 1),
      ("furude_rika", 6),
      ("hanyuu", 10),
      ("houjou_satoko", 1),
      ("ikazuchi_(kancolle)", 1),
      ("inazuma_(kancolle)", 1),
      ("maebara_keiichi", 1),
      ("moriya_suwako", 1),
      ("ryuuguu_rena", 1),
      ("sonozaki_mion", 1),
    )

    assert(charactersTotal.collect().toSet.equals(charactersTotalArray.toSet))
  }

  test("countTag Test") {
    val tagsByDay = Analysis.countTag(multipleImageRDD)

    val tagsByMonth = UtilitiesAnalysis.groupDayCountByMonth(tagsByDay)

    val tagsByYear = UtilitiesAnalysis.groupMonthCountBYear(tagsByMonth)

    val tagsTotal = UtilitiesAnalysis.groupYearCountByTotal(tagsByYear)
    val tagsTotalArray =
      Array(
        ("smirk", 1),
        ("hexagon", 1),
        ("medium_breasts", 2),
        ("cowboy_shot", 1),
        ("shadow", 1),
        ("long_sleeves", 4),
        ("holding_hands", 2),
        ("outstretched_hand", 1),
        ("blush", 3),
        ("miko", 6),
        ("long_skirt", 1),
        ("sliding_doors", 1),
        ("very_long_hair", 2),
        ("bush", 1),
        ("night", 1),
        ("multiple_girls", 8),
        ("white_shirt", 1),
        ("short_sleeves", 1),
        ("tail", 1),
        ("ikazuchi_(kancolle)_(cosplay)", 1),
        ("serious", 1),
        ("nanodesu_(phrase)", 1),
        ("smile", 1),
        ("sleeveless_shirt", 1),
        ("standing", 2),
        ("horns", 8),
        ("crossover", 1),
        ("smug", 1),
        ("bare_arms", 1),
        ("empty_eyes", 1),
        ("3girls", 1),
        ("sun", 1),
        ("bobby_socks", 1),
        ("dancing", 1),
        ("sandals", 1),
        ("frilled_dress", 1),
        ("triangle_mouth", 1),
        ("shoes", 1),
        ("white_background", 1),
        ("broken_horn", 1),
        ("red_hakama", 6),
        ("pink_bow", 1),
        ("1girl", 2),
        ("profile", 2),
        ("purple_hair", 8),
        ("school_uniform", 2),
        ("black_legwear", 1),
        ("honeycomb_background", 1),
        ("2girls", 6),
        ("mary_janes", 1),
        ("sleeveless", 2),
        ("interlocked_fingers", 2),
        ("blunt_bangs", 1),
        ("tombstone", 1),
        ("short_hair", 1),
        ("purple_eyes", 6),
        ("moon", 1),
        ("closed_eyes", 1),
        ("cat_tail", 1),
        ("long_hair", 7),
        ("veranda", 1),
        ("ponytail", 1),
        ("dress_shirt", 1),
        ("green_hair", 1),
        ("frills", 2),
        ("wolf", 1),
        ("suspenders", 1),
        ("shirt", 2),
        ("bare_shoulders", 1),
        ("midair", 1),
        ("ribbon", 1),
        ("large_breasts", 1),
        ("honeycomb_(pattern)", 1),
        ("neck_ribbon", 1),
        ("hoe", 1),
        ("bow", 2),
        ("sleeves_rolled_up", 1),
        ("breasts", 2),
        ("japanese_clothes", 7),
        ("serafuku", 1),
        ("ceremony", 1),
        ("looking_at_viewer", 2),
        ("looking_afar", 1),
        ("detached_sleeves", 4),
        ("blue_skirt", 1),
        ("alternate_hairstyle", 1),
        ("5girls", 1),
        ("funeral_veil", 1),
        ("fire", 1),
        ("hakama_skirt", 6),
        ("sundress", 1),
        ("hanyuu_(cosplay)", 1),
        ("arms_under_breasts", 1),
        ("alternate_costume", 2),
        ("blue_eyes", 2),
        ("skirt", 8),
        ("cosplay", 2),
        ("inazuma_(kancolle)_(cosplay)", 1),
        ("pantyhose", 1),
        ("bangs", 1),
        ("pleated_skirt", 2),
        ("mouth_hold", 1),
        ("wide_sleeves", 1),
        ("1boy", 1),
        ("shrine", 1),
        ("floating_hair", 1),
        ("sideboob", 1),
        ("copyright_name", 1),
        ("pink_eyes", 1),
        ("hakama", 6),
        ("looking_back", 2),
        ("funeral_dress", 1),
        ("crossed_arms", 1),
        ("open_door", 1),
        ("seven-branched_sword", 1),
        ("chihaya_(clothing)", 1),
        ("green_skirt", 1),
        ("dress", 2),
        ("hat", 1),
        ("blue_hair", 3),
        ("solo", 2),
        ("light_smile", 1),
        ("socks", 1),
        ("holding", 1),
        ("blonde_hair", 2),
        ("kneepits", 1),
        ("crescent_moon", 1),
        ("sweatdrop", 1),
        ("back-to-back", 2)
      )

    assert(tagsTotal.collect().toSet.equals(tagsTotalArray.toSet))
  }

  //This test also covers the the values between each computation this will not be done for the getCharacterCombinations and getCharacterCopyrightCombinations tests
  test("getCopyrightCombinations Test") {
    val copyrightCombinationsByDay = Analysis.getCopyrightCombinations(multipleImageRDD)
    val copyrightCombinationsByDayArray = Array(
      ((("higurashi_no_naku_koro_ni", "kantai_collection"), (2013, 9, 9)), 1),
      ((("higurashi_no_naku_koro_ni", "touhou"), (2007, 10, 1)), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), (2008, 9, 30)), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), (2013, 7, 27)), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), (2021, 4, 6)), 1),
    )

    assert(copyrightCombinationsByDay.collect().toSet.equals(copyrightCombinationsByDayArray.toSet))

    val copyrightCombinationsByMonth = UtilitiesAnalysis.groupCombinationCountByMonth(copyrightCombinationsByDay)
    val copyrightCombinationsByMonthArray = Array(
      ((("higurashi_no_naku_koro_ni", "kantai_collection"), (2013, 9)), 1),
      ((("higurashi_no_naku_koro_ni", "touhou"), (2007, 10)), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), (2008, 9)), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), (2013, 7)), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), (2021, 4)), 1),
    )

    assert(copyrightCombinationsByMonth.collect().toSet.equals(copyrightCombinationsByMonthArray.toSet))

    val copyrightCombinationsByYear = UtilitiesAnalysis.groupCombinationCountByYear(copyrightCombinationsByMonth)
    val copyrightCombinationsByYearArray = Array(
      ((("higurashi_no_naku_koro_ni", "kantai_collection"), 2013), 1),
      ((("higurashi_no_naku_koro_ni", "touhou"), 2007), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), 2008), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), 2013), 1),
      ((("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), 2021), 1),
    )

    assert(copyrightCombinationsByYear.collect().toSet.equals(copyrightCombinationsByYearArray.toSet))

    val copyrightCombinationsTotal = UtilitiesAnalysis.groupCombinationCountByTotal(copyrightCombinationsByYear)
    val copyrightCombinationsTotalArray = Array(
      (("higurashi_no_naku_koro_ni", "kantai_collection"), 1),
      (("higurashi_no_naku_koro_ni", "touhou"), 1),
      (("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"), 3))

    assert(copyrightCombinationsTotal.collect().toSet.equals(copyrightCombinationsTotalArray.toSet))
  }

  test("getCharacterCombinations Test") {
    val characterCombinationsByDay = Analysis.getCharacterCombinations(multipleImageRDD)

    val characterCombinationsByMonth = UtilitiesAnalysis.groupCombinationCountByMonth(characterCombinationsByDay)

    val characterCombinationsByYear = UtilitiesAnalysis.groupCombinationCountByYear(characterCombinationsByMonth)

    val characterCombinationsTotal = UtilitiesAnalysis.groupCombinationCountByTotal(characterCombinationsByYear)
    val copyrightCombinationsTotalArray = Array(
      (("beatrice_(umineko)", "hanyuu"), 1),
      (("featherine_augustus_aurora", "hanyuu"), 1),
      (("frederica_bernkastel", "furude_rika"), 1),
      (("frederica_bernkastel", "hanyuu"), 1),
      (("furude_rika", "hanyuu"), 6),
      (("furude_rika", "houjou_satoko"), 1),
      (("furude_rika", "ikazuchi_(kancolle)"), 1),
      (("furude_rika", "inazuma_(kancolle)"), 1),
      (("furude_rika", "maebara_keiichi"), 1),
      (("furude_rika", "ryuuguu_rena"), 1),
      (("furude_rika", "sonozaki_mion"), 1),
      (("hanyuu", "houjou_satoko"), 1),
      (("hanyuu", "ikazuchi_(kancolle)"), 1),
      (("hanyuu", "inazuma_(kancolle)"), 1),
      (("hanyuu", "maebara_keiichi"), 1),
      (("hanyuu", "moriya_suwako"), 1),
      (("hanyuu", "ryuuguu_rena"), 1),
      (("hanyuu", "sonozaki_mion"), 1),
      (("houjou_satoko", "maebara_keiichi"), 1),
      (("houjou_satoko", "ryuuguu_rena"), 1),
      (("houjou_satoko", "sonozaki_mion"), 1),
      (("ikazuchi_(kancolle)", "inazuma_(kancolle)"), 1),
      (("maebara_keiichi", "ryuuguu_rena"), 1),
      (("maebara_keiichi", "sonozaki_mion"), 1),
      (("ryuuguu_rena", "sonozaki_mion"), 1),
    )

    assert(characterCombinationsTotal.collect().toSet.equals(copyrightCombinationsTotalArray.toSet))
  }

  test("getCharacterCopyrightCombinations Test") {
    val characterCopyrightCombinationsByDay = Analysis.getCharacterCopyrightCombinations(multipleImageRDD)

    val characterCopyrightCombinationsByMonth = UtilitiesAnalysis.groupCombinationCountByMonth(characterCopyrightCombinationsByDay)

    val characterCopyrightCombinationsByYear = UtilitiesAnalysis.groupCombinationCountByYear(characterCopyrightCombinationsByMonth)

    val characterCopyrightCombinationsTotal = UtilitiesAnalysis.groupCombinationCountByTotal(characterCopyrightCombinationsByYear)
    val characterCopyrightCombinationsTotalArray = Array(
      (("beatrice_(umineko)", "higurashi_no_naku_koro_ni"), 1),
      (("beatrice_(umineko)", "umineko_no_naku_koro_ni"), 1),
      (("featherine_augustus_aurora", "higurashi_no_naku_koro_ni"), 1),
      (("frederica_bernkastel", "higurashi_no_naku_koro_ni"), 1),
      (("featherine_augustus_aurora", "umineko_no_naku_koro_ni"), 1),
      (("furude_rika", "higurashi_no_naku_koro_ni"), 6),
      (("furude_rika", "kantai_collection"), 1),
      (("furude_rika", "umineko_no_naku_koro_ni"), 1),
      (("hanyuu", "higurashi_no_naku_koro_ni"), 10),
      (("hanyuu", "kantai_collection"), 1),
      (("hanyuu", "touhou"), 1),
      (("hanyuu", "umineko_no_naku_koro_ni"), 3),
      (("houjou_satoko", "higurashi_no_naku_koro_ni"), 1),
      (("frederica_bernkastel", "umineko_no_naku_koro_ni"), 1),
      (("ikazuchi_(kancolle)", "higurashi_no_naku_koro_ni"), 1),
      (("ikazuchi_(kancolle)", "kantai_collection"), 1),
      (("inazuma_(kancolle)", "kantai_collection"), 1),
      (("inazuma_(kancolle)", "higurashi_no_naku_koro_ni"), 1),
      (("maebara_keiichi", "higurashi_no_naku_koro_ni"), 1),
      (("moriya_suwako", "higurashi_no_naku_koro_ni"), 1),
      (("moriya_suwako", "touhou"), 1),
      (("ryuuguu_rena", "higurashi_no_naku_koro_ni"), 1),
      (("sonozaki_mion", "higurashi_no_naku_koro_ni"), 1),
    )

    assert(characterCopyrightCombinationsTotal.collect().toSet.equals(characterCopyrightCombinationsTotalArray.toSet))
  }

  test("getCharacterGeneralTagCombinations Test") {
    val characterGeneralTagCombinationsTotal = Analysis.getCharacterGeneralTagCombinations(multipleImageRDD)
    val characterGeneralTagCombinationsTotalArray = Array(
      (("hanyuu", "2girls"), 6),
      (("hanyuu", "hakama"), 6),
      (("hanyuu", "hakama_skirt"), 6),
      (("hanyuu", "horns"), 8),
      (("hanyuu", "japanese_clothes"), 7),
      (("hanyuu", "long_hair"), 7),
      (("hanyuu", "miko"), 6),
      (("hanyuu", "multiple_girls"), 8),
      (("hanyuu", "purple_hair"), 8),
      (("hanyuu", "purple_eyes"), 6),
      (("hanyuu", "red_hakama"), 6),
      (("hanyuu", "skirt"), 8),
      (("furude_rika", "horns"), 6),
      (("furude_rika", "long_hair"), 6),
      (("furude_rika", "multiple_girls"), 6),
      (("furude_rika", "purple_hair"), 6),
    )

    assert(characterGeneralTagCombinationsTotal.collect().toSet.equals(characterGeneralTagCombinationsTotalArray.toSet))
  }

  //In this test only values for one tag will be checked to prove the concept
  test("getAvgUpScore") {
    val avgUpScoreByDay = Analysis.getAvgUpScore(multipleImageRDD).filter(_._1._1 == "hanyuu")
    val avgUpScoreByDayArray = Array(
      (("hanyuu", (2006, 9, 29)), (2, 1)),
      (("hanyuu", (2007, 10, 1)), (10, 1)),
      (("hanyuu", (2008, 8, 18)), (3, 1)),
      (("hanyuu", (2008, 9, 30)), (2, 1)),
      (("hanyuu", (2008, 11, 24)), (4, 1)),
      (("hanyuu", (2010, 1, 6)), (5, 1)),
      (("hanyuu", (2013, 7, 27)), (11, 1)),
      (("hanyuu", (2013, 9, 9)), (3, 1)),
      (("hanyuu", (2015, 7, 9)), (2, 1)),
      (("hanyuu", (2021, 4, 6)), (13, 1)),
    )

    assert(avgUpScoreByDay.collect().toSet.equals(avgUpScoreByDayArray.toSet))

    val avgUpScoreByMonth = UtilitiesAnalysis.groupAvgValueByMonth(avgUpScoreByDay)
    val avgUpScoreByMonthArray = Array(
      (("hanyuu", (2006, 9)), (2, 1)),
      (("hanyuu", (2007, 10)), (10, 1)),
      (("hanyuu", (2008, 8)), (3, 1)),
      (("hanyuu", (2008, 9)), (2, 1)),
      (("hanyuu", (2008, 11)), (4, 1)),
      (("hanyuu", (2010, 1)), (5, 1)),
      (("hanyuu", (2013, 7)), (11, 1)),
      (("hanyuu", (2013, 9)), (3, 1)),
      (("hanyuu", (2015, 7)), (2, 1)),
      (("hanyuu", (2021, 4)), (13, 1)),
    )

    assert(avgUpScoreByMonth.collect().toSet.equals(avgUpScoreByMonthArray.toSet))

    val avgUpScoreByYear = UtilitiesAnalysis.groupAvgMonthValueByYear(avgUpScoreByMonth)
    val avgUpScoreByYearArray = Array(
      (("hanyuu", 2006), (2, 1)),
      (("hanyuu", 2008), (9, 3)),
      (("hanyuu", 2007), (10, 1)),
      (("hanyuu", 2010), (5, 1)),
      (("hanyuu", 2013), (14, 2)),
      (("hanyuu", 2015), (2, 1)),
      (("hanyuu", 2021), (13, 1)),
    )

    assert(avgUpScoreByYear.collect().toSet.equals(avgUpScoreByYearArray.toSet))

    val avgUpScoreTotal = UtilitiesAnalysis.groupAvgYearValueByTotal(avgUpScoreByYear)
    val avgUpScoreTotalArray = Array(("hanyuu", (55, 10)))

    assert(avgUpScoreTotal.collect().toSet.equals(avgUpScoreTotalArray.toSet))

  }
}
