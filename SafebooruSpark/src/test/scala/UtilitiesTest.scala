
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

class UtilitiesTest extends AnyFunSuite with BeforeAndAfterAll {
  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _
  var singleImageRDD: RDD[Document] = _

  val singleImageDataName = "singleImage.json"

  override protected def beforeAll() {

    val conf = new SparkConf().
      setAppName("MongoSparkConnector").
      setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    sc = spark.sparkContext

    val singleImage = scala.io.Source.fromFile("src/test/resources/" + singleImageDataName).mkString.replace("[{", "").replace("}]", "")
      .split("},\\{").map(elem => "{" + elem + "}")

    singleImageRDD = sc.parallelize(singleImage).map(elem => Document.parse(elem))

  }

  test("Initialization Test") {
    assert(singleImageRDD.count() === 1)
  }

  test("getID test") {
    assert(Utilities.getID(singleImageRDD.first()) === 1472187)
  }

  test("getTime test") {
    assert(Utilities.getTime(singleImageRDD.first()) === (2013, 7, 27))
  }

  test("getCopyrights test") {
    assert(Utilities.getCopyrights(singleImageRDD.first()) === List("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"))
  }

  test("getCharacters test") {
    assert(Utilities.getCharacters(singleImageRDD.first()) === List("frederica_bernkastel", "furude_rika", "hanyuu"))
  }

  test("getGeneralTags test") {
    val generalTags = List("3girls",
      "blue_hair",
      "bobby_socks",
      "bow",
      "bush",
      "cat_tail",
      "detached_sleeves",
      "dress",
      "empty_eyes",
      "frills",
      "green_skirt",
      "hakama",
      "hakama_skirt",
      "horns",
      "interlocked_fingers",
      "japanese_clothes",
      "kneepits",
      "light_smile",
      "long_hair",
      "long_sleeves",
      "looking_at_viewer",
      "looking_back",
      "mary_janes",
      "midair",
      "miko",
      "multiple_girls",
      "open_door",
      "purple_eyes",
      "purple_hair",
      "red_hakama",
      "sandals",
      "shadow",
      "shoes",
      "skirt",
      "sleeveless",
      "sliding_doors",
      "socks",
      "standing",
      "sundress",
      "tail",
      "veranda")


    assert(Utilities.getGeneralTags(singleImageRDD.first()) === generalTags)
  }

  test("getAllTags test") {
    val allTags = List("3girls",
      "blue_hair",
      "bobby_socks",
      "bow",
      "bush",
      "cat_tail",
      "commentary_request",
      "detached_sleeves",
      "domonekoteki",
      "dress",
      "empty_eyes",
      "frederica_bernkastel",
      "frills",
      "furude_rika",
      "green_skirt",
      "hakama",
      "hakama_skirt",
      "hanyuu",
      "higurashi_no_naku_koro_ni",
      "horns",
      "interlocked_fingers",
      "japanese_clothes",
      "kneepits",
      "light_smile",
      "long_hair",
      "long_sleeves",
      "looking_at_viewer",
      "looking_back",
      "mary_janes",
      "midair",
      "miko",
      "multiple_girls",
      "open_door",
      "photoshop_(medium)",
      "purple_eyes",
      "purple_hair",
      "red_hakama",
      "sandals",
      "shadow",
      "shoes",
      "skirt",
      "sleeveless",
      "sliding_doors",
      "socks",
      "standing",
      "sundress",
      "tail",
      "umineko_no_naku_koro_ni",
      "veranda")

    assert(Utilities.getAllTags(singleImageRDD.first()) === allTags)
  }

  test("getImageUrl test") {
    val url = "https://cdn.donmai.us/original/fd/9a/fd9aa8eac7bbe497ff3680529cc61b83.jpg"
    assert(Utilities.getImageUrl(singleImageRDD.first()) === url)
  }

  test("getUpScore test") {
    assert(Utilities.getUpScore(singleImageRDD.first()) === 11)
  }

  test("getCharacterCombinationList test") {
    val combinations = List(("frederica_bernkastel", "furude_rika"), ("furude_rika", "hanyuu"), ("frederica_bernkastel", "hanyuu"))
    println(combinations)
  }

  test("getCopyrightCombinationList test") {
    val combinations = List(("higurashi_no_naku_koro_ni", "umineko_no_naku_koro_ni"))
    assert(Utilities.getCopyrightCombinationList(singleImageRDD.first()) === combinations)
  }

  test("getCharacterCopyrightCombinationsList test") {
    val combinations = List(("frederica_bernkastel", "higurashi_no_naku_koro_ni"),
      ("frederica_bernkastel", "umineko_no_naku_koro_ni"),
      ("furude_rika", "higurashi_no_naku_koro_ni"),
      ("furude_rika", "umineko_no_naku_koro_ni"),
      ("hanyuu", "higurashi_no_naku_koro_ni"),
      ("hanyuu", "umineko_no_naku_koro_ni"))

    assert(Utilities.getCharacterCopyrightCombinationsList(singleImageRDD.first()) === combinations)
  }

  test("getCharacterTagCombinationsList test") {
    val combinations = List(
      ("frederica_bernkastel", "3girls"),
      ("frederica_bernkastel", "blue_hair"),
      ("frederica_bernkastel", "bobby_socks"),
      ("frederica_bernkastel", "bow"),
      ("frederica_bernkastel", "bush"),
      ("frederica_bernkastel", "cat_tail"),
      ("frederica_bernkastel", "detached_sleeves"),
      ("frederica_bernkastel", "dress"),
      ("frederica_bernkastel", "empty_eyes"),
      ("frederica_bernkastel", "frills"),
      ("frederica_bernkastel", "green_skirt"),
      ("frederica_bernkastel", "hakama"),
      ("frederica_bernkastel", "hakama_skirt"),
      ("frederica_bernkastel", "horns"),
      ("frederica_bernkastel", "interlocked_fingers"),
      ("frederica_bernkastel", "japanese_clothes"),
      ("frederica_bernkastel", "kneepits"),
      ("frederica_bernkastel", "light_smile"),
      ("frederica_bernkastel", "long_hair"),
      ("frederica_bernkastel", "long_sleeves"),
      ("frederica_bernkastel", "looking_at_viewer"),
      ("frederica_bernkastel", "looking_back"),
      ("frederica_bernkastel", "mary_janes"),
      ("frederica_bernkastel", "midair"),
      ("frederica_bernkastel", "miko"),
      ("frederica_bernkastel", "multiple_girls"),
      ("frederica_bernkastel", "open_door"),
      ("frederica_bernkastel", "purple_eyes"),
      ("frederica_bernkastel", "purple_hair"),
      ("frederica_bernkastel", "red_hakama"),
      ("frederica_bernkastel", "sandals"),
      ("frederica_bernkastel", "shadow"),
      ("frederica_bernkastel", "shoes"),
      ("frederica_bernkastel", "skirt"),
      ("frederica_bernkastel", "sleeveless"),
      ("frederica_bernkastel", "sliding_doors"),
      ("frederica_bernkastel", "socks"),
      ("frederica_bernkastel", "standing"),
      ("frederica_bernkastel", "sundress"),
      ("frederica_bernkastel", "tail"),
      ("frederica_bernkastel", "veranda"),
      ("furude_rika", "3girls"),
      ("furude_rika", "blue_hair"),
      ("furude_rika", "bobby_socks"),
      ("furude_rika", "bow"),
      ("furude_rika", "bush"),
      ("furude_rika", "cat_tail"),
      ("furude_rika", "detached_sleeves"),
      ("furude_rika", "dress"),
      ("furude_rika", "empty_eyes"),
      ("furude_rika", "frills"),
      ("furude_rika", "green_skirt"),
      ("furude_rika", "hakama"),
      ("furude_rika", "hakama_skirt"),
      ("furude_rika", "horns"),
      ("furude_rika", "interlocked_fingers"),
      ("furude_rika", "japanese_clothes"),
      ("furude_rika", "kneepits"),
      ("furude_rika", "light_smile"),
      ("furude_rika", "long_hair"),
      ("furude_rika", "long_sleeves"),
      ("furude_rika", "looking_at_viewer"),
      ("furude_rika", "looking_back"),
      ("furude_rika", "mary_janes"),
      ("furude_rika", "midair"),
      ("furude_rika", "miko"),
      ("furude_rika", "multiple_girls"),
      ("furude_rika", "open_door"),
      ("furude_rika", "purple_eyes"),
      ("furude_rika", "purple_hair"),
      ("furude_rika", "red_hakama"),
      ("furude_rika", "sandals"),
      ("furude_rika", "shadow"),
      ("furude_rika", "shoes"),
      ("furude_rika", "skirt"),
      ("furude_rika", "sleeveless"),
      ("furude_rika", "sliding_doors"),
      ("furude_rika", "socks"),
      ("furude_rika", "standing"),
      ("furude_rika", "sundress"),
      ("furude_rika", "tail"),
      ("furude_rika", "veranda"), ("hanyuu", "3girls"),
      ("hanyuu", "blue_hair"),
      ("hanyuu", "bobby_socks"),
      ("hanyuu", "bow"),
      ("hanyuu", "bush"),
      ("hanyuu", "cat_tail"),
      ("hanyuu", "detached_sleeves"),
      ("hanyuu", "dress"),
      ("hanyuu", "empty_eyes"),
      ("hanyuu", "frills"),
      ("hanyuu", "green_skirt"),
      ("hanyuu", "hakama"),
      ("hanyuu", "hakama_skirt"),
      ("hanyuu", "horns"),
      ("hanyuu", "interlocked_fingers"),
      ("hanyuu", "japanese_clothes"),
      ("hanyuu", "kneepits"),
      ("hanyuu", "light_smile"),
      ("hanyuu", "long_hair"),
      ("hanyuu", "long_sleeves"),
      ("hanyuu", "looking_at_viewer"),
      ("hanyuu", "looking_back"),
      ("hanyuu", "mary_janes"),
      ("hanyuu", "midair"),
      ("hanyuu", "miko"),
      ("hanyuu", "multiple_girls"),
      ("hanyuu", "open_door"),
      ("hanyuu", "purple_eyes"),
      ("hanyuu", "purple_hair"),
      ("hanyuu", "red_hakama"),
      ("hanyuu", "sandals"),
      ("hanyuu", "shadow"),
      ("hanyuu", "shoes"),
      ("hanyuu", "skirt"),
      ("hanyuu", "sleeveless"),
      ("hanyuu", "sliding_doors"),
      ("hanyuu", "socks"),
      ("hanyuu", "standing"),
      ("hanyuu", "sundress"),
      ("hanyuu", "tail"),
      ("hanyuu", "veranda"))

    assert(Utilities.getCharacterTagCombinationsList(singleImageRDD.first()) === combinations)
  }

}
