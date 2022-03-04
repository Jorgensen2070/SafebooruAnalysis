import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.toSparkContextFunctions
import org.apache.spark.rdd.RDD

import org.bson.Document

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import com.mongodb.spark.toDocumentRDDFunctions
import org.apache.spark.SparkContext

import java.util
import scala.collection.JavaConverters._

object Utilities {
  val connectionString: String = sys.env("REAGENT_MONGO") + "danbooruData."

  val authString = "?authSource=admin"

  // the timestamps have the following form e.g. 2021-11-19T06:03:37.216-05:00
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss")

  /**
   * This method returns an implication map
   * The implication has the following form
   * Map [MainTag, List[ImplicatedTags]
   *
   * @param sparkContext   the current spark context
   * @param collectionName the name of the implication collection
   * @return the implicationMap
   */
  def getImplicationsMap(sparkContext: SparkContext, collectionName: String): scala.collection.Map[String, List[String]] = {
    val rdd = sparkContext.loadFromMongoDB(ReadConfig(Map("uri" -> (connectionString + collectionName + authString)))).rdd
    rdd.map(elem => {
      (
        elem.get("_id").asInstanceOf[String],
        elem.get("implications").asInstanceOf[util.ArrayList[String]].asScala.toList)
    }).collectAsMap()
  }

  /**
   * This method returns the CharacterCopyrightCombinationsRDD from the db
   * This can be used if it is wished to not use the one in  the db and not recalculate it
   *
   * @param sparkContext   the current spark context
   * @param collectionName the name of the CharacterCopyrightCombinations collection
   * @return RDD[((String, String), Int)] - of the form  RDD[((character, copyright), count)]
   */
  def getCharacterCopyrightCombinationsRDD(sparkContext: SparkContext, collectionName: String): RDD[((String, String), Int)] = {
    val rdd = sparkContext.loadFromMongoDB(ReadConfig(Map("uri" -> (connectionString + collectionName + authString)))).rdd
    rdd.map(elem => {
      (
        (elem.get("_id").asInstanceOf[Document].get("character").asInstanceOf[String], elem.get("_id").asInstanceOf[Document].get("copyright").asInstanceOf[String]),
        elem.get("val").asInstanceOf[Int])
    })
  }

  /**
   * This method returns the id of a document as a Integer
   *
   * @param doc document from which the id should be obtained
   * @return Long - the id of the given document
   */
  def getID(doc: Document): Int = {
    doc.get("_id").asInstanceOf[Int]
  }

  /**
   *
   * @param doc the document from which the timestamp should be obtained
   * @return (Int, Int, Int) - timestamp with the following format (year,month,day (of month))
   */
  def getTime(doc: Document): (Int, Int, Int) = {
    val timestamp = getTimestamp(doc)
    (timestamp.getYear, timestamp.getMonthValue, timestamp.getDayOfMonth)
  }

  /**
   * This method takes a string and parses it into a LocalDateTime
   *
   * @param date - string that should be parsed
   * @return the string as a LocalDateTime
   */
  def parseTime(date: String): LocalDateTime = {
    LocalDateTime.parse(date.splitAt(19)._1, dateFormatter)
  }

  /**
   * This method returns the created_at field as a LocalDateTime
   *
   * @param doc document from which the timestamp should be obtained
   * @return the timestamp as a LocalDateTime
   */
  def getTimestamp(doc: Document): LocalDateTime = {
    parseTime(doc.get("created_at").asInstanceOf[String])
  }

  /**
   * This method returns a list of all copyright tags in the given document as a list of strings
   *
   * @param doc document from which the copyrights should be obtained
   * @return List[String] - the list of copyright tags
   */
  def getCopyrights(doc: Document): List[String] = {
    doc.getOrDefault("tag_string_copyright", new java.util.ArrayList[String](List("no_copyright").asJava))
      .asInstanceOf[util.ArrayList[String]]
      .asScala
      .toList
      .map(x => {
      if (x.isBlank)
        "no_copyright"
      else
        x
    })
  }

  /**
   * This method returns a list of all character tags in the given document as a list of strings
   *
   * @param doc document from which the characters should be obtained
   * @return List[String] - the list of character tags
   */
  def getCharacters(doc: Document): List[String] = {
    doc.get("tag_string_character")
      .asInstanceOf[util.ArrayList[String]]
      .asScala
      .toList.map(x => {
      if (x.isBlank)
        "no_character"
      else
        x
    })
  }

  /**
   * This method returns a list of all general tags in the given document as a list of strings
   *
   * @param doc document from which the general tags should be obtained
   * @return List[String] - the list of general tags
   */
  def getGeneralTags(doc: Document): List[String] = {
    doc.getOrDefault("tag_string_general", new java.util.ArrayList[String](List("no_tag").asJava))
      .asInstanceOf[util.ArrayList[String]]
      .asScala
      .map(x => {
        //The replacement of a simple backslash to a double backslash is performed in order for the backslash to be written correctly into the database
        x.replace("\\", "\\\\")
      }).toList
  }

  /**
   * This method returns a list of all artists in the given document as a list of strings
   *
   * @param doc document from which the artists should be obtained
   * @return List[String] - the list of artists
   */
  def getArtist(doc: Document): List[String] = {

    doc.get("tag_string_artist")
      .asInstanceOf[util.ArrayList[String]]
      .asScala
      .toList
      .map(x => {
      if (x.isBlank)
        "no_artist"
      else
        x
    })
  }

  /**
   * This method returns a list of all tags in the given document as a list of strings
   * All tags include copyright, character, general tags, artists and the metatags
   *
   * @param doc document from which the tags should be obtained
   * @return List[String] - the list of all tags
   */
  def getAllTags(doc: Document): List[String] = {
    doc.getOrDefault("tag_string", new java.util.ArrayList[String](List("no_tag").asJava))
      .asInstanceOf[util.ArrayList[String]]
      .asScala
      .map(x => {
        //The replacement of a simple backslash to a double backslash is performed in order for the backslash to be written correctly into the database
        x.replace("\\", "\\\\")
      }).toList
  }

  /**
   * This method returns the sample file url for a given image
   * The sample file url contains a scaled down version of the original image on the content delivery network of danbooru
   *
   * @param doc document from which the url should be obtained
   * @return String - the url as a string
   */
  def getImageUrl(doc: Document): String = {
    doc.get("large_file_url").asInstanceOf[String]
  }

  /**
   * This method returns the upScore (Likes) for a given image
   * The source determines where the image was originally uploaded
   *
   * @param doc document from which the upScore should be obtained
   * @return Int - the upScore
   */
  def getUpScore(doc: Document): Int = {
    doc.get("up_score").asInstanceOf[Int]
  }

  /**
   * This method returns the a list of sources for a given image
   * The source determines where the image was originally uploaded
   *
   * @param doc document from which the sources should be obtained
   * @return List[String] - A list of sources
   */
  def getSource(doc: Document): String = {
    val source = doc.get("source").asInstanceOf[String].replaceAll(" ", "_").replaceAll("\"", "\'").replaceAll("\\\\", "/")

    try {
      //println(url.split("//")(1).split("/")(0))
      if (source.isBlank) {
        "no_URL"
      } else {
        val extracted = source.split("//")(1).split("/")(0)

        //Here entries from different URL related to pixiv are grouped together e.g.
        // i3.pixiv.net -> www.pixiv.net
        // i2.pixiv.net -> www.pixiv.net
        //etc
        if (extracted.contains("pximg") || extracted.contains("pixiv")) {
          "www.pixiv.net"
        } else {
          extracted
        }
      }

    } catch {
      case _: Throwable =>
        source
    }
  }


  /**
   * This method returns a list of all possible character combinations in an image
   * e.g. if the image contains a,b,c
   * List[(a,b),(a,c),(b,c)] will be returned
   * the elements are ordered so that the "smaller" string is in the front
   *
   * @param document the document from which the combinations should be obtained
   * @return the created combinations list
   */
  def getCharacterCombinationList(document: Document): List[(String, String)] = {
    document.get("tag_string_character").asInstanceOf[util.ArrayList[String]].asScala.splitAt(1)._2.flatMap(char1 =>
      document.get("tag_string_character").asInstanceOf[util.ArrayList[String]].asScala.map(char2 =>
        if (char2 < char1) {
          (char2, char1)
        } else {
          (char1, char2)
        }
      )).toList.filter(ent => ent._1 != ent._2).distinct
  }

  /**
   * This method returns a list of all possible copyright combinations in an image
   * e.g. if the image contains a,b,c
   * List[(a,b),(a,c),(b,c)] will be returned
   * the elements are ordered so that the "smaller" string is in the front
   *
   * @param document the document from which the combinations should be obtained
   * @return the created combinations list
   */
  def getCopyrightCombinationList(document: Document): List[(String, String)] = {
    document.get("tag_string_copyright").asInstanceOf[util.ArrayList[String]].asScala.splitAt(1)._2.flatMap(copyright1 =>
      document.get("tag_string_copyright").asInstanceOf[util.ArrayList[String]].asScala.map(copyright2 =>
        if (copyright2 < copyright1) {
          (copyright2, copyright1)
        } else {
          (copyright1, copyright2)
        }
      )).toList.filter(ent => ent._1 != ent._2).distinct
  }

  /**
   * This method returns the combination of all characters together with all copyrights in the given document
   * e.g if the image has character a and b and the copyright C and D
   * List[(a,C),(a,D),(b,C),(b,D)] will be returned
   *
   * @param document the document from which the combinations should be obtained
   * @return the created combinations list in the form (character,copyright)
   */
  def getCharacterCopyrightCombinationsList(document: Document): List[(String, String)] = {
    document.get("tag_string_character").asInstanceOf[util.ArrayList[String]].asScala
      .map(x => {
        if (x.isBlank)
          "no_character"
        else
          x
      }).flatMap(char =>
      document.get("tag_string_copyright").asInstanceOf[util.ArrayList[String]].asScala.map(copyright =>
        if (copyright.isBlank)
          (char, "no_copyright")
        else
          (char, copyright)
      )).toList
  }

  /**
   * This method returns the combination of all characters together with all generalTags in the given document
   * e.g if the image has character a and b and the tags blonde_hair, bangs and dress
   * List[(a,blonde_hair),(a,bangs),(a,dress),(b,blonde_hair),(b,bangs),(b,dress)] will be returned
   *
   * @param document the document from which the combinations should be obtained
   * @return the created combinations list in the form (character,generalTag)
   */
  def getCharacterTagCombinationsList(document: Document): List[(String, String)] = {
    document.get("tag_string_character").asInstanceOf[util.ArrayList[String]].asScala
      .map(x => {
        if (x.isBlank)
          "no_character"
        else
          x
      }).flatMap(char =>
      document.get("tag_string_general").asInstanceOf[util.ArrayList[String]].asScala.map(tag =>
        if (tag.isBlank)
          (char, "no_tag")
        else
          (char, tag.replace("\\", "\\\\"))
      )).toList
  }
}
