import Utilities.{getAllTags, getCharacterCombinationList, getCharacterCopyrightCombinationsList, getCharacterTagCombinationsList, getCharacters, getCopyrightCombinationList, getCopyrights, getGeneralTags, getID, getImageUrl, getSource, getTime, getUpScore}
import org.apache.spark.rdd.RDD
import org.bson.Document

object Analysis {

  /**
   * This method takes an RDD of Documents and generates an RDD of all tags together with the 5 latest images uploaded for the tag with their respective id and url
   *
   * @param rdd RDD[Document]
   *            - rdd from which the latest images should be obtained
   * @return RDD[(String, List[(Int, String)])]
   *         - of the form [(tag, List[(imageID, url)])]
   */
  def getLatestImages(rdd: RDD[Document]): RDD[(String, List[(Int, String)])] = {

    val procLatestImages = rdd.flatMap(entry => getAllTags(entry).map(copyright => (copyright, (getID(entry), getImageUrl(entry)))))
      .groupByKey()
      .mapValues(_.toList.sortBy(-_._1).take(5))

    procLatestImages
  }

  /**
   * This method takes an RDD of Documents and generates an RDD of all tags together with the source url and a count of occurrences
   *
   * @param rdd RDD[Document]
   *            - rdd from which the pair RDD of tag and sources should be obtained
   * @return RDD[((String, String), Int)]
   *          - of the form  [((tag, source), count)]
   */
  def getSoucesAll(rdd: RDD[Document]): RDD[((String, String), Int)] = {
    val procSources = rdd.flatMap(entry => getAllTags(entry).map(tag => ((tag, getSource(entry)), 1)))
      .reduceByKey(_ + _)

    procSources
  }

  /**
   * This method takes an RDD of Documents and generates an Pair RDD from it
   * The combination of all tags together with their timestamps is the key
   * Two values needed (summedUpScore, summedUpCount) to calculate the average Score of images are the value
   *
   * @param rdd RDD[Document]
   *            - rdd from which the pair RDD should be generated
   * @return RDD[((String, (Int, Int, Int)), (Int, Int))]
   *         - of the form [((tag, (year, month, day (of month))), (summedUpScore, summedUpCount))]
   */
  def getAvgUpScore(rdd: RDD[Document]): RDD[((String, (Int, Int, Int)), (Int, Int))] = {
    val procAvgUpScore = rdd.flatMap(entry => getAllTags(entry).map(tag => ((tag, getTime(entry)), (getUpScore(entry), 1))))
      .reduceByKey((A, B) => (A._1 + B._1, A._2 + B._2))

    procAvgUpScore
  }

  /**
   * This method takes an RDD of Documents and generates an Pair RDD from it
   * The combination of a  copyrights and the timestamp is the key
   * The count of occurrences is the value
   *
   * @param rdd RDD[Document]
   *             - rdd from which the pair RDD should be generated
   * @return RDD[((String, (Int, Int, Int)), Int)]
   *         - of the form [((copyright, (year, mount, day (of month))), count)]
   */
  def countCopyright(rdd: RDD[Document]): RDD[((String, (Int, Int, Int)), Int)] = {
    val procCopyright = rdd.flatMap(entry => getCopyrights(entry).map(copyright => ((copyright, getTime(entry)), 1)))
      .reduceByKey(_ + _)

    procCopyright
  }

  /**
   * This method takes an RDD of Documents and generates a Pair RDD from it
   * The combination of character and the timestamp is the key
   * The count of occurrences is the value
   *
   * @param rdd RDD[Document]
   *             - rdd from which the pair RDD should be generated
   * @return RDD[((String, (Int, Int, Int)), Int)]
   *         - of the form [((character, (year, mount, day (of month))), count)]
   */
  def countCharacter(rdd: RDD[Document]): RDD[((String, (Int, Int, Int)), Int)] = {
    val procCharacter = rdd.flatMap(entry => getCharacters(entry).map(character => ((character, getTime(entry)), 1)))
      .reduceByKey(_ + _)

    procCharacter
  }

  /**
   * This method takes an RDD of Documents and generates a Pair RDD from it
   * The combination of generalTag and the timestamp is the key
   * The count of occurrences is the value
   *
   * @param rdd RDD[Document]
   *             - rdd from which the rdd should be generated
   * @return RDD[((String, (Int, Int, Int)), Int)]
   *         - of the form [((generalTag, (year, mount, day (of month))), count)]
   */
  def countTag(rdd: RDD[Document]): RDD[((String, (Int, Int, Int)), Int)] = {
    val procTag = rdd.flatMap(entry => getGeneralTags(entry).map(tag => ((tag, getTime(entry)), 1)))
      .reduceByKey(_ + _)

    procTag
  }

  /**
   * This method takes an RDD of Documents and generates a Pair RDD from it
   * The combination of present characters with each other together with their timestamp is the key
   * The count of occurrences is the value
   *
   * @param rdd RDD[Document]
   *             - rdd from which the rdd should be generated
   * @return RDD[(((String, String), (Int, Int, Int)), Int)]
   *         - of the form [(((character1,character2), (year, mount, day (of month))), count)]
   */
  def getCharacterCombinations(rdd: RDD[Document]): RDD[(((String, String), (Int, Int, Int)), Int)] = {
    val characterCombinations = rdd.filter(getCharacters(_).size > 1)
      .flatMap(image => getCharacterCombinationList(image).map(pair => ((pair, getTime(image)), 1)))
      .reduceByKey(_ + _)

    characterCombinations
  }

  /**
   * This method takes an RDD of Documents and generates a Pair RDD from it
   * The combination of present copyrights with each other together with their timestamp is the key
   * The count of occurrences is the value
   *
   * @param rdd RDD[Document]
   *             - rdd from which the rdd should be generated
   * @return RDD[(((String, String), (Int, Int, Int)), Int)]
   *         - of the form RDD[(((copyright1, copyright2), (year, mount, day (of month))), count)]
   */
  def getCopyrightCombinations(rdd: RDD[Document]): RDD[(((String, String), (Int, Int, Int)), Int)] = {
    val copyrightCombinations = rdd.filter(getCopyrights(_).size > 1)
      .flatMap(image => getCopyrightCombinationList(image).map(pair => ((pair, getTime(image)), 1)))
      .reduceByKey(_ + _)

    copyrightCombinations
  }

  /**
   * This method takes an RDD of Documents and generates a Pair RDD from it
   * The combination of present copyrights and present copyrights together with their timestamp is the key
   * The count of occurrences is the value
   *
   * @param rdd RDD[Document]
   *             - rdd from which the rdd should be generated
   * @return RDD[(((String, String), (Int, Int, Int)), Int)]
   *         - of the form RDD[(((character, copyright),(year, mount, day (of month))), count)]
   */
  def getCharacterCopyrightCombinations(rdd: RDD[Document]): RDD[(((String, String), (Int, Int, Int)), Int)] = {
    val characterCopyrightCombinations = rdd.filter(ent => getCharacters(ent).nonEmpty && getCopyrights(ent).nonEmpty)
      .flatMap(image => getCharacterCopyrightCombinationsList(image).map(pair => ((pair, getTime(image)), 1)))
      .reduceByKey(_ + _)

    characterCopyrightCombinations
  }

  /**
   * This method takes an RDD of Documents and generates a Pair RDD from it
   * The combination of present character and present generalTag together with their timestamp is the key
   * The count of occurrences is the value
   *
   * @param rdd RDD[Document]
   *             - rdd from which the rdd should be generated
   * @return RDD[(((String, String), (Int, Int, Int)), Int)]
   *         - of the form RDD[(((character, generalTag),(year, mount, day (of month))), count)]
   */
  def getCharacterGeneralTagCombinations(rdd: RDD[Document]): RDD[((String, String), Int)] = {
    val procTotal = rdd.filter(ent => getCharacters(ent).nonEmpty && getGeneralTags(ent).nonEmpty)
      .flatMap(image => getCharacterTagCombinationsList(image).map(pair => (pair, 1)))
      .reduceByKey(_ + _).filter(_._2 > 5)

    procTotal
  }

  /**
   * This method generates an RDD of combinations between characters and the copyright that they were tagged together with the most (so called mainCopyright)
   * In the case that a copyright that originates from another copyright is the most frequent occurrence,
   * the copyright from which the given copyright originates will be returned as the mainCopyright
   *
   * @param characterTotalRDD              - the RDD that contains all characters together with a count of their total occurrences
   * @param characterCopyrightCombinations - RDD[((String, String), Int)],
   *                                       - RDD that contains the combination between characters and copyrights and the count of their total occurrences
   * @param implicationToCopyrightMap      - scala.collection.Map[String, String]
   *                                       - A map that contains copyrights that originate from another copyright
   *                                         (has the form (copyright, originalCopyright)
   * @return RDD[(String, String)]
   *         - of the form RDD[(character, mainCopyright)]
   */
  def getCharacterMainCopyrightCombinations(characterTotalRDD: RDD[Document],
                                            characterCopyrightCombinations: RDD[((String, String), Int)],
                                            implicationToCopyrightMap: scala.collection.Map[String, String],
                                           ): RDD[(String, String)] = {
    //This rdd needs to be collected
    //If it is not collected an error is thrown because you cant perform an action on an rdd while in the map of another rdd
    val temp = characterCopyrightCombinations.map(x => (x._1._1, (x._1._2, x._2))).groupByKey().mapValues(_.maxBy(_._2)).collectAsMap()

    val proc = characterTotalRDD.map(elem => {
      val char = elem.get("_id").asInstanceOf[Document].get("character").toString
      if (char.isBlank) {
        ("no_character", "no_copyright")
      } else {
        val topEntry = temp(char)
        (char, implicationToCopyrightMap.getOrElse(topEntry._1, topEntry._1))
      }
    }
    )

    proc
  }
}
