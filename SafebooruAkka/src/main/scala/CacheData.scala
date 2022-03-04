import org.apache.spark.rdd.RDD

object CacheData {

  /**
   * This method executes a count on the data causing all the data to be loaded into the cache
   *
   * @param characterByDay                         - RDD containing the counts for characters by day (last 31 days)
   * @param generlTagByMonth                       - RDD containing the counts for generalTags by month
   * @param generlTagByDay                         - RDD containing the counts for generalTags by day (last 31 days)
   * @param copyrightPairings                      - RDD containing the total count of combinations between copyrights
   * @param characterCombinationRDD                - RDD containing the total count of combinations between characters
   * @param characterCopyrightCombinationRDD       - RDD containing the total count of combinations between characters and copyrights
   * @param characterCopyrightCombinationsYearRDD  - RDD containing the count of combinations between characters and copyrights by year
   * @param characterCopyrightCombinationsMonthRDD - RDD containing the count of combinations between characters and copyrights by month
   * @param characterCopyrightCombinationsDayRDD   - RDD containing the count of combinations between characters and copyrights by day (last 31 days)
   * @param characterGeneralTagCombinationsRDD     - RDD containing the total count of combinations between characters and generalTags
   * @param avgUpScoreYear                         - RDD containing the averageScore per tag by year
   * @param avgUpScoreMonth                        - RDD containing the averageScore per tag by month
   */
  def cacheData(characterByDay: RDD[(String, ((Int, Int, Int), Int))],
                generlTagByMonth: RDD[(String, ((Int, Int), Int))],
                generlTagByDay: RDD[(String, ((Int, Int, Int), Int))],
                copyrightPairings: RDD[(String, String, Int)],
                characterCombinationRDD: RDD[(String, String, Int)],
                characterCopyrightCombinationRDD: RDD[((String, String), Int)],
                characterCopyrightCombinationsYearRDD: RDD[(((String, String), Int), Int)],
                characterCopyrightCombinationsMonthRDD: RDD[(((String, String), Int, Int), Int)],
                characterCopyrightCombinationsDayRDD: RDD[(((String, String), Int, Int, Int), Int)],
                characterGeneralTagCombinationsRDD: RDD[((String, String), Int)],
                avgUpScoreYear: RDD[(String, (Int, Double))],
                avgUpScoreMonth: RDD[(String, ((Int, Int), Double))]
               ) = {


    val countList = List(
      characterByDay.count(),
      generlTagByMonth.count(),
      generlTagByDay.count(),
      copyrightPairings.count(),
      characterCopyrightCombinationRDD.count(),
      characterCombinationRDD.count(),
      characterCopyrightCombinationsYearRDD.count(),
      characterCopyrightCombinationsMonthRDD.count(),
      characterCopyrightCombinationsDayRDD.count(),
      characterGeneralTagCombinationsRDD.count(),
      avgUpScoreYear.count(),
      avgUpScoreMonth.count()
    )

    println("All Data Cached" + countList.toString())


  }
}
