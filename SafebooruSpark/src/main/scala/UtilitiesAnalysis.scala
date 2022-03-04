import org.apache.spark.rdd.RDD

object UtilitiesAnalysis {


  /**
   * This method groups the given RDD that was grouped by day and groups it by month
   *
   * @param rdd RDD[((String, (Int, Int, Int)), Int)]
   *            - of the form [((attribute, (year, month, day (of month))), count)]
   * @return RDD[((String, (Int, Int)), Int)]
   *            - of the form [((attribute, (year, month)), count)]
   */
  def groupDayCountByMonth(rdd: RDD[((String, (Int, Int, Int)), Int)]): RDD[((String, (Int, Int)), Int)] = {
    rdd.map(entry => ((entry._1._1, (entry._1._2._1, entry._1._2._2)), entry._2)).reduceByKey(_ + _)
  }

  /**
   * This method takes the given RDD that was grouped by month and groups it by year
   *
   * @param rdd RDD[((String, (Int, Int)), Int)]
   *            - of the form [((attribute, (year, month)), count)]
   * @return RDD[((String, Int), Int)]
   *            - of the form [((attribute, year), count)]
   */
  def groupMonthCountBYear(rdd: RDD[((String, (Int, Int)), Int)]): RDD[((String, Int), Int)] = {
    rdd.map(entry => ((entry._1._1, entry._1._2._1), entry._2)).reduceByKey(_ + _)
  }

  /**
   * This method takes the given RDD that was grouped by year and groups it by the attribute
   *
   * @param rdd RDD[((String, Int), Int)]
   *            - of the form [((attribute, year), count)]
   * @return RDD[(String, Int)]
   *            - of the form [(attribute, count)]
   */
  def groupYearCountByTotal(rdd: RDD[((String, Int), Int)]): RDD[(String, Int)] = {
    rdd.map(entry => (entry._1._1, entry._2)).reduceByKey(_ + _)
  }

  /**
   * This method groups the given combination RDD by month
   *
   * @param rdd RDD[(((String, String), (Int, Int, Int)), Int)]
   *            - of the form  [(((source, target), (year, month, day (of month))), count)]
   * @return RDD[(((String, String), (Int, Int)), Int)]
   *            - of the form  [(((source, target), (year, month)), count)]
   */
  def groupCombinationCountByMonth(rdd: RDD[(((String, String), (Int, Int, Int)), Int)]): RDD[(((String, String), (Int, Int)), Int)] = {
    rdd.map(entry => ((entry._1._1, (entry._1._2._1, entry._1._2._2)), entry._2)).reduceByKey(_ + _)
  }

  /**
   * This method takes the given combination RDD that was grouped by month and groups it by year
   *
   * @param rdd RDD[(((String, String), (Int, Int)), Int)]
   *            - of the form  [(((source, target), (year, Month)), count)]
   * @return RDD[(((String, String), Int), Int)]
   *            - of the form  [(((source, target), year), count)]
   */
  def groupCombinationCountByYear(rdd: RDD[(((String, String), (Int, Int)), Int)]): RDD[(((String, String), Int), Int)] = {
    rdd.map(entry => ((entry._1._1, entry._1._2._1), entry._2)).reduceByKey(_ + _)
  }

  /**
   * This method takes the given combination RDD that was grouped by year and groups it by the combination of source and target
   *
   * @param rdd RDD[(((String, String), Int), Int)]
   *            - of the form  [(((source, target), year), count)]
   * @return RDD[((String, String), Int)]
   *            - of the form [((source, target), count)]
   */
  def groupCombinationCountByTotal(rdd: RDD[(((String, String), Int), Int)]): RDD[((String, String), Int)] = {
    rdd.map(entry => (entry._1._1, entry._2)).reduceByKey(_ + _)
  }

  /**
   * This method groups the given average RDD by month
   *
   * @param rdd RDD[((String, (Int, Int, Int)), (Int, Int))]
   *            - of the form [((attribute, (year, month, day (of month))), (summedUpScore, summedUpCount))]
   * @return RDD[((String, (Int, Int)), (Int, Int))]
   *         - of the form [((attribute, (year, month)), (summedUpScore, summedUpCount))]
   */
  def groupAvgValueByMonth(rdd: RDD[((String, (Int, Int, Int)), (Int, Int))]): RDD[((String, (Int, Int)), (Int, Int))] = {
    rdd.map(entry => ((entry._1._1, (entry._1._2._1, entry._1._2._2)), entry._2)).reduceByKey((A, B) => (A._1 + B._1, A._2 + B._2))
  }

  /**
   * This method groups the given average RDD that was grouped by month and groups it by year
   *
   * @param rdd RDD[((String, (Int, Int)), (Int, Int))]
   *         - of the form [((attribute, (year, month)), (summedUpScore, summedUpCount))]
   * @return RDD[((String, Int), (Int, Int))]
   *         - of the form [((attribute, year), (summedUpScore, summedUpCount))]
   */
  def groupAvgMonthValueByYear(rdd: RDD[((String, (Int, Int)), (Int, Int))]): RDD[((String, Int), (Int, Int))] = {
    rdd.map(entry => ((entry._1._1, entry._1._2._1), entry._2)).reduceByKey((A, B) => (A._1 + B._1, A._2 + B._2))
  }

  /**
   * This method takes the given average RDD that was grouped by year and groups it by the attribute
   *
   * @param rdd RDD[((String, Int), (Int, Int))]
   *         - of the form [((attribute, year), (summedUpScore, summedUpCount))]
   * @return RDD[(String, (Int, Int))]
   *         - of the form [(attribute, (summedUpScore, summedUpCount))]
   */
  def groupAvgYearValueByTotal(rdd: RDD[((String, Int), (Int, Int))]): RDD[(String, (Int, Int))] = {
    rdd.map(entry => (entry._1._1, entry._2)).reduceByKey((A, B) => (A._1 + B._1, A._2 + B._2))
  }

}
