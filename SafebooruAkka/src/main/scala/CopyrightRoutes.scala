import AkkaMain.{copyList, corsHandler}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, pathPrefix}
import akka.http.scaladsl.server.{RequestContext, RouteResult}
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scala.collection.JavaConverters._
import java.time.LocalDate
import scala.concurrent.Future
import akka.http.scaladsl.server.Directives._
import scala.language.postfixOps


object CopyrightRoutes {
  def getCopyrightRoutes(pathname: String,
                         copyrightImplications: Array[String],
                         implicationToCopyrightMap: Map[String, String],
                         implicationsRDD: RDD[(String, List[String])],
                         yearRDD: RDD[Document],
                         monthRDD: RDD[Document],
                         dayRDD: RDD[Document],
                         totalRDD: RDD[(String, Int)],
                         copyrightPairings: RDD[(String, String, Int)],
                         characterTotalRDD: RDD[(String, Int)],
                         mainCopyrightToCharacterRDD: RDD[(String, List[String])],
                         characterToMainCopyright: Map[String, String],
                         characterCopyrightCombinations: RDD[((String, String), Int)],
                         characterCopyrightCombinationsYearRDD: RDD[(((String, String), Int), Int)],
                         characterCopyrightCombinationsMonthRDD: RDD[(((String, String), Int, Int), Int)],
                         characterCopyrightCombinationsDayRDD: RDD[(((String, String), Int, Int, Int), Int)],
                         characterCombinationRDD: RDD[(String, String, Int)],
                        ): RequestContext => Future[RouteResult] = {
    pathPrefix(pathname) {
      concat(

        // This path returns a list of all copyrights. This list can be used for routing purposes to check if the desired copyright rly exists
        path("list") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = totalRDD.sortBy(-_._2).map(_._1)
              Json(DefaultFormats).write(temp.collect())
            })))
          }
        },

        // This path returns data for a piechart regarding total the total count of images for copyrights
        // Two types of entries are generated. If a copyright has more than 30.000 entries a normal entry is generated
        // If that is not the case it is grouped together with other entries below that threshold to not clutter the pie chart
        // Implication copyrights are not included in the data
        path("total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val normalEntries = totalRDD.filter(ent => !copyrightImplications.contains(ent._1)).filter(_._2 > 30000).sortBy(-_._2).collect()

              val accumulatedEntries = totalRDD
                .filter(entry => !copyrightImplications.contains(entry._1))
                .filter(x => !(x._2 > 30000)).fold(("Copyrights below 30.000 entries", 0))((A, B) => (A._1, A._2 + B._2))

              Json(DefaultFormats).write(normalEntries :+ accumulatedEntries)
            })))
          }
        },

        //This path returns the 5 key values of a boxplot regarding the total image counts per copyright
        // The 5 values are in the following order [Min, Q1, Median, Q3, Max]
        path("totalBoxplot") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val entries = totalRDD.values

              val numbers = entries.sortBy(identity).zipWithIndex().map(_.swap)
              val count = entries.count()

              val minVal = numbers.first()._2

              val quart1 = if (count % 2 == 0) {
                val left = count / 4 - 1
                val right = left + 1
                (numbers.lookup(left).head + numbers.lookup(right).head).toDouble / 2
              } else numbers.lookup(count / 4).head.toDouble

              val median: Double = if (count % 2 == 0) {
                val left = count / 2 - 1
                val right = left + 1
                (numbers.lookup(left).head + numbers.lookup(right).head).toDouble / 2
              } else numbers.lookup(count / 2).head.toDouble

              val quart3 = if (count % 2 == 0) {
                val left = (count / 4) * 3 - 1
                val right = left + 1
                (numbers.lookup(left).head + numbers.lookup(right).head).toDouble / 2
              } else numbers.lookup((count / 4) * 3).head.toDouble

              val maxVal = numbers.lookup(count - 1).head.toDouble


              Json(DefaultFormats).write(Array(minVal, quart1, median, quart3, maxVal))
            })))
          }
        },

        // This path returns data regarding the imagecount per year for the 9 most popular copyrights on safebooru
        // If a copyright had 0 images/is not present in a given year after the first image of the copyright was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => copyList.contains(elem.get("_id").asInstanceOf[Document].get("copyright").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
                  (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap)

              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min to max) yield {
                  (i, elem.getOrElse(i, 0))
                }
              }.toList.sortBy(_._1))

              val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
              val absolute = temp2.flatMap(_._2.map(_._1)).max

              Json(DefaultFormats).write((absoluteMin, absolute, temp2.collect()))
            })))
          }

        },

        //This path returns data regarding the image count per month  for the 9 most popular copyrights on safebooru
        // If a copyright had 0 images/is not present in a given month after the first image of the copyright was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => copyList.contains(elem.get("_id").asInstanceOf[Document].get("copyright").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
                  ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap)

              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min._1 to max._1) yield {
                  for (j <- 1 to 12) yield {
                    (i, j) -> elem.getOrElse((i, j), 0)
                  }
                }
              }.flatten.toList)

              val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
              val absoulteMax = temp2.flatMap(_._2.map(_._1)).max

              val minString = absoluteMin._1 + "-" + absoluteMin._2
              val maxString = absoulteMax._1 + "-" + absoulteMax._2

              val tempFin = temp2.mapValues(_.map(x => (x._1._1 + "-" + x._1._2, x._2)))

              Json(DefaultFormats).write((minString, maxString, tempFin.collect()))
            })))
          }
        },

        //This path returns data regarding the image count per day for the last 31 days for the 9 most popular copyrights on safebooru
        // If a copyright had 0 images/is not present on a given day a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("day") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val endDate = LocalDate.now()
              val beginDate = LocalDate.now().minusDays(31)

              val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList

              val temp = dayRDD.filter(elem => copyList.contains(elem.get("_id").asInstanceOf[Document].get("copyright").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
                  ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int],
                    elem.get("_id").asInstanceOf[Document].get("day").asInstanceOf[Int]),
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap.filter(entry => dateList.contains(entry._1)))

              val temp2 = temp.mapValues(elem => {
                for (i <- dateList) yield {
                  i -> elem.getOrElse(i, 0)
                }
              })

              val minString = beginDate.getYear + "-" + beginDate.getMonthValue + "-" + beginDate.getDayOfMonth
              val maxString = endDate.getYear + "-" + endDate.getMonthValue + "-" + endDate.getDayOfMonth

              val tempFin = temp2.mapValues(_.map(x => (x._1._1 + "-" + x._1._2 + "-" + x._1._3, x._2)))

              Json(DefaultFormats).write((minString, maxString, tempFin.collect()))
            })))
          }
        },

        //This path returns data regarding the connections between the 100 most popular copyrights on safebooru
        // The first field contains information regarding the copyrights (nodes) with name, count of total images and the main copyright
        // The second field contains information regarding the connections with the 2 Nodes and the count for the connections between them
        // To improve computing times for the forcegraph at least 10 entries must be present to count as a connection
        // Here implication copyrights are included to show clusters of related copyrigths
        path("copyrightPairing") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val top100 = totalRDD.takeOrdered(100)(Ordering[Int].reverse.on(_._2)).map(entry =>
                (entry._1, entry._2, implicationToCopyrightMap.getOrElse(entry._1, entry._1))
              )

              val top100List = top100.map(_._1)

              val filteredConnections = copyrightPairings.filter(ent => top100List.contains(ent._1) && top100List.contains(ent._2)).filter(_._3 > 10).sortBy(-_._3)


              Json(DefaultFormats).write((top100, filteredConnections.collect()))
            })))
          }
        },

        // This path returns data regarding the character counts per mainCopyright
        // Two types of entries are generated. If a copyright is within the 19 highest ranking ones a normal entry is generated
        // If that is not the case it is grouped together with other entries to not clutter the pie chart
        path("characterPerCopyright") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val entries = mainCopyrightToCharacterRDD.mapValues(_.size).takeOrdered(19)(Ordering[Int].reverse.on(_._2))

              val copyrightEntries = entries.map(_._1)


              val accumulatedEntries = mainCopyrightToCharacterRDD.filter(entry => !copyrightEntries.contains(entry._1)).mapValues(_.size)
                .fold(("Characters in Copyrights besides the Top 19", 0))((A, B) => (A._1, A._2 + B._2))

              Json(DefaultFormats).write(entries :+ accumulatedEntries)
            })))
          }
        },

        //This path returns the 5 key values of a boxplot regarding the total count of characters per maincopyright
        // The 5 values are in the following order [Min, Q1, Median, Q3, Max]
        path("characterPerCopyrightBoxplot") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val entries = mainCopyrightToCharacterRDD.mapValues(_.size).values
              val numbers = entries.sortBy(identity).zipWithIndex().map(_.swap)
              val count = entries.count()

              val minVal = numbers.first()._2

              val quart1 = if (count % 2 == 0) {
                val left = count / 4 - 1
                val right = left + 1
                (numbers.lookup(left).head + numbers.lookup(right).head).toDouble / 2
              } else numbers.lookup(count / 4).head.toDouble

              val median: Double = if (count % 2 == 0) {
                val left = count / 2 - 1
                val right = left + 1
                (numbers.lookup(left).head + numbers.lookup(right).head).toDouble / 2
              } else numbers.lookup(count / 2).head.toDouble

              val quart3 = if (count % 2 == 0) {
                val left = (count / 4) * 3 - 1
                val right = left + 1
                (numbers.lookup(left).head + numbers.lookup(right).head).toDouble / 2
              } else numbers.lookup((count / 4) * 3).head.toDouble

              val maxVal = numbers.lookup(count - 1).head.toDouble


              Json(DefaultFormats).write(Array(minVal, quart1, median, quart3, maxVal))
            })))
          }
        },


        // -------------------- Start of the section with routes for specific copyrights --------------------


        // This path returns the total count of images for the given copyright
        path("total" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = totalRDD.filter(_._1 == copy)
              Json(DefaultFormats).write(temp.collect())
            })))
          }
        },

        // This path returns the imagecount per year for the given copyright
        // If the copyright had 0 images/is not present in a given year after the first image of the copyright was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("year" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => copy == elem.get("_id").asInstanceOf[Document].get("copyright").toString).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
                  (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap)

              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min to max) yield {
                  (i, elem.getOrElse(i, 0))
                }
              }.toList.sortBy(_._1))

              val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
              val absolute = temp2.flatMap(_._2.map(_._1)).max

              Json(DefaultFormats).write((absoluteMin, absolute, temp2.collect()))
            })))
          }

        },

        // This path returns the imagecount per month for the given copyright
        // If the copyright had 0 images/is not present in a given month after the first image of the copyright was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("month" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => copy == elem.get("_id").asInstanceOf[Document].get("copyright").toString).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
                  ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap)

              //.groupBy(_._1).mapValues(elem => elem.map(x => ((x._2, x._3), x._4)).toMap)


              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min._1 to max._1) yield {
                  for (j <- 1 to 12) yield {
                    ((i, j) -> elem.getOrElse((i, j), 0))
                  }
                }
              }.flatten.toList)

              val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
              val absoulteMax = temp2.flatMap(_._2.map(_._1)).max

              val minString = absoluteMin._1 + "-" + absoluteMin._2
              val maxString = absoulteMax._1 + "-" + absoulteMax._2

              val tempFin = temp2.mapValues(_.map(x => (x._1._1 + "-" + x._1._2, x._2)))

              Json(DefaultFormats).write((minString, maxString, tempFin.collect()))
            })))
          }
        },

        //This path returns data regarding the image count per day for the last 31 days for the given copyright
        // If the copyright had 0 images/is not present on a given day a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("day" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val endDate = LocalDate.now()
              val beginDate = LocalDate.now().minusDays(31)

              val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList

              val temp = dayRDD.filter(elem => copy == (elem.get("_id").asInstanceOf[Document].get("copyright").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("copyright").toString,
                  ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int],
                    elem.get("_id").asInstanceOf[Document].get("day").asInstanceOf[Int]),
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap.filter(entry => dateList.contains(entry._1)))
              //.groupBy(_._1).mapValues(elem => elem.map(x => ((x._2, x._3, x._4), x._5)).filter(entry => dateList.contains(entry._1)).toMap)


              val temp2 = temp.mapValues(elem => {
                for (i <- dateList) yield {
                  i -> elem.getOrElse(i, 0)
                }
              })


              val minString = beginDate.getYear + "-" + beginDate.getMonthValue + "-" + beginDate.getDayOfMonth
              val maxString = endDate.getYear + "-" + endDate.getMonthValue + "-" + endDate.getDayOfMonth

              var tempFin = temp2.mapValues(_.map(x => (x._1._1 + "-" + x._1._2 + "-" + x._1._3, x._2))).collect()

              if (temp2.count() == 0) {
                val dayList = for (i <- dateList) yield {
                  (i._1 + "-" + i._1 + "-" + i._1, 0)
                }
                tempFin = Array((copy, dayList))
              }


              Json(DefaultFormats).write((minString, maxString, tempFin))
            })))
          }
        },

        // This path returns 2 fields the first one shows if the given tag originates from a given list of tags (usually one)
        // The second field shows a list of tags that originate from the given tag (e.g touhou_(pc-98) originates from touhou)
        path("implications" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val originating = implicationsRDD.filter(entry => entry._2.contains(copy)).keys
              val implications = implicationsRDD.filter(entry => entry._1.equals(copy)).flatMap(ent => ent._2)
              Json(DefaultFormats).write((originating.collect(), implications.collect()))
            })))
          }
        },

        // This path returns a list of all copyrights that exist in images of the current copyright together with a count of occurrences.
        // The list of occurrences is sorted in an descending order
        path("copyrightPairing" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val copyrights = copyrightPairings.filter(ent => ent._1 == copy || ent._2 == copy).map(ent =>
                if (ent._1 == copy) {
                  (ent._2, ent._3)
                } else {
                  (ent._1, ent._3)
                }
              ).sortBy(-_._2)
              Json(DefaultFormats).write(copyrights.collect())
            })))
          }
        },

        //This path returns the top 19 copyrights that exist in images of the current copyright together with a count of occurrences.
        // The last entry represents the accumulated counts of other copyrights besides the top 19
        path("copyrightPairingPie" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val copyrights = copyrightPairings.filter(ent => ent._1 == copy || ent._2 == copy).map(ent =>
                if (ent._1 == copy) {
                  (ent._2, ent._3)
                } else {
                  (ent._1, ent._3)
                }
              ).sortBy(-_._2)

              val top19 = copyrights.takeOrdered(19)(Ordering[Int].reverse.on(_._2))

              val top19List = top19.map(_._1)

              val accumulatedEntries = copyrights.filter(ent => !top19List.contains(ent._1))
                .fold(("Copyrigths besides the top 19", 0))((A, B) => (A._1, A._2 + B._2))
              Json(DefaultFormats).write(top19 :+ accumulatedEntries)
            })))
          }
        },

        // This path returns a list of all present main characters in the given copyright or an associated/implicated copyright together with a count of occurrences
        // The list is sorted by occurrences in a descending order
        path("mainCharacters" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val implicationPair = implicationsRDD.filter(entry => entry._2.contains(copy) || entry._1 == copy).collect()

              try {
                val listOfPotentialCharacters = mainCopyrightToCharacterRDD.filter(entr => entr._1 == copy || implicationPair.map(_._1).contains(entr._1)).collect().head._2

                val charsPresent = characterCopyrightCombinations.filter(_._1._2 == copy).map(ent => (ent._1._1, ent._2))

                val filteredCharacters = charsPresent.filter(entr => listOfPotentialCharacters.contains(entr._1)).sortBy(-_._2)

                Json(DefaultFormats).write(filteredCharacters.collect())

              } catch { //An error occurs if there are no characters that have the given copyright as their maincopyright
                case _: Throwable => Json(DefaultFormats).write(List[String]())
              }
            })))
          }
        },


        //This path returns the top 19 main characters of a given copyright together with a count of occurrences
        // The last entry represents the accumulated counts of main characters besides the top 19
        path("mainCharactersPie" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val implicationPair = implicationsRDD.filter(entry => entry._2.contains(copy) || entry._1 == copy).collect()

              try {
                val listOfPotentialCharacters = mainCopyrightToCharacterRDD.filter(entr => entr._1 == copy || implicationPair.map(_._1).contains(entr._1)).collect().head._2

                val charsPresent = characterCopyrightCombinations.filter(_._1._2 == copy).map(ent => (ent._1._1, ent._2))

                val filteredCharacters = charsPresent.filter(entr => listOfPotentialCharacters.contains(entr._1))

                val top19Entries = filteredCharacters.takeOrdered(19)(Ordering[Int].reverse.on(_._2))
                val topEntryList = top19Entries.map(_._1)
                val accumulatedEntries = filteredCharacters.filter(ent => !topEntryList.contains(ent._1))
                  .fold(("Main characters besides the top 19", 0))((A, B) => (A._1, A._2 + B._2))
                Json(DefaultFormats).write(top19Entries :+ accumulatedEntries)

              } catch { //An error occurs if there are no characters that have the given copyright as their maincopyright
                case _: Throwable => Json(DefaultFormats).write(List[String]())
              }
            })))
          }
        },

        // This path returns a list of all present characters in the given copyright together with a count of occurrences
        // The list is sorted by occurrences in a descending order
        path("allCharacters" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val presentCharacters = characterCopyrightCombinations.filter(_._1._2 == copy).map(ent => (ent._1._1, ent._2)).sortBy(-_._2)
              Json(DefaultFormats).write(presentCharacters.collect())
            })))
          }
        },

        //This path returns the top 19 characters of a given copyright together with a count of occurrences
        // The last entry represents the accumulated counts of characters besides the top 19
        path("allCharactersPie" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val presentCharacters = characterCopyrightCombinations.filter(_._1._2 == copy).map(ent => (ent._1._1, ent._2))
              val top19Entries = presentCharacters.takeOrdered(19)(Ordering[Int].reverse.on(_._2))
              val topEntryList = top19Entries.map(_._1)
              val accumulatedEntries = presentCharacters.filter(ent => !topEntryList.contains(ent._1))
                .fold(("Characters besides the top 19", 0))((A, B) => (A._1, A._2 + B._2))
              Json(DefaultFormats).write(top19Entries :+ accumulatedEntries)
            })))
          }
        },

        // This path returns the image count per year for the top 8 characters connected to this copyright
        // If the character had 0 images/is not present in a given year after the first image of the character was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("topCharactersYear" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val presentCharacters = characterCopyrightCombinations.filter(_._1._2 == copy).map(ent => (ent._1._1, ent._2)).takeOrdered(8)(Ordering[Int].reverse.on(_._2)).map(_._1)
              val charCopyCombination = presentCharacters.map(ent => (ent, copy)) //Transform into form (Char,Copy)
              val filtered = characterCopyrightCombinationsYearRDD.filter(ent => charCopyCombination.contains(ent._1._1))

              val temp = filtered.groupBy(_._1._1._1).mapValues(elem => elem.map(x => (x._1._2, x._2)).toMap)

              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min to max) yield {
                  (i, elem.getOrElse(i, 0))
                }
              }.toList.sortBy(_._1))

              val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
              val absolute = temp2.flatMap(_._2.map(_._1)).max

              Json(DefaultFormats).write((absoluteMin, absolute, temp2.collect()))
            })))
          }
        },

        // This path returns the image count per month for the the top 8 characters connected to this copyright
        // If the character had 0 images/is not present in a given month after the first image of the character was present a zero is assigned for the missing data
        // This route takes some time because the characterCopyrightCombinationsMonthRDD cannot be cached or otherwise the calculations may randomly stop
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("topCharactersMonth" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val presentCharacters = characterCopyrightCombinations.filter(_._1._2 == copy).map(ent => (ent._1._1, ent._2)).takeOrdered(8)(Ordering[Int].reverse.on(_._2)).map(_._1)
              val charCopyCombination = presentCharacters.map(ent => (ent, copy)) //Transform into form (Char,Copy)
              val filtered = characterCopyrightCombinationsMonthRDD.filter(ent => charCopyCombination.contains(ent._1._1))

              val temp = filtered.groupBy(_._1._1._1).mapValues(elem => elem.map(x => ((x._1._2, x._1._3), x._2)).toMap)

              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min._1 to max._1) yield {
                  for (j <- 1 to 12) yield {
                    (i, j) -> elem.getOrElse((i, j), 0)
                  }
                }
              }.flatten.toList)

              val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
              val absoulteMax = temp2.flatMap(_._2.map(_._1)).max

              val minString = absoluteMin._1 + "-" + absoluteMin._2
              val maxString = absoulteMax._1 + "-" + absoulteMax._2

              val tempFin = temp2.mapValues(_.map(x => (x._1._1 + "-" + x._1._2, x._2)))

              Json(DefaultFormats).write((minString, maxString, tempFin.collect()))
            })))
          }
        },

        //This path returns data regarding the image count per day for the last 31 days for the the top 8 characters connected to this copyright
        // If the character had 0 images/is not present on a given day a zero is assigned for the missing data if another value is present in the timeframe
        // If no entry is present in the entire timeframe for a character the character is left out
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("topCharactersDay" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val endDate = LocalDate.now()
              val beginDate = LocalDate.now().minusDays(31)

              val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList

              val presentCharacters = characterCopyrightCombinations.filter(_._1._2 == copy).map(ent => (ent._1._1, ent._2)).takeOrdered(8)(Ordering[Int].reverse.on(_._2)).map(_._1)
              val charCopyCombination = presentCharacters.map(ent => (ent, copy)) //Transform into form (Char,Copy)
              val filtered = characterCopyrightCombinationsDayRDD.filter(ent => charCopyCombination.contains(ent._1._1))

              val temp = filtered.groupBy(_._1._1._1).mapValues(elem => elem.map(x => ((x._1._2, x._1._3, x._1._4), x._2)).filter(entry => dateList.contains(entry._1)).toMap)

              val temp2 = temp.mapValues(elem => {
                for (i <- dateList) yield {
                  i -> elem.getOrElse(i, 0)
                }
              })

              val minString = beginDate.getYear + "-" + beginDate.getMonthValue + "-" + beginDate.getDayOfMonth
              val maxString = endDate.getYear + "-" + endDate.getMonthValue + "-" + endDate.getDayOfMonth

              val tempFin = temp2.mapValues(_.map(x => (x._1._1 + "-" + x._1._2 + "-" + x._1._3, x._2)))

              Json(DefaultFormats).write((minString, maxString, tempFin.collect()))
            })))
          }
        },

        //This path returns data regarding the connections between the 100 most popular characters associated with the given tag
        // In this case only characters are shown that either have the given copyright as a maincopyright or the maincopyright of the given copyright as a main copyright
        // This is done to exclude data with potentially too many connections from other copyrights to not clutter the forcegraph
        // The first field contains information regarding the character (nodes) with name, count of total images and the main copyright
        // The second field contains information regarding the connections with the 2 Nodes and the count for the connections between them
        // To improve computing times for the forcegraph at least 5 entries must be present to count as a connection
        path("characterCombinations" / Segment) { copy =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {

              try {

                val implicationPair = implicationsRDD.filter(entry => entry._2.contains(copy) || entry._1 == copy).collect()
                val listOfPotentialCharacters = mainCopyrightToCharacterRDD.filter(entr => entr._1 == copy || implicationPair.map(_._1).contains(entr._1)).collect().head._2

                val top100PresentCharacters = characterCopyrightCombinations.filter(_._1._2 == copy)
                  .filter(ent => listOfPotentialCharacters.contains(ent._1._1)).map(ent => (ent._1._1, ent._2)).takeOrdered(100)(Ordering[Int].reverse.on(_._2)).map(_._1)
                val filteredNodes = characterTotalRDD.filter(ent => top100PresentCharacters.contains(ent._1))
                  .map(entry => (entry._1, entry._2, characterToMainCopyright(entry._1))).sortBy(-_._2)

                val filteredConnections = characterCombinationRDD.filter(ent => top100PresentCharacters.contains(ent._1) && top100PresentCharacters.contains(ent._2)).filter(_._3 > 5).sortBy(-_._3)

                val topFilteredConnections = filteredConnections.take(5).map(_._3)

                val avgTopConnectionValue = topFilteredConnections.sum / topFilteredConnections.length

                val cleanedConnections = filteredConnections.filter(_._3 > (avgTopConnectionValue/100))

                val presentNodes = (cleanedConnections.map(_._1) ++ cleanedConnections.map(_._2) ).distinct().collect()

                val cleanedNodes = filteredNodes.filter(x => presentNodes.contains(x._1))

                Json(DefaultFormats).write((avgTopConnectionValue, cleanedNodes.collect(), cleanedConnections.collect()))

              } catch { //An error occurs if there are no characters that have the given copyright as their maincopyright
                case _: Throwable => Json(DefaultFormats).write((0,List[String](), List[String]()))
              }
            })))
          }
        },
      )
    }
  }
}
