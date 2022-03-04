import AkkaMain.{charList, corsHandler}
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

object CharacterRoutes {
  def getCharacterRoutes(pathname: String,
                         yearRDD: RDD[Document],
                         monthRDD: RDD[Document],
                         dayRDD: RDD[(String, ((Int, Int, Int), Int))],
                         totalRDD: RDD[(String, Int)],
                         characterToMainCopyright: Map[String, String],
                         characterCombinationRDD: RDD[(String, String, Int)],
                         implicationsRDD: RDD[(String, List[String])],
                         characterCopyrightCombinationRDD: RDD[((String, String), Int)],
                         characterGeneralTagCombinationsRDD: RDD[((String, String), Int)]
                        ): RequestContext => Future[RouteResult] = {
    pathPrefix(pathname) {
      concat(

        // This path returns a list of all characters. This list can be used for routing purposes to check if the desired character rly exists
        path("list") {
          get {
            corsHandler(
              complete(HttpEntity(ContentTypes.`application/json`, {
                val temp = totalRDD.sortBy(-_._2).map(_._1)
                Json(DefaultFormats).write(temp.collect())
              })))
          }
        },

        // This path returns data for a piechart regarding total the total count of images for characters
        // Two types of entries are generated. If a character is within the top 1op 20 entries a normal entry is generated
        // If that is not the case it is grouped together with other entries below that threshold to not clutter the pie chart
        path("total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val top19Entries = totalRDD.takeOrdered(19)(Ordering[Int].reverse.on(_._2))
              val topEntryList = top19Entries.map(_._1)
              val accumulatedEntries = totalRDD.filter(ent => !topEntryList.contains(ent._1)).fold(("Characters besides The top 19", 0))((A, B) => (A._1, A._2 + B._2))
              Json(DefaultFormats).write(top19Entries :+ accumulatedEntries)
            })))
          }
        },

        // This path returns data regarding the image count per year for the 9 most popular characters (one character per copyright) on safebooru
        // If a character had 0 images/is not present in a given year after the first image of the character was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => charList.contains(elem.get("_id").asInstanceOf[Document].get("character").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("character").toString,
                  (elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap)

              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min to max) yield {
                  (i, elem.getOrElse(i, 0).asInstanceOf[Int])
                }
              }.toList.sortBy(_._1))

              val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
              val absolute = temp2.flatMap(_._2.map(_._1)).max

              Json(DefaultFormats).write((absoluteMin, absolute, temp2.collect()))
            })))
          }

        },

        //This path returns data regarding the image count per month  for the 9 most popular characters (one character per copyright) on safebooru
        // If a character had 0 images/is not present in a given month after the first image of the character was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => charList.contains(elem.get("_id").asInstanceOf[Document].get("character").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("character").toString,
                  ((elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                    elem.get("_id").asInstanceOf[Document].get("month").asInstanceOf[Int]),
                    elem.get("count").asInstanceOf[Int])
                )).groupByKey().mapValues(_.toMap)


              val temp2 = temp.mapValues(elem => {
                val min = elem.keys.min
                val max = elem.keys.max

                for (i <- min._1 to max._1) yield {
                  for (j <- 1 to 12) yield {
                    ((i, j) -> elem.getOrElse((i, j), 0).asInstanceOf[Int])
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

        //This path returns data regarding the image count per day for the last 31 days for the 9 most popular characters (one character per copyright) on safebooru
        // If a character had 0 images/is not present on a given day a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("day") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val endDate = LocalDate.now()
              val beginDate = LocalDate.now().minusDays(31)

              val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList

              val temp = dayRDD.filter(elem => charList.contains(elem._1)).groupByKey().mapValues(_.toMap)

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

        //This path returns data regarding the connections between the 100 most popular characters on safebooru
        // The first field contains information regarding the avg value of the top 5 connections which is used as a variable for generating the forcegraph
        // The second field contains information regarding the character (nodes) with name, count of total images and the main copyright
        // The third field contains information regarding the connections between two nodes and the count for the amount  of connections between them
        // To improve computing times for the forcegraph the connections are filtered so that only connections that exceed 1% of the avg value of the top 5 connections are returned
        path("characterPairing") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val top100 = totalRDD.takeOrdered(101)(Ordering[Int].reverse.on(_._2)).filter(!_._1.isBlank).map(
                entry => (entry._1, entry._2, characterToMainCopyright(entry._1))
              )
              val top100Characters = top100.map(_._1)

              val filteredConnections = characterCombinationRDD.filter(ent => top100Characters.contains(ent._1) && top100Characters.contains(ent._2)).filter(_._3 > 5).sortBy(-_._3)

              val topFilteredConnections = filteredConnections.take(5).map(_._3)

              val avgTopConnectionValue = topFilteredConnections.sum / topFilteredConnections.length

              val cleanedConnections = filteredConnections.filter(_._3 > (avgTopConnectionValue / 100))

              /*val presentNodes = (cleanedConnections.map(_._1) ++ cleanedConnections.map(_._2) ).distinct().collect()

              val cleanedNodes = top100.filter(x => presentNodes.contains(x._1))*/

              val cleanedNodes = top100

              Json(DefaultFormats).write((avgTopConnectionValue, cleanedNodes, cleanedConnections.collect()))
            })))
          }
        },

        //This path returns the 5 key values of a boxplot regarding the total image counts per character
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


        // -------------------- Start of the section with routes for specific characters --------------------

        // This path returns the total count of images for the given character
        path("total" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = totalRDD.filter(_._1 == char)
              Json(DefaultFormats).write(temp.collect())
            })))
          }
        },

        // This path returns the imagecount per year for the given character
        // If the character had 0 images/is not present in a given year after the first image of the character was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("year" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => char == elem.get("_id").asInstanceOf[Document].get("character").toString).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("character").toString, (
                  elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
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

        // This path returns the imagecount per month for the given character
        // If the character had 0 images/is not present in a given month after the first image of the character was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("month" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => char == elem.get("_id").asInstanceOf[Document].get("character").toString).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("character").toString,
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

        //This path returns data regarding the image count per day for the last 31 days for the given character
        // If the character had 0 images/is not present on a given day a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("day" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val endDate = LocalDate.now()
              val beginDate = LocalDate.now().minusDays(31)

              val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList

              val temp = dayRDD.filter(elem => char == elem._1)
                .groupByKey().mapValues(_.toMap)

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
                tempFin = Array((char, dayList))
              }

              Json(DefaultFormats).write((minString, maxString, tempFin))
            })))
          }
        },

        // This path returns the maincopyright of the given character. If no main copyright is present "- no main copyright -" will be returned
        path("mainCopyright" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              try {
                val mainCopyright = characterToMainCopyright(char)
                Json(DefaultFormats).write(mainCopyright)
              } catch {
                case _: Throwable => Json(DefaultFormats).write("- no main copyright -")
              }
            })))
          }
        },

        // This path returns a list of copyrights that the given character was tagged together with together with occurrences
        path("copyright" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val copyrights = characterCopyrightCombinationRDD.filter(_._1._1 == char).map(ent => (ent._1._2, ent._2)).sortBy(-_._2)
              Json(DefaultFormats).write(copyrights.collect())
            })))
          }
        },

        //This path returns the top 19 copyrights (besides the main copyright) that the given character was tagged together with occurrences
        // The last entry represents the accumulated counts of copyrights besides the top 19
        path("copyrightPie" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              try {

                val mainCopyright = characterToMainCopyright(char)
                val copyrights = characterCopyrightCombinationRDD.filter(_._1._1 == char).filter(_._1._2 != mainCopyright).map(ent => (ent._1._2, ent._2))
                val top19Entries = copyrights.takeOrdered(19)(Ordering[Int].reverse.on(_._2))

                val topEntryList = top19Entries.map(_._1)
                val accumulatedEntries = copyrights.filter(ent => !topEntryList.contains(ent._1))
                  .fold(("Copyrights besides the top 19", 0))((A, B) => (A._1, A._2 + B._2))
                Json(DefaultFormats).write(top19Entries :+ accumulatedEntries)

              } catch { //If no maincopyrigth is available
                case _: Throwable => {
                  val copyrights = characterCopyrightCombinationRDD.filter(_._1._1 == char).map(ent => (ent._1._2, ent._2))
                  val top19Entries = copyrights.takeOrdered(19)(Ordering[Int].reverse.on(_._2))

                  val topEntryList = top19Entries.map(_._1)
                  val accumulatedEntries = copyrights.filter(ent => !topEntryList.contains(ent._1))
                    .fold(("Copyrights besides the top 19", 0))((A, B) => (A._1, A._2 + B._2))
                  Json(DefaultFormats).write(top19Entries :+ accumulatedEntries)
                }
              }
            })))
          }
        },

        //This path returns a list of characters that the given character was tagged together with together with occurrences
        path("characterPairing" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val characters = characterCombinationRDD.filter(ent => ent._1 == char || ent._2 == char).map(ent =>
                if (ent._1 == char) {
                  (ent._2, ent._3)
                } else {
                  (ent._1, ent._3)
                }
              ).sortBy(-_._2)
              Json(DefaultFormats).write(characters.collect())
            })))
          }
        },

        //This path returns the top 19 characters that the given character was tagged together with occurrences
        // The last entry represents the accumulated counts of characters besides the top 19
        path("characterPairingPie" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val characters = characterCombinationRDD.filter(ent => ent._1 == char || ent._2 == char).map(ent =>
                if (ent._1 == char) {
                  (ent._2, ent._3)
                } else {
                  (ent._1, ent._3)
                }
              ).sortBy(-_._2)

              val top19 = characters.takeOrdered(19)(Ordering[Int].reverse.on(_._2))

              val top19List = top19.map(_._1)

              val accumulatedEntries = characters.filter(ent => !top19List.contains(ent._1))
                .fold(("Characters besides the top 19", 0))((A, B) => (A._1, A._2 + B._2))
              Json(DefaultFormats).write(top19 :+ accumulatedEntries)
            })))
          }
        },

        // This path returns a list of all generalTags that the given character was tagged with together with occurrences
        path("tags" / Segment) { char => {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val tags = characterGeneralTagCombinationsRDD.filter(_._1._1 == char).map(ent => (ent._1._2, ent._2)).sortBy(-_._2)
              Json(DefaultFormats).write(tags.collect())
            })))
          }
        }
        },

        // This path returns information needed for creating a radarChart regarding the top 10 tags given to a certain character
        // The first field indicated the maximum value of images associated with a given character
        // The second field is an array of the top 10 tags the character was tagged with
        // The third field is an array of occurrences of the tags from the second field
        path("tagsRadar" / Segment) { char => {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val topTags = characterGeneralTagCombinationsRDD.filter(_._1._1 == char).map(ent => (ent._1._2, ent._2)).takeOrdered(10)(Ordering[Int].reverse.on(_._2))
              val tagNames = topTags.map(_._1)
              val tagValues = topTags.map(_._2)
              val maxValue = totalRDD.filter(_._1 == char).first()._2

              Json(DefaultFormats).write((maxValue, tagNames, tagValues))
            })))
          }
        }
        }

      )
    }
  }
}
