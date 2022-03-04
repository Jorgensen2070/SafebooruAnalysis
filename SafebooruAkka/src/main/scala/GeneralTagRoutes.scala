import AkkaMain.{corsHandler, tagList}
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

object GeneralTagRoutes {
  def getGeneralTagRoutes(pathname: String, yearRDD: RDD[Document], monthRDD: RDD[(String, ((Int, Int), Int))], dayRDD: RDD[(String, ((Int, Int, Int), Int))], totalRDD: RDD[Document]): RequestContext => Future[RouteResult] = {
    pathPrefix(pathname) {
      concat(

        // This path returns a list of all generalTags. This list can be used for routing purposes to check if the desired tag rly exists
        path("list") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = totalRDD.map(elem => (elem.get("_id").asInstanceOf[Document].get("tag").toString, elem.get("count").asInstanceOf[Int])).sortBy(-_._2).map(_._1)
              Json(DefaultFormats).write(temp.collect())
            })))
          }
        },

        // This path returns data for a piechart regarding total the total count of images per generalTag
        // Two types of entries are generated. If a generalTag is within the top 1op 19 entries a normal entry is generated
        // If that is not the case it is grouped together with other entries below that threshold to not clutter the pie chart
        path("total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val normalEntries = totalRDD.map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).takeOrdered(19)(Ordering[Int].reverse.on(_._2))

              val topEntryList = normalEntries.map(_._1)

              val accumulatedEntries = totalRDD.filter(elem => topEntryList.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                ))
                .fold(("Tags besides the Top 19", 0))((A, B) => (A._1, A._2 + B._2))

              Json(DefaultFormats).write(normalEntries :+ accumulatedEntries)
            })))
          }
        },

        // This path returns data regarding the image count per year for the 9 most popular generalTags on safebooru
        // If a tag had 0 images/is not present in a given year after the first image of the tag was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => tagList.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
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

        //This path returns data regarding the image count per month  for the 9 most popular generalTags on safebooru
        // If a tag had 0 images/is not present in a given month after the first image of the tag was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => tagList.contains(elem._1)).groupByKey().mapValues(_.toMap)


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

        //This path returns data regarding the image count per day for the last 31 days for the 9 most popular generalTags on safebooru
        // If a tag had 0 images/is not present on a given day a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("day") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val endDate = LocalDate.now()
              val beginDate = LocalDate.now().minusDays(31)
              val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList

              val temp = dayRDD.filter(elem => tagList.contains(elem._1)).groupByKey().mapValues(_.toMap)
              //.groupBy(_._1).mapValues(elem => elem.map(x => ((x._2, x._3, x._4), x._5)).filter(entry => dateList.contains(entry._1)).toMap)


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

        //This path returns the 5 key values of a boxplot regarding the total image counts per tag
        // The 5 values are in the following order [Min, Q1, Median, Q3, Max]
        path("totalBoxplot") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val entries = totalRDD.map(elem => (elem.get("_id").asInstanceOf[Document].get("tag").toString, elem.get("count").asInstanceOf[Int])).values

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

        // -------------------- Start of the section with routes for specific generalTag --------------------

        // This path returns the total count of images for the given tag
        path("total" / Segment) { tag =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = totalRDD.map(elem => (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                elem.get("count").asInstanceOf[Int])).filter(_._1 == tag)
              Json(DefaultFormats).write(temp.collect())
            })))
          }
        },

        // This path returns the image count per year for the given tag
        // If the tag had 0 images/is not present in a given year after the first image of the tag was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("year" / Segment) { char =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => char == elem.get("_id").asInstanceOf[Document].get("tag").toString).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("_id").asInstanceOf[Document].get("year").asInstanceOf[Int],
                  elem.get("count").asInstanceOf[Int]
                )).groupBy(_._1).mapValues(elem => elem.map(x => (x._2, x._3)).toMap)

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

        // This path returns the imagecount per month for the given tag
        // If the tag had 0 images/is not present in a given month after the first image of the tag was present a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("month" / Segment) { tag =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => tag == elem._1).groupByKey().mapValues(_.toMap)

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

        //This path returns data regarding the image count per day for the last 31 days for the given tag
        // If the tag had 0 images/is not present on a given day a zero is assigned for the missing data
        // The first and second field contain information about the start and the end date for the graphic which is important for Apache E-Charts
        path("day" / Segment) { tag =>
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val endDate = LocalDate.now()
              val beginDate = LocalDate.now().minusDays(31)
              val dateList = beginDate.datesUntil(endDate).iterator().asScala.map(x => (x.getYear, x.getMonthValue, x.getDayOfMonth)).toList

              val temp = dayRDD.filter(elem => tag == elem._1).groupByKey().mapValues(_.toMap)


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
                tempFin = Array((tag, dayList))
              }

              Json(DefaultFormats).write((minString, maxString, tempFin))
            })))
          }
        },
      )
    }
  }
}
