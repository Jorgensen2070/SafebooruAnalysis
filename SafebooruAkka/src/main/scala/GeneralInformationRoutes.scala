import AkkaMain.{charList, corsHandler}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, pathPrefix, _}
import akka.http.scaladsl.server.{RequestContext, RouteResult}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scala.concurrent.Future

object GeneralInformationRoutes {
  def getGeneralInformationRoutes(pathname: String,
                                  avgUpScoreTotal: RDD[(String, Double)],
                                  avgUpScoreYear: RDD[(String, (Int, Double))],avgUpScoreMonth: RDD[(String, ((Int, Int), Double))]): RequestContext => Future[RouteResult] = {
    pathPrefix(pathname) {
      concat(
        path("avgUpScore" / "total" / Segment) { tag =>
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = avgUpScoreTotal.filter(_._1 == tag).mapValues(BigDecimal(_).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble)
            Json(DefaultFormats).write(temp.collect())
          })
          ))
        },

        path("avgUpScore" / "year" / Segment) { tag =>
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = avgUpScoreYear.filter(_._1 == tag).groupByKey().mapValues(_.toMap)
            val temp2 = temp.mapValues(elem => {
              val min = elem.keys.min
              val max = elem.keys.max

              for (i <- min to max) yield {
                (i, BigDecimal(elem.getOrElse(i, 0.0)).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble)
              }
            }.toList.sortBy(_._1))

            val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
            val absolute = temp2.flatMap(_._2.map(_._1)).max

            Json(DefaultFormats).write((absoluteMin, absolute, temp2.collect()))
          })
          ))
        },

        path("avgUpScore" / "month" / Segment) { tag =>
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = avgUpScoreMonth.filter(_._1 == tag).groupByKey().mapValues(_.toMap)
            val temp2 = temp.mapValues(elem => {
              val min = elem.keys.min
              val max = elem.keys.max

              for (i <- min._1 to max._1) yield {
                for (j <- 1 to 12) yield {
                  (i, j) -> BigDecimal(elem.getOrElse((i, j), 0.0)).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble
                }
              }
            }.flatten.toList)

            val absoluteMin = temp2.flatMap(_._2.map(_._1)).min
            val absoulteMax = temp2.flatMap(_._2.map(_._1)).max

            val minString = absoluteMin._1 + "-" + absoluteMin._2
            val maxString = absoulteMax._1 + "-" + absoulteMax._2

            val tempFin = temp2.mapValues(_.map(x => (x._1._1 + "-" + x._1._2, x._2)))

            Json(DefaultFormats).write((minString, maxString, tempFin.collect()))
          })
          ))
        }
      )
    }
  }

}
