import AkkaMain.corsHandler
import akka.http.scaladsl.server.{RequestContext, RouteResult}
import scala.concurrent.Future
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, pathPrefix}
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import akka.http.scaladsl.server.Directives._


object TagGroupRoutes {

  //Here are the definitions of the tagGroup with the tags that they contain
  //The topXXXXXXXX are the 8 most occurring entries per tagGroup
  val headwearAndHeadgear: Array[String] = Array("crown", "hair_bow", "hair_ribbon", "hairband", "headband", "headdress", "hat", "helmet", "tiara", "veil")
  val topHeadwearAndHeadgear: Array[String] = Array("crown", "hair_bow", "hair_ribbon", "hairband", "headband", "hat", "helmet", "tiara")

  val shirtsAndTopwear: Array[String] = Array("blouse", "bustier", "crop top", "camisole", "cardigan", "corset", "dress", "halterneck", "hoodie", "jacket",
    "poncho", "raglan_sleeves", "sash", "shirt", "shrug", "surcoat", "sweater", "tabard", "tailcoat", "tank_top", "tube_top", "underbust", "vest")
  val topShirtsAndTopwear: Array[String] = Array("dress", "shirt", "jacket", "vest", "sweater", "sash", "hoodie", "halterneck", "cardigan")

  val pantsAndBottomwear: Array[String] = Array("bloomers", "buruma", "chaps", "pants", "pelvic_curtain", "petticoat", "sarong", "shorts", "skirt")

  val legsAndFootwear: Array[String] = Array("garters", "kneehighs", "leggings", "leg_warmers", "over-kneehighs", "pantyhose", "socks", "thighhighs", "thigh_strap")

  val shoesAndFootwear: Array[String] = Array("boots", "cross-laced_footwear", "flats", "high_heels", "loafers", "mary_janes", "platform_footwear", "pointy_footwear", "pumps", "sandals",
    "footwear_ribbon", "slippers", "sneakers", "wedge_heels")
  val topShoesAndFootwear: Array[String] = Array("boots", "high_heels", "sandals", "loafers", "sneakers", "mary_janes", "cross-laced_footwear", "slippers", "platform_footwear")

  val uniformsAndCostumes: Array[String] = Array(
    "apron", "armor", "band_uniform", "cape", "cassock", "cheerleader", "gym_uniform", "habit", "kigurumi", "maid", "military_uniform", "overalls", "pajamas", "pilot_suit", "sailor",
    "santa_costume", "school_uniform", "shosei", "suit", "track_suit", "waitress")
  val topUniformAndCostumes: Array[String] = Array("school_uniform", "cape", "armor", "apron", "maid", "military_uniform", "suit", "santa_costume", "pajamas")

  val swimsuitsAndBodysuits: Array[String] = Array("bikesuit", "bodystocking", "bodysuit", "jumpsuit", "leotard", "swimsuit", "rash_guard", "robe", "kesa", "romper", "sarong", "tunic", "unitard")
  val topSwimsuitsAndBodysuits: Array[String] = Array("swimsuit", "leotard", "bodysuit", "robe", "sarong", "bodystocking", "jumpsuit", "tunic", "kesa")

  val traditionalClothing: Array[String] = Array("chinese_clothes", "dirndl", "korean_clothes", "vietnamese_dress", "hakama", "kimono", "haori", "miko", "fundoshi")

  def getTagGroupRoutes(pathname: String, yearRDD: RDD[Document], monthRDD: RDD[(String, ((Int, Int), Int))], totalRDD: RDD[Document]): RequestContext => Future[RouteResult] = {
    pathPrefix(pathname) {
      concat(

        // ---------- Routes for the headwearAndHeadgear tagGroup ----------

        path("headwearAndHeadgear" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => headwearAndHeadgear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("headwearAndHeadgear" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => headwearAndHeadgear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
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
        path("headwearAndHeadgear" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => headwearAndHeadgear.contains(elem._1)).groupByKey().mapValues(_.toMap)

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

        // ---------- Routes for the shirtsAndTopwear tagGroup ----------

        path("shirtsAndTopwear" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => shirtsAndTopwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("shirtsAndTopwear" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => topShirtsAndTopwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
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
        path("shirtsAndTopwear" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => topShirtsAndTopwear.contains(elem._1)).groupByKey().mapValues(_.toMap)

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

        // ---------- Routes for the pantsAndBottomwear tagGroup ----------
        
        path("pantsAndBottomwear" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => pantsAndBottomwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("pantsAndBottomwear" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => pantsAndBottomwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
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
        path("pantsAndBottomwear" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => pantsAndBottomwear.contains(elem._1)).groupByKey().mapValues(_.toMap)

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

        // ---------- Routes for the legsAndFootwear tagGroup ----------

        path("legsAndFootwear" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => legsAndFootwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("legsAndFootwear" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => legsAndFootwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
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
        path("legsAndFootwear" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => legsAndFootwear.contains(elem._1)).groupByKey().mapValues(_.toMap)
              //.groupBy(_._1).mapValues(elem => elem.map(x => ((x._2, x._3), x._4)).toMap)


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

        // ---------- Routes for the shoesAndFootwear tagGroup ----------

        path("shoesAndFootwear" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => shoesAndFootwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("shoesAndFootwear" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => topShoesAndFootwear.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
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
        path("shoesAndFootwear" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => topShoesAndFootwear.contains(elem._1)).groupByKey().mapValues(_.toMap)
              //.groupBy(_._1).mapValues(elem => elem.map(x => ((x._2, x._3), x._4)).toMap)


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

        // ---------- Routes for the uniformsAndCostumes tagGroup ----------

        path("uniformsAndCostumes" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => uniformsAndCostumes.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("uniformsAndCostumes" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => topUniformAndCostumes.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
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
        path("uniformsAndCostumes" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => topUniformAndCostumes.contains(elem._1)).groupByKey().mapValues(_.toMap)

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

        // ---------- Routes for the swimsuitsAndBodysuits tagGroup ----------

        path("swimsuitsAndBodysuits" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => swimsuitsAndBodysuits.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("swimsuitsAndBodysuits" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => topSwimsuitsAndBodysuits.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
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
        path("swimsuitsAndBodysuits" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => topSwimsuitsAndBodysuits.contains(elem._1)).groupByKey().mapValues(_.toMap)

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

        // ---------- Routes for the traditionalClothing tagGroup ----------

        path("traditionalClothing" / "total") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val total = totalRDD.filter(elem => traditionalClothing.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
                  elem.get("count").asInstanceOf[Int]
                )).sortBy(-_._2)
              Json(DefaultFormats).write(total.collect())
            })))
          }
        },
        path("traditionalClothing" / "year") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = yearRDD.filter(elem => traditionalClothing.contains(elem.get("_id").asInstanceOf[Document].get("tag").toString)).map(elem =>
                (elem.get("_id").asInstanceOf[Document].get("tag").toString,
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
        path("traditionalClothing" / "month") {
          get {
            corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
              val temp = monthRDD.filter(elem => traditionalClothing.contains(elem._1)).groupByKey().mapValues(_.toMap)

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
      )
    }
  }
}
