import AkkaMain.corsHandler
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, concat, get, path, pathPrefix}
import akka.http.scaladsl.server.{RequestContext, RouteResult}
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import scala.collection.JavaConverters._
import scala.concurrent.Future
import akka.http.scaladsl.server.Directives._
import java.util

object LatestImages {
  def getLatestImagesRoute(pathname: String, latestImages: RDD[Document]): RequestContext => Future[RouteResult] = {
    pathPrefix(pathname) {
      concat(
        // This path returns a list of up to 5 URLs for the given tag
        // The URLs represent the 5 latest images uploaded for the given tag
        path(Segment) { tag =>
          corsHandler(complete(HttpEntity(ContentTypes.`application/json`, {
            val temp = latestImages.map(elem => {
              (
                elem.get("_id").asInstanceOf[Document].get("tag").asInstanceOf[String],
                elem.get("images").asInstanceOf[util.ArrayList[Document]].asScala.toList.map(ent => ent.get("url").asInstanceOf[String]))
            }).filter(_._1 == tag)
            Json(DefaultFormats).write(temp.collect())
          })
          ))
        }
      )
    }
  }
}
