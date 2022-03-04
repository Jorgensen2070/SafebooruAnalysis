import DownloadMethods.{downloadLoopSingleTag, generateImplicationsList}

import java.io.FileWriter
import java.time.LocalDateTime

object UpdateDownload {

  /**
   * This main method is used to update the image data in the databse and download a new implication list before saving it all to the database
   */
  def main(args: Array[String]): Unit = {
    val fw = new FileWriter("UpdateDownloadLog.txt", true)

    println("Start")
    val startTimeMillis = System.currentTimeMillis()
    val todaysDate = LocalDateTime.now().toString
    fw.append("\n Started the update download application \n")
    fw.append("\n Current date "+todaysDate)

    generateImplicationsList(fw)
    downloadLoopSingleTag("#", "update", fw)

    println("Ende")
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    fw.append("\n Ended Download Application:")
    fw.append("\n Duration: " + durationSeconds)

    fw.close()
  }
}
