import DownloadMethods.{downloadLoopSingleTag, generateImplicationsList}

import java.io.FileWriter
import java.time.LocalDateTime

object InitialDownload {

  /**
   * This main method is used to downlaod all the image data and downlaod and generate an implication list before saving it all to the databse
   */
  def main(args: Array[String]): Unit = {
    val fw = new FileWriter("InitialDownloadLog.txt", true)

    println("Start")
    val startTimeMillis = System.currentTimeMillis()
    val todaysDate = LocalDateTime.now().toString
    fw.append("\n Started the initial download \n")
    fw.append("\n Current date " + todaysDate)

    downloadLoopSingleTag("#", "downloadAll", fw)
    generateImplicationsList(fw)

    println("Ende")
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    fw.append("\n Ended Download Application:")
    fw.append("\n Duration: " + durationSeconds)

    fw.close()
  }

}
