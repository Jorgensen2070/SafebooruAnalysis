import DownloadMethods.{downloadLoopSingleTag, generateImplicationsList, testMethodForDownload}

import java.io.FileWriter
import java.time.LocalDateTime

object TestDownload {
  /**
   * This main method is used to downlaod all the image data and downlaod and generate an implication list before saving it all to the databse
   */
  def main(args: Array[String]): Unit = {
    val fw = new FileWriter("TestLog.txt", true)

    println("Start")
    val startTimeMillis = System.currentTimeMillis()
    fw.append("\n Started the test download \n")

    testMethodForDownload(fw)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    fw.append("\n Ended Download Application:")
    fw.append("\n Duration: " + durationSeconds)

    fw.close()
  }

}
