package util

import java.io.{BufferedReader, InputStreamReader}

import scala.io.Source

/**
  * Created by ajoseph on 8/5/16.
  */
object TestUtil {

  def resourceFiles(path: String) = {

    val files = List[String]()
    val resourceAsStream = {
      val in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)
      Option(in).getOrElse(getClass().getResourceAsStream(path))
    }
    val reader = Source.fromInputStream(resourceAsStream).bufferedReader()

    Iterator.continually(reader.readLine()).takeWhile(_ != null).map{
      line =>
        path + "/" + line
    }.toList

  }

  def openResourceFile(path: String) = {
    val resourceAsStream = {
      val in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)
      Option(in).getOrElse(getClass().getResourceAsStream(path))
    }

    resourceAsStream

  }

}
