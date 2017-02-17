package com.github.xmlavrobridge

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by ajoseph on 1/8/17.
  */
trait HFSReader {
  def listFiles(s: String): List[String] = {
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

    val fs = FileSystem.get(conf)
    fs.listStatus(new Path(s)).filter(!_.getPath.toString.endsWith(".avsc")).map {
      p => p.getPath.toString
    }.toList
  }

  def openFileDemo(s: String) = {
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

    val fs = FileSystem.get(conf)
    fs.open(new Path(s))
  }
}
