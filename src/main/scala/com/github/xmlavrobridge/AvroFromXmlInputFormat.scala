package com.github.xmlavrobridge

import java.rmi.server.UID

import org.apache.hadoop.conf.Configuration

//import org.apache.avro.mapred.FsInput
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{RecordReader, _}

/**
  * Created by ajoseph on 6/25/16.
  */
class AvroFromXmlInputFormat extends FileInputFormat[NullWritable, AvroGenericRecordWritable] with JobConfigurable {


  var jobConf: JobConf = null

  override def getRecordReader(inputSplit: InputSplit, jobConf: JobConf, reporter: Reporter):
  RecordReader[NullWritable, AvroGenericRecordWritable] = {
    new AvroFromXmlGenericRecordReader(jobConf, inputSplit.asInstanceOf[FileSplit], reporter)
  }

  override def configure(jobConf: JobConf): Unit = {
    this.jobConf = jobConf;
  }
}


class AvroFromXmlGenericRecordReader(job: JobConf, split: FileSplit, reporter: Reporter) extends RecordReader[NullWritable, AvroGenericRecordWritable] with JobConfigurable {

  val SEPARATOR_TAG = "xml.separator.tag"
  val SEPARATOR_TAG_TYPE = "xml.separator.tag.type"
  val XML_SCHEMA_LOCATION = "xml.schema.location"

  val separatorTag = job.get(SEPARATOR_TAG, "")
  val separatorTagType = job.get(SEPARATOR_TAG_TYPE, "")
  val xmlSchemaLocation = job.get(XML_SCHEMA_LOCATION, "")

  require(separatorTag != "", s"Missing property: $SEPARATOR_TAG")
  require(separatorTagType != "", s"Missing property: $SEPARATOR_TAG_TYPE")
  require(xmlSchemaLocation != "", s"Missing property: $XML_SCHEMA_LOCATION")

  val conf = new Configuration()
  conf.set("xml.separator.tag", separatorTag)
  conf.set("xml.separator.tag.type", separatorTagType)

  val reader = AvroTransormer.avroTransformer(split.getPath, conf)
  val recordReaderID = new UID()

  val stop = split.getStart() + split.getLength()

  val (isEmptyInput, start) = if (split.getLength() == 0)
    (true, 0L)
  else
    (false, reader.tell)


  var jobConf: JobConf = job

  override def next(k: NullWritable, v: AvroGenericRecordWritable): Boolean = {

    reader.nextRecord match {
      case Some(r) =>
        v.setRecord(r);
        v.setRecordReaderID(recordReaderID);
        val s1 = reader.recordSchema
        v.setFileSchema(s1)
        true;
      case None =>
        false

    }

  }

  override def getProgress: Float = {
    if (stop == start) 0.0f else {
      Math.min(1.0f, (getPos() - start).toFloat / (stop - start).toFloat)
    }
  }

  override def getPos: Long = {
    if (isEmptyInput) 0 else reader.tell
  }

  override def createKey(): NullWritable = {
    NullWritable.get()
  }

  override def close(): Unit = {
    if (!isEmptyInput) reader.close
  }

  override def createValue(): AvroGenericRecordWritable = {
    new AvroGenericRecordWritable()
  }

  override def configure(jobConf: JobConf): Unit = {
    this.jobConf = jobConf
  }
}
