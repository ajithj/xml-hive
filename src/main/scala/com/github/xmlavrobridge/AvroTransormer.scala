package com.github.xmlavrobridge

import java.io.{InputStream, StringReader}
import java.lang.{Boolean => JBoolean, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong}
import java.util

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.commons.io.input.ReaderInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConversions._
import scala.collection.mutable.Stack
import scala.io.Source
import scala.util.control.Breaks._
import scala.xml.MetaData
import scala.xml.pull._

/**
  * Created by ajoseph on 12/10/16.
  */

object AvroTransormer extends HFSReader {

  def avroTransformer(path: Path, conf: Configuration): AvroTransormer = {
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    val schemaConverter = new XSDToAvroSchemaConverter() {
      override val readFile: (String) => InputStream = openFile
      override val listDirectory: (String) => List[String] = listFiles
    }

    val separatorTagType = conf.get("xml.separator.tag.type")
    val separatorTagTypeNs = conf.get("xml.separator.tag.type.ns")
    val xmlSchemaLocation = conf.get("xml.schema.location")

    val schemaStr = schemaConverter.generateAvroSchema(xmlSchemaLocation,
      separatorTagTypeNs, separatorTagType, "default")

    val schema = new Schema.Parser().parse(new ReaderInputStream(new StringReader(schemaStr)))

    new AvroTransormer(openFile(path.toString), conf, schema)
  }


}

class AvroTransormer(in: InputStream, conf: Configuration, schema: Schema) {

  val progressCounter = new ProgressCounter(in)

  val xml = new XMLEventReader(Source.fromInputStream(progressCounter))

  val separatorTag = conf.get("xml.separator.tag")
  val separatorTagType = conf.get("xml.separator.tag.type")

  require( separatorTag != null, "xml.separator.tag is not set")
  require( separatorTagType != null, "xml.separator.tag.type is not set")


  val path = new Path


  val pathToSchemaMap = scala.collection.mutable.Map[String, Schema]()

  val recordSchema = XmlAvroHelper.recordSchema(separatorTagType, schema).get


  pathToSchemaMapper("." + separatorTag, recordSchema)

  def tell = progressCounter.getProgress

  def close = {
    in.close()
  }

  def nextRecord: Option[GenericData.Record] = {
    var insidePage = false
    val elementText = new StringBuilder
    val objStack = Stack[AvroObject]()

    def secondElement = {
      if (objStack.size < 2) None else Some(objStack.drop(1).top)
    }

    def handleEndTag(tag: String): Unit = secondElement match {
      case Some(e) if (e.field == tag) =>
        val tObj = objStack.pop()
        tObj.obj match {
          case _: GenericData.Record =>
            e.obj.asInstanceOf[util.ArrayList[Object]].add(tObj.obj)
          case _: util.ArrayList[_] =>
            val array = new GenericData.Array[Object](tObj.schema, tObj.obj.asInstanceOf[util.ArrayList[Object]])
            e.obj.asInstanceOf[GenericData.Record].put(tObj.field, array)
            handleEndTag(tag)
        }
      case _ =>
        setRecordFieldValue(objStack.top.obj.asInstanceOf[GenericData.Record], tag, elementText.toString().trim, path.schema)
    }

    breakable {
      for (event: XMLEvent <- xml) {

        event match {

          case EvElemStart(_, tag, attrs, _) => {
            if (tag == separatorTag) {
              insidePage = true
            }
            if (insidePage) {
              path.down(tag)

              handleContainer(path.schema)

              def handleContainer(theSchema: Schema): Unit =
                theSchema.getType match {
                  case Schema.Type.ARRAY => {

                    if (!objStack.isEmpty && objStack.top.field != tag) {
                      objStack.push(AvroObject(new util.ArrayList[Object](), true, tag, theSchema, path.copy))
                    }

                    if (theSchema.getElementType.getType == Schema.Type.RECORD) {
                      val record = new GenericData.Record(theSchema.getElementType)
                      setAttributes(attrs, record)
                      objStack.push(AvroObject(record, false, tag, theSchema, path.copy))

                    }
                  }
                  case Schema.Type.RECORD => {
                    val record = new GenericData.Record(theSchema)
                    setAttributes(attrs, record)
                    objStack.push(AvroObject(record, false, tag, theSchema, path.copy))
                  }
                  case Schema.Type.UNION => {
                    val utype = theSchema.getTypes.filter(e => e.getType != Schema.Type.NULL)(0)
                    handleContainer(utype)
                  }
                  case _ =>
                }
            }
          }
          case EvElemEnd(_, tag) => {
            if (tag == separatorTag) {
              handleEndTag(tag)

              path.reset

              insidePage = false
              break
            }
            if (insidePage) {
              handleEndTag(tag)
              path.up(tag)
              elementText.clear()
            }
          }

          case EvText(text) => {
            if (insidePage) {
              elementText.append(text)
            }
          }
          case _ =>
        }
      }
    }
    if (!objStack.isEmpty) Some(objStack.top.obj.asInstanceOf[GenericData.Record]) else None

  }

  def avroObject(obj: Object, s: Schema) = {
    obj match {
      case _: GenericData.Record => obj.asInstanceOf[GenericData.Record]
      case _: util.ArrayList[_] => new GenericData.Array[Object](s,
        obj.asInstanceOf[util.ArrayList[Object]])
    }
  }

  def setAttributes(metedata: MetaData, rec: GenericData.Record) = {
    metedata.asAttrMap.foreach {
      a =>
        pathToSchemaMap.get(path + "." + a._1) match {
          case Some(s) => setRecordFieldValue(rec, a._1, a._2, s)
          case None => println("oopsie..." + path + "." + a._1)
        }
    }
  }

  def setRecordFieldValue(rec: GenericData.Record, fieldName: String, fieldVal: String, s: Schema) = convert(s, fieldVal, o => rec.put(fieldName, o))

  def addToDataArray(array: util.ArrayList[Object], fieldVal: String, s: Schema) = convert(s, fieldVal, o => array.add(o))

  def convert(s: Schema, fieldVal: String, f: Object => Unit): Unit = {
    s.getType match {
      case Schema.Type.BOOLEAN => f(JBoolean.valueOf(fieldVal))
      case Schema.Type.INT => f(JInteger.valueOf(fieldVal))
      case Schema.Type.LONG => f(JLong.valueOf(fieldVal))
      case Schema.Type.FLOAT => f(JFloat.valueOf(fieldVal))
      case Schema.Type.DOUBLE => f(JDouble.valueOf(fieldVal))
      case Schema.Type.STRING => f(fieldVal)
      case Schema.Type.ENUM => f(fieldVal)
      case Schema.Type.UNION =>
        val utype = s.getTypes.filter(e => e.getType != Schema.Type.NULL)(0)
        convert(utype, fieldVal, f)
      case _ =>
    }
  }

  def setValue(container: Object, field: String, value: Object): Unit = {
    if (separatorTag != separatorTag) {
      if (container.isInstanceOf[GenericData.Record])
        container.asInstanceOf[GenericData.Record].put(field, value)
      else
        container.asInstanceOf[GenericData.Array[Object]].add(value)
    }
  }

  def pathToSchemaMapper(path: String, s: Schema): Unit = {

    if (!pathToSchemaMap.contains(path)) pathToSchemaMap += (path -> s)
    s.getType match {
      case Schema.Type.RECORD =>
        val fields = s.getFields
        for (f <- fields) {
          pathToSchemaMapper(path + "." + f.name(), f.schema())
        }
      case Schema.Type.UNION =>
        val utype = s.getTypes.filter(e => e.getType != Schema.Type.NULL)(0)
        pathToSchemaMapper(path, utype)

      case Schema.Type.ARRAY =>
        pathToSchemaMapper(path, s.getElementType)
      case _ =>

    }
  }

  class Path(val sb: StringBuilder) {
    def this() = this(new StringBuilder)

    def up(s: String): Unit = {
      sb.setLength(sb.lastIndexOf('.'))
    }

    def down(s: String) = {
      sb.append(".").append(s)
    }

    def schema = {
      pathToSchemaMap(path.toString())
    }

    def schemaType = {
      pathToSchemaMap(path.toString()).getType
    }

    def print = println("Path: " + sb.toString())

    def printSchema = println("Schema: " + pathToSchemaMap(path.toString()))

    def isParentOf(other: Path) = {
      val p = other.sb.lastIndexOf('.')
      if (p == -1) false
      else
        sb.toString == other.sb.substring(0, p).toString
    }

    def isPeer(other: StringBuilder) = {
      val p1 = sb.lastIndexOf('.')
      val p2 = other.lastIndexOf('.')

      if (sb == other || p1 == -1 || p2 == -1) false
      else
        sb.substring(0, p1).toString == other.substring(0, p2).toString
    }

    def copy: Path = {
      new Path(sb.clone())
    }
    def reset = sb.clear()

    override def toString: String = sb.toString()
  }

  case class AvroObject(obj: Object, isArray: Boolean, field: String, schema: Schema, path: Path) {
    override def toString(): String = {
      s"path=${path.toString()}, objType: ${obj.getClass.getName}"
    }
  }

}

