package com.github.xmlavrobridge

import java.io.{InputStream, StringReader}
import java.net.URI
import java.util
import java.util.Stack

import scala.collection.mutable

//import input.xmlhive.XmlSchemaParser
import org.apache.avro.Schema
import org.apache.commons.io.input.ReaderInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.JavaConversions._

/**
  * Created by ajoseph on 6/25/16.
  */
object XmlAvroHelper extends HFSReader {

  def recordSchemaOld(sepTag: String, rootSchema: Schema) = {
    val st: Option[Schema] = None

    def find(s: Schema): Option[Schema] = {

      val schemaOpt = s.getType match {
        case Schema.Type.RECORD => Some(s)
        case Schema.Type.ARRAY if s.getElementType == Schema.Type.RECORD => Some(s.getElementType)
        case _ => None
      }
      if (schemaOpt.isEmpty) None
      else if (sepTag == s.getName) schemaOpt
      else {
        schemaOpt.get.getFields.foldLeft(st) {
          (b, a) => {
            if (b.isDefined) b else find(a.schema())
          }
        }
      }
    }
    find(rootSchema)
  }

  def recordSchema(sepTag: String, rootSchema: Schema) = {

    def schema(s: Schema): Option[Schema] = s.getType match {
      case Schema.Type.RECORD => Some(s)
      case Schema.Type.UNION => {
        val utype = s.getTypes.filter(s => s.getType != Schema.Type.NULL)(0)
        schema(utype)
      }
      case Schema.Type.ARRAY if s.getElementType.getType == Schema.Type.RECORD => Some(s.getElementType)
      case _ => None
    }

    findNode[Schema](rootSchema, (s) => s.getName == sepTag, (s) => {
      s.getFields.flatMap {
        f =>
          schema(f.schema())
      }
    })
  }

  def findNode[T](n: T, matchNode: T => Boolean, listChildren: T => Traversable[T]) = {
    var node: Option[T] = None
    findNode0(n)
    def findNode0(n0: T): Option[T] = {
      node = if (matchNode(n0)) {
        Some(n0)
      } else {
        listChildren(n0)
          .foldLeft(node) {
            (a, b) => {
              val oNode = findNode0(b)
              a match {
                case Some(_) => a
                case _ => oNode
              }
            }
          }
      }
      node
    }
    node
  }

  def schema(conf: Configuration) = {

    val fs = FileSystem.get(conf)

    val schemaConverter = new XSDToAvroSchemaConverter() {
      override val readFile: (String) => InputStream = openFile
      override val listDirectory: (String) => List[String] = listFiles
    }

    val separatorTagType = conf.get("xml.separator.tag.type")
    val separatorTagTypeNs = Option(conf.get("xml.separator.tag.type.ns")).getOrElse("")
    val xmlSchemaLocation = conf.get("xml.schema.location")

    val schemaStr = schemaConverter.generateAvroSchema(xmlSchemaLocation,
       separatorTagTypeNs, separatorTagType, "example.com")


    val s = new Schema.Parser().parse(new ReaderInputStream(new StringReader(schemaStr)))
    recordSchema(separatorTagType, s).get
  }

}
