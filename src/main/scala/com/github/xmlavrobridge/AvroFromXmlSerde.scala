package com.github.xmlavrobridge

import java.util
import java.util.Properties

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde2.avro.{AvroSerDe, AvroSerdeUtils}

/**
  * Created by ajoseph on 7/11/16.
  */
class AvroFromXmlSerde extends AvroSerDe {

  override def determineSchemaOrReturnErrorSchema(conf: Configuration, props: Properties): Schema = {
    XmlAvroHelper.schema(conf)
  }

  override def initialize(configuration: Configuration, properties: Properties): Unit = {
    properties.put(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName, "dummypath")
    List("xml.separator.tag", "xml.separator.tag.type", "xml.separator.tag.type.ns", "xml.schema.location").foreach {
      p=> if(properties.containsKey(p)) configuration.set(p, properties.get(p).toString)
    }
    super.initialize(configuration, properties)
  }
}
