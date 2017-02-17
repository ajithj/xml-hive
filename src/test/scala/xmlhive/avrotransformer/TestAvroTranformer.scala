package xmlhive.avrotransformer

import java.io.{ByteArrayOutputStream, InputStream, StringReader}

import com.github.xmlavrobridge.{AvroTransormer, XSDToAvroSchemaConverter, XmlAvroHelper}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.DatumWriter
import org.apache.commons.io.input.ReaderInputStream
import org.apache.hadoop.conf.Configuration
import org.scalatest.{FlatSpec, Matchers}
import util.TestUtil
/**
  * Created by ajoseph on 12/31/16.
  */
class TestAvroTranformer extends FlatSpec with Matchers {

  val venitianBlindNS = "http://schemas.sun.com/point/venetianblind"

  "AvroTransformer" should "transform xml elements to avro records" in {
    runTest("testdata1", "book", "bookType", "catalogType", "")

  }

  "AvroTransformer" should "transform xml to avro records with Venetian Blind schema " in {
    runTest("testdata2", "Member", "MemberType", "MembersType", venitianBlindNS)

  }

  "AvroTransformer" should "transform schema with nested simpleType " in {
    runTest("testdata3", "Member", "MemberType", "MembersType", venitianBlindNS)

  }

  def runTest(testDir: String, separatorTag: String, separatorTagType: String, rootTagType: String, tns: String) = {
    val in = getClass.getResourceAsStream(s"/$testDir/data/data.xml")
    val conf = new Configuration()
    conf.set("xml.separator.tag", separatorTag)
    conf.set("xml.separator.tag.type", separatorTagType)

    val converter = new XSDToAvroSchemaConverter() {
      override val readFile: (String) => InputStream = TestUtil.openResourceFile
      override val listDirectory: (String) => List[String] = TestUtil.resourceFiles
    }
    val schemaJson = converter.generateAvroSchema(s"/$testDir/schema", tns, rootTagType, "com.example")

    val schema = new Schema.Parser().parse(new ReaderInputStream(new StringReader(schemaJson)))
    val recordSchema = XmlAvroHelper.recordSchema(separatorTagType, schema).get

    val avroTransformer = new AvroTransormer(in, conf, schema)

    val datumWriter: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](recordSchema);
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter);
    val outBuffer = new ByteArrayOutputStream
    dataFileWriter.create(recordSchema, outBuffer);

    println("Schema: " + recordSchema)
    Iterator.continually(avroTransformer.nextRecord).takeWhile(_.nonEmpty).foreach(
      e => {
        println("Next record returned: " + e)
        dataFileWriter.append(e.get)
      }

    )
    dataFileWriter.close()
    outBuffer.close()

  }
}
