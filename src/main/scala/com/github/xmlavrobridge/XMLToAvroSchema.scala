package com.github.xmlavrobridge

import java.io.{File, InputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.map.JsonMappingException
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.ObjectNode

import scala.xml.{Elem, Node, NodeSeq, XML}
import scala.collection.mutable.Map

case class Attribute(name: String, ns: String, ttype: String, required: Boolean)

case class Element(name: String, ns: String, ttype: String, minOccurs: String, maxOccurs: String)

case class ComplexType(name: String, ns: String, elements: List[Element], attributes: List[Attribute])

case class SimpleType(name: String, ns: String, baseTypeNs: String, base: String, enumVals: List[String])

/**
  * Created by ajoseph on 6/25/16.
  */
trait XSDToAvroSchemaConverter {

  val xsdNS = "http://www.w3.org/2001/XMLSchema"
  val complexTypesMap = Map[(String, String), ComplexType]()
  val simpleTypesMap = Map[(String, String), SimpleType]()
  val elementToType = Map[String, String]()

  val primitivesMap = Map[(String, String), String](
    (xsdNS, "string") -> "string",
    (xsdNS, "int") -> "int",
    (xsdNS, "long") -> "long",
    (xsdNS, "boolean") -> "boolean",
    (xsdNS, "decimal") -> "double",
    (xsdNS, "float") -> "float",
    (xsdNS, "double") -> "double",
    (xsdNS, "dateTime") -> "string",
    (xsdNS, "date") -> "string",
    (xsdNS, "time") -> "string"
  )

  val readFile: (String) => InputStream
  val listDirectory: (String) => List[String]

  def nameSpaces(e: Elem) = {
    e.attributes.asAttrMap.map {
      x =>
        val h :: t = x._1.split(":").toList
        (h, t, x._2)
    }.filter {
      v =>
        v._2.nonEmpty && v._1 == "xmlns"
    }.map {
      v => (v._1 -> v._3)
    }.toMap
  }

  def generateAvroSchema(xsdPath: String, rootElementNS: String, rootElement: String, ns: String) = {

    parseXmlSchema(xsdPath)
    val mapper = new ObjectMapper()
    def enumSchema(st: SimpleType): ObjectNode = {
      val rootNode = mapper.createObjectNode()

      rootNode.put("type", xsdToAvro(st.base))
      rootNode.put("name", st.name)
      val symbols = mapper.createArrayNode()
      st.enumVals.foreach {
        e =>
          symbols.add(e)
      }
      rootNode.put("symbols", symbols)
      rootNode
    }

    def buildSchemaTree(rootObj: ComplexType): ObjectNode = {

      val rootNode = mapper.createObjectNode()

      rootNode.put("name", rootObj.name)
      rootNode.put("type", "record")

      val fields = mapper.createArrayNode()
      rootNode.put("fields", fields)
      for (attr <- rootObj.attributes) {
        val fieldObj = mapper.createObjectNode()
        fieldObj.put("name", attr.name)
        val (primitive, ttype) = attr.ttype match {
          case "string" | "int" | "long" | "boolean" | "decimal" | "float" | "double" | "dateTime" | "date" | "time" =>
            (true, xsdToAvro(attr.ttype))
          case _ =>
            (false, attr.ttype)
        }
        if (attr.required) {
          if (primitive)
            fieldObj.put("type", ttype)
          else
            fieldObj.put("type", enumSchema(simpleTypesMap((attr.ns, attr.ttype))))
        } else {
          val union = mapper.createArrayNode()
          if (primitive)
            union.add(ttype)
          else
            union.add(enumSchema(simpleTypesMap((attr.ns, attr.ttype))))
          union.add("null")
          fieldObj.put("type", union)
        }
        fields.add(fieldObj)
      }

      for (e <- rootObj.elements) {
        val fieldObj = mapper.createObjectNode()
        fieldObj.put("name", e.name)

        val optional = e.minOccurs match {
          case "0" => true
          case "1" => false
        }

        e.ttype match {
          case "string" | "int" | "long" | "boolean" | "decimal" | "float" | "double" | "dateTime" | "date" | "time" =>
            e.maxOccurs match {
              case "1" =>
                //scalar
                if (optional) {
                  val union = mapper.createArrayNode()
                  union.add(xsdToAvro(e.ttype))
                  union.add("null")
                  fieldObj.put("type", union)
                } else {
                  fieldObj.put("type", xsdToAvro(e.ttype))
                }
              case _ =>
                //array
                if (optional) {
                  val arrayType = mapper.createObjectNode()
                  arrayType.put("type", "array")
                  arrayType.put("items", xsdToAvro(e.ttype));

                  val union = mapper.createArrayNode()
                  union.add(arrayType)
                  union.add("null")
                  fieldObj.put("type", union)
                } else {
                  val arrayType = mapper.createObjectNode()
                  arrayType.put("type", "array")
                  arrayType.put("items", xsdToAvro(e.ttype));
                  fieldObj.put("type", arrayType)
                }

            }
          case _ =>
            //simple or complex types
            val childSchema = if (complexTypesMap.contains((e.ns, e.ttype)))
              buildSchemaTree(complexTypesMap(e.ns, e.ttype))
            else
              enumSchema(simpleTypesMap(e.ns, e.ttype))

            e.maxOccurs match {
              case "1" =>
                //scalar
                if (optional) {
                  val union = mapper.createArrayNode()
                  union.add(childSchema)
                  union.add("null")
                  fieldObj.put("type", union)
                } else {
                  fieldObj.put("type", childSchema)
                }
              case _ =>
                //array
                if (optional) {
                  val arrayNode = mapper.createObjectNode()
                  arrayNode.put("type", "array")
                  arrayNode.put("items", childSchema)
                  val union = mapper.createArrayNode()
                  union.add(arrayNode)
                  union.add("null")
                  fieldObj.put("type", union)
                } else {
                  val arrayType = mapper.createObjectNode()
                  arrayType.put("type", "array")
                  arrayType.put("items", childSchema)
                  fieldObj.put("type", arrayType)
                }
            }
        }
        fields.add(fieldObj)
      }
      rootNode
    }

    val schemaTree = buildSchemaTree(complexTypesMap(rootElementNS, rootElement))
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schemaTree)
  }

  private[this] def parseXmlSchema(xsdPath: String) = {

    listDirectory(xsdPath).foreach {
      file =>
        val rootElement = XML.load(readFile(file))
        val targetNamespace = attr(rootElement, "targetNamespace").getOrElse("")

        val elementsAndTypes = parseElements(rootElement, targetNamespace)

        complexTypesMap ++= parseComplexTypes(rootElement, None, targetNamespace) ++=
          elementsAndTypes.flatMap{e => e._3}.map {e => (e.ns, e.name) -> e}

        simpleTypesMap ++= parseSimpleTypes(rootElement, None, targetNamespace) ++=
          elementsAndTypes.flatMap{e => e._2}.map {e => (e.baseTypeNs, e.name) -> e}
    }
  }

  def parseSimpleTypes(rootElement: Elem, typeName: Option[String], targetNamespace: String) = (rootElement \ "simpleType").flatMap {
    st =>
      val stName = attr(st, "name").getOrElse(typeName.getOrElse(""))
      val restrictions = st \ "restriction"
      val res = restrictions.flatMap {
        r =>
          val base = attr(r, "base")
          if (base.isDefined) {
            val enumValues = (r \ "enumeration").map {
              e =>
                attr(e, "value").getOrElse("")
            }.toList
            val (nsOpt, b) = splitType(base.get)
            val ns = if (nsOpt.isDefined) r.getNamespace(nsOpt.get) else r.namespace
            Some(((targetNamespace, stName) -> SimpleType(stName, targetNamespace, ns, b, enumValues)))
          } else {
            None
          }
      }
      res
  }.toMap

  def parseComplexTypes(rootElement: Elem, typeName: Option[String], targetNamespace: String) = (rootElement \ "complexType").map {
    ct =>
      val ctName = attr(ct, "name").getOrElse(typeName.getOrElse(""))

      val elmsAndTypes = parseElements((ct \ "sequence"), targetNamespace) ++
        parseElements((ct \ "sequence" \ "sequence"), targetNamespace) ++
        parseElements((ct \ "all"), targetNamespace)

      complexTypesMap ++= elmsAndTypes.flatMap{e => e._3}.map {e => (e.ns, e.name) -> e}
      simpleTypesMap ++= elmsAndTypes.flatMap{e => e._2}.map {e => (e.ns, e.name) -> e}

      val elms = elmsAndTypes.map{e => e._1}

      val stExt = ct \ "simpleContent" \ "extension"

      def attributes(n: Node) = (n \ "attribute").map {
        attribute =>
          val name = attr(attribute, "name").getOrElse("")
          val ttype = attr(attribute, "type").getOrElse("")
          val use = attr(attribute, "use").getOrElse("")
          val (nsOpt, t) = splitType(ttype)
          val ns = if (nsOpt.isDefined) attribute.getNamespace(nsOpt.get) else targetNamespace
          Attribute(name, ns, t, use == "required")
      }.toList

      if (stExt.nonEmpty) {
        val base = attr(stExt(0), "base").getOrElse("")
        val (nsOpt, b) = splitType(base)
        val ns = if (nsOpt.isDefined) stExt(0).getNamespace(nsOpt.get) else targetNamespace
        ((targetNamespace, ctName) -> ComplexType(ctName, targetNamespace, Element("_Value", ns, b, "1", "1") :: elms, attributes(stExt(0))))
      } else
        ((targetNamespace, ctName) -> ComplexType(ctName, targetNamespace, elms, attributes(ct)))
  }.toMap

  def parseElements(seq: NodeSeq, tns: String) = (seq \ "element").map {
    elm =>
      parseElement(elm, Some(seq(0)), tns)
  }.toList

  def parseElement(elm: Node, seq: Option[Node], tns: String): (Element, Option[SimpleType], Option[ComplexType]) = {
    val name = attr(elm, "name").getOrElse("")

    val (ttype, ctOpt, stOpt) = attr(elm, "type") match {
      case Some(s) => (s, None, None)
      case None =>

        val genName = name + "Type"

        val ctMap = parseComplexTypes(elm.asInstanceOf[Elem], Some(genName), tns)
        if(ctMap.isEmpty) {
          val bar = parseSimpleTypes(elm.asInstanceOf[Elem], Some(genName), tns)
          (genName, if (!bar.isEmpty) Some(bar.head._2) else None, None)
        }else {
          (genName, None, Some(ctMap.head._2))
        }
    }

    def cardinality(cAttr: String): String = attr(elm, cAttr) match {
      case Some(s) => s
      case None => seq match {
        case Some(n) => attr(n, cAttr).getOrElse("1")
        case None => "1"
      }
    }

    val minOccurs = cardinality("minOccurs")

    val maxOccurs = cardinality("maxOccurs")

    val (nsOpt, typeStr) = splitType(ttype)
    val ns = if (nsOpt.isDefined) elm.getNamespace(nsOpt.get) else tns
    (Element(name, ns, typeStr, minOccurs, maxOccurs), ctOpt, stOpt)
  }


  def splitType(s: String) = {
    val head :: tail = s.split(":").toList
    if (tail.isEmpty) (None, head) else (Some(head), tail(0))
  }

  def attr(n: Node, a: String) = {
    val ns = n \ s"@$a"
    if (ns.length > 0) Some(ns(0).text)
    else None
  }

  // TODO: add few missing types
  // TODO: https://msdn.microsoft.com/en-us/library/ms256050(v=vs.110).aspx -- Handle union type
  //
  def xsdToAvro(xsdType: String): String = {
    if (xsdType.endsWith("string")) "string"
    else if (xsdType.endsWith("int")) "int"
    else if (xsdType.endsWith("integer")) "int"
    else if (xsdType.endsWith("long")) "long"
    else if (xsdType.endsWith("boolean")) "boolean"
    else if (xsdType.endsWith("decimal")) "double"
    else if (xsdType.endsWith("float")) "float"
    else if (xsdType.endsWith("double")) "double"
    else if (xsdType.endsWith("dateTime")) "string"
    else if (xsdType.endsWith("date")) "string"
    else if (xsdType.endsWith("time")) "string"
    else s"$xsdType"

  }

}
