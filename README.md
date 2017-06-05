# xml-hive
Hive Serde for XML data. This serde maps XML schema to table schema. The implementation piggy back on the Hive's built-in avro serde. The XML schema is first trnslated into Avro schema and the data reader maps XML records to Avro.

## Usage
