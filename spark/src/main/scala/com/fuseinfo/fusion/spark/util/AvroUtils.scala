/*
 * Copyright (c) 2018 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.fuseinfo.fusion.spark.util

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.LocalDate

import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object AvroUtils {
  case class SchemaType(dataType: DataType, nullable: Boolean)

  def fromDays(days:Int): Date = java.sql.Date.valueOf(LocalDate.ofEpochDay(days.toInt))

  def fromMilliSecs(milliSecs: Long) = new java.sql.Timestamp(milliSecs)

  def fromMicroSecs(microSecs: Long): Timestamp = {
    val ts = new java.sql.Timestamp(microSecs / 1000)
    ts.setNanos((microSecs % 1000000 * 1000).toInt)
    ts
  }

  def toEpochMicroSecs(ts: java.sql.Timestamp): Long = ts.getTime * 1000 + (ts.getNanos / 1000) % 1000

  def createConverterToSQL(sourceSchema:Schema, targetSqlType:DataType): AnyRef => AnyRef = {
    def createConverter(avroSchema: Schema, sqlType: DataType, path:List[String]): AnyRef => AnyRef = {
      val avroType = avroSchema.getType
      (sqlType, avroType) match {
        case (StringType, STRING) | (StringType, ENUM) =>
          (item: AnyRef) => if (item == null) null else item.toString
        case (IntegerType, INT) | (BooleanType, BOOLEAN) | (DoubleType, DOUBLE)   |
             (FloatType, FLOAT) | (LongType, LONG) => identity
        case (BinaryType, FIXED) =>
          (item: AnyRef) => if (item == null) null else item.asInstanceOf[GenericData.Fixed].bytes.clone
        case (BinaryType, BYTES) =>
          (item: AnyRef) => if (item == null) null else item.asInstanceOf[ByteBuffer].array
        case (decimalType:DecimalType, BYTES) =>
          (item: AnyRef) => if (item == null) null else
            new java.math.BigDecimal(new java.math.BigInteger(item.asInstanceOf[ByteBuffer].array), decimalType.scale)
        case (DateType, INT) =>
          (item: AnyRef) => if (item == null) null else fromDays(item.asInstanceOf[Integer])
        case (TimestampType, LONG) =>
          (item: AnyRef) => if (item == null || avroSchema.getLogicalType == null) null else {
            val ts = item.asInstanceOf[Long]
            if (avroSchema.getLogicalType.getName == "timestamp-millis") fromMilliSecs(ts) else fromMicroSecs(ts)
          }
        case (TimestampType, INT) =>
          (item: AnyRef) => if (item == null) null else fromMilliSecs(item.asInstanceOf[Integer].toLong)
        case (arrayType:ArrayType, ARRAY) =>
          val elementConverter = createConverter(avroSchema.getElementType, arrayType.elementType, path)
          (item: AnyRef) => if (item == null) null else
            item.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map(element =>
              if (element == null && !arrayType.containsNull)
                throw new RuntimeException(s"Array value at path ${path.mkString(".")} can not be null")
              else elementConverter(element)
            )
        case (mapType: MapType, MAP) if mapType.keyType == StringType =>
          val valueConverter = createConverter(avroSchema.getValueType, mapType.valueType, path)
          (item: AnyRef) => if (item == null) null else
            item.asInstanceOf[java.util.Map[AnyRef, AnyRef]].asScala.map(x =>
              if (x._2 == null && !mapType.valueContainsNull)
                throw new RuntimeException(s"Map value at path ${path.mkString(".")} can not be null")
              else (x._1.toString, valueConverter(x._2))
            ).toMap
        case (struct: StructType, RECORD) =>
          val length = struct.fields.length
          val converters = new Array[AnyRef => AnyRef](length)
          val fieldIdx = new Array[Int](length)
          for (i <- 0 until length) {
            val sqlField = struct.fields(i)
            val avroField = avroSchema.getField(sqlField.name)
            if (avroField != null) {
              val converter = createConverter(avroField.schema, sqlField.dataType, path :+ sqlField.name)
              converters(i) = converter
              fieldIdx(i) = avroField.pos
            } else if (!sqlField.nullable)
              throw new RuntimeException(s"Value ${sqlField.name} at path ${path.mkString(".")} can not be null")
          }
          (item: AnyRef) => if (item == null) null else {
            val result = new Array[Any](length)
            for (i <- 0 until length) {
              if (converters(i) != null) result(i) = converters(i)(item.asInstanceOf[GenericRecord].get(fieldIdx(i)))
            }
            new GenericRow(result)
          }
        case (sqlType, UNION) =>
          if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
            val restTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
            if (restTypes.size == 1) createConverter(restTypes.head, sqlType, path)
            else createConverter(Schema.createUnion(restTypes.asJava), sqlType, path)
          } else avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(t1) => createConverter(avroSchema.getTypes.get(0), sqlType, path)
            case Seq(a, b) if Set(a, b) == Set(INT, LONG) && sqlType == LongType =>
              (item: AnyRef) => item match {
                case null => null
                case longValue: java.lang.Long => longValue
                case intValue: java.lang.Integer => new java.lang.Long(intValue.longValue)
              }
            case Seq(a, b) if Set(a, b) == Set(DOUBLE, FLOAT) && sqlType == DoubleType =>
              (item: AnyRef) => item match {
                case null => null
                case doubleValue: java.lang.Double => doubleValue
                case floatValue: java.lang.Float => new java.lang.Double(floatValue.doubleValue)
              }
            case other => sqlType match {
              case t: StructType if t.fields.length == avroSchema.getTypes.size =>
                val converters = t.fields.zip(avroSchema.getTypes.asScala).map{case (f, s) =>
                  createConverter(s, f.dataType, path :+ f.name)
                }
                (item: AnyRef) => if (item == null) null else {
                  val i = GenericData.get.resolveUnion(avroSchema, item)
                  val converted = new Array[Any](converters.length)
                  converted(i) = converters(i)(item)
                  new GenericRow(converted)
                }
              case _ => throw new RuntimeException(
                s"Incompatible at ${path.mkString(".")}: from $other in $sourceSchema to $sqlType in $targetSqlType")
            }
          }
        case (left, right) => throw new RuntimeException(
          s"Incompatible at ${path.mkString(".")}: from $left in $sourceSchema to $right in $targetSqlType")
      }
    }
    createConverter(sourceSchema, targetSqlType, List.empty[String])
  }

  def toSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(ArrayType(schemaType.dataType, schemaType.nullable), false)
      case BOOLEAN => SchemaType(BooleanType, false)
      case BYTES =>
        avroSchema.getLogicalType match {
          case decimal:LogicalTypes.Decimal => SchemaType(DecimalType(decimal.getPrecision, decimal.getScale), false)
          case _ => SchemaType(BinaryType, false)
        }
      case DOUBLE => SchemaType(DoubleType, false)
      case ENUM => SchemaType(StringType, false)
      case FIXED =>
        avroSchema.getLogicalType match {
          case decimal:LogicalTypes.Decimal => SchemaType(DecimalType(decimal.getPrecision, decimal.getScale), false)
          case _ => SchemaType(BinaryType, false)
        }
      case FLOAT => SchemaType(FloatType, false)
      case INT =>
        avroSchema.getLogicalType match {
          case _:LogicalTypes.Date => SchemaType(DateType, false)
          case _:LogicalTypes.TimeMillis => SchemaType(TimestampType, false)
          case _ => SchemaType(IntegerType, false)
        }
      case LONG =>
        avroSchema.getLogicalType match {
          case _:LogicalTypes.TimestampMillis|_:LogicalTypes.TimestampMicros => SchemaType(TimestampType, false)
          case _:LogicalTypes.TimeMicros => SchemaType(TimestampType, false)
          case _ => SchemaType(LongType, false)
        }
      case MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(MapType(StringType, schemaType.dataType, schemaType.nullable), false)
      case RECORD =>
        val fields = avroSchema.getFields.asScala.map{f =>
          val schemaType = toSqlType(f.schema)
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }
        SchemaType(StructType(fields), false)
      case STRING => SchemaType(StringType, false)
      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          val restTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (restTypes.size == 1) toSqlType(restTypes.head).copy(nullable = true)
          else toSqlType(Schema.createUnion(restTypes.asJava)).copy(nullable = true)
        } else avroSchema.getTypes.asScala.map(_.getType) match {
          case Seq(t1) => toSqlType(avroSchema.getTypes.get(0))
          case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) => SchemaType(LongType, false)
          case Seq(t1, t2) if Set(t1, t2) == Set(DOUBLE, FLOAT) => SchemaType(DoubleType, false)
          case _ =>
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (st, idx) => StructField(s"member$idx", toSqlType(st).dataType, true)
            }
            SchemaType(StructType(fields), false)
        }
      case _ => SchemaType(NullType, true)
    }
  }

  val timestampMilliType = LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG))
  val timestampMicroType = LogicalTypes.timestampMicros.addToSchema(Schema.create(Schema.Type.LONG))
  val dateType = LogicalTypes.date.addToSchema(Schema.create(Schema.Type.INT))
  def decimalType(prec:Int) = LogicalTypes.decimal(prec).addToSchema(Schema.create(Schema.Type.BYTES))
  def decimalType(prec:Int, scale:Int) = LogicalTypes.decimal(prec, scale).addToSchema(Schema.create(Schema.Type.BYTES))

  def setNull[S](builder:SchemaBuilder.FieldTypeBuilder[S], nullable:Boolean) =
    if (nullable) builder.nullable else builder

  def setNull[S](builder:SchemaBuilder.TypeBuilder[_ <: SchemaBuilder.FieldDefault[S, _]], containsNull:Boolean) =
    if (containsNull) builder.nullable else builder

  def setType[S](builder:SchemaBuilder.BaseTypeBuilder[_ <: SchemaBuilder.FieldDefault[S, _]],
                 dataType: DataType, fieldName: String, path: String):SchemaBuilder.FieldDefault[S, _] = {
    dataType match {
      case _:BinaryType => builder.bytesType
      case _:BooleanType => builder.booleanType
      case _:ByteType => builder.intType
      case _:FloatType => builder.floatType
      case _:DoubleType => builder.doubleType
      case _:IntegerType => builder.intType
      case _:LongType => builder.longType
      case _:ShortType => builder.intType
      case _:StringType => builder.stringType
      case _:DateType => builder.`type`(dateType)
      case _:TimestampType => builder.`type`(timestampMicroType)
      case item:DecimalType => builder.`type`(decimalType(item.precision, item.scale))
      case item:ArrayType =>
        val array = setNull(builder.array.items, item.containsNull)
        setType(array, item.elementType, fieldName, path)
      case item:MapType =>
        val map = setNull(builder.map.values, item.valueContainsNull)
        setType(map, item.valueType, fieldName, path)
      case item:StructType =>
        val recordName = path + fieldName
        val record = builder.record(recordName).fields
        item.fields.foreach(subType => assembleField(record, subType, recordName + "_"))
        record.endRecord
    }
  }

  def assembleField[S](assembler: SchemaBuilder.FieldAssembler[S], sf: StructField, path:String):SchemaBuilder.FieldAssembler[S] = {
    val fieldBuilder = assembler.name(sf.name)
    sf.dataType match {
      case _:BinaryType => setNull(fieldBuilder.`type`, sf.nullable).bytesType.noDefault
      case _:BooleanType => setNull(fieldBuilder.`type`, sf == null).booleanType.noDefault
      case _:ByteType => setNull(fieldBuilder.`type`, sf == null).intType.noDefault
      case _:DoubleType => setNull(fieldBuilder.`type`, sf == null).doubleType.noDefault
      case _:FloatType => setNull(fieldBuilder.`type`, sf == null).floatType.noDefault
      case _:IntegerType => setNull(fieldBuilder.`type`, sf == null).intType.noDefault
      case _:LongType => setNull(fieldBuilder.`type`, sf == null).longType.noDefault
      case _:ShortType => setNull(fieldBuilder.`type`, sf == null).intType.noDefault
      case _:StringType => setNull(fieldBuilder.`type`, sf == null).stringType.noDefault
      case _:DateType =>
        if (sf.nullable) fieldBuilder.`type`.optional.`type`(dateType)
        else fieldBuilder.`type`(dateType).noDefault
      case _:TimestampType =>
        if (sf.nullable) fieldBuilder.`type`.optional.`type`(timestampMicroType)
        else fieldBuilder.`type`(timestampMicroType).noDefault
      case item:DecimalType =>
        if (sf.nullable) fieldBuilder.`type`.optional.`type`(decimalType(item.precision, item.scale))
        else fieldBuilder.`type`(decimalType(item.precision, item.scale)).noDefault
      case item:ArrayType =>
        val base = fieldBuilder.`type`
        val array = (if (sf.nullable) base.nullable else base).array.items
        val builder = setNull(array, item.containsNull)
        setType(builder, item.elementType, sf.name, path).noDefault
      case item: MapType =>
        val base = fieldBuilder.`type`
        val map = (if (sf.nullable) base.nullable else base).map.values
        val builder = setNull(map, item.valueContainsNull)
        setType(builder, item.valueType, sf.name, path).noDefault
      case item: StructType =>
        val base = fieldBuilder.`type`
        val recordName = path + sf.name
        val record = (if (sf.nullable) base.nullable else base).record(recordName).fields
        item.fields.foreach(subType => assembleField(record, subType, recordName + "_" ))
        record.endRecord.noDefault
    }
  }

  def toAvroSchema(tableName:String, struct:Array[StructField]): Schema = {
    val assembler = SchemaBuilder.record(tableName).fields
    struct.foreach(sqlType => assembleField(assembler, sqlType, ""))
    assembler.endRecord
  }
}
