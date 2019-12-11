package com.assignment.three

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Parser extends Serializable {

  def parse(msg:String):Row={
    val message=msg.split(",")
    System.out.println(message.length)
    Row(message(0), message(1), message(2), message(3), message(4), message(5), message(6),
      message(7), message(8), message(9), message(10), message(11), message(12), message(13), message(14), message(15), message(16), message(17), message(18), message(19), message(20), message(21), message(22), message(23),
      message(24), message(25), message(26))
  }

  def schema() ={
    StructType(Seq(
      StructField("start_date", StringType, true),
      StructField("start_station", StringType, true),
      StructField("end_date", StringType, true),
      StructField("end_station", StringType, true),
      StructField("is_member", StringType, true),
      StructField("start__stations__station_id", StringType, true),
      StructField("start_data__stations__external_id", StringType, true),
      StructField("start_data__stations__name", StringType, true),
      StructField("start_data__stations__short_name", StringType, true),
      StructField("start_data__stations__lat", StringType, true),
      StructField("start_data__stations__lon", StringType, true),
      StructField("start_data__stations__rental_methods__001", StringType, true),
      StructField("start_data__stations__rental_methods__002", StringType, true),
      StructField("start_data__stations__capacity", StringType, true),
      StructField("start_data__stations__electric_bike_surcharge_waiver", StringType, true),
      StructField("start_data__stations__eightd_has_key_dispenser", StringType, true),
      StructField("end__stations__station_id", StringType, true),
      StructField("end_data__stations__external_id", StringType, true),
      StructField("end_data__stations__name", StringType, true),
      StructField("end_data__stations__short_name", StringType, true),
      StructField("end_data__stations__lat", StringType, true),
      StructField("end_data__stations__lon", StringType, true),
      StructField("end_data__stations__rental_methods__001", StringType, true),
      StructField("end_data__stations__rental_methods__002", StringType, true),
      StructField("end_data__stations__capacity", StringType, true),
      StructField("end_data__stations__electric_bike_surcharge_waiver", StringType, true),
      StructField("end_data__stations__eightd_has_key_dispenser", StringType, true)
    ))
  }
}
