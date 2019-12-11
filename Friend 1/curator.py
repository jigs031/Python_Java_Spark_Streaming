#    Spark
from pyspark import SparkContext
from pyspark.sql import SparkSession
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from pyhive import hive


import os

os.environ['PYSPARK_SUBMIT_ARGS']=' --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 pyspark-shell '

def loadLookUp( spark):
    conn = hive.Connection ( host = "34.69.50.158" , port = 10000 , username = "ravit" , database = "bixi_feed" )
    cursor = conn.cursor ( )
    cursor.execute ( "select * from station_status_json" )
    f = open ("resources/lookup.json","a")
    for result in cursor.fetchall ( ) :
        data=result[0]
        f.write(data)

    f.close()
    df=spark.read.json("resources/lookup.json")
    return df
def send_to_kafka ( row ) :
    producer = KafkaProducer ( bootstrap_servers = '34.69.50.158:9092' )
    value= ""
    for item in row:
        value=value+","+str(item)
    print(value)
    producer.send ( 'curated_trip' , value.encode('utf-8') )
    producer.flush ( )
    producer.close()

def joinRDD( spark,rdd ):
    if rdd.isEmpty()==False:
        raw_rdd=rdd.map(lambda raw: str(raw).split(","))
        df=raw_rdd.toDF(["start_date","start_station_code","end_date","end_station_code","duration_sec","is_member","start_station_data__stations__station_id","start_station_data__stations__external_id","start_station_data__stations__name","start_station_data__stations__short_name","start_station_data__stations__lat","start_station_data__stations__lon","start_station_data__stations__rental_methods__001","start_station_data__stations__rental_methods__002","start_station_data__stations__capacity","start_station_data__stations__electric_bike_surcharge_waiver","start_station_data__stations__eightd_has_key_dispenser","end_station_data__stations__station_id","end_station_data__stations__external_id","end_station_data__stations__name","end_station_data__stations__short_name","end_station_data__stations__lat","end_station_data__stations__lon","end_station_data__stations__rental_methods__001","end_station_data__stations__rental_methods__002","end_station_data__stations__capacity","end_station_data__stations__electric_bike_surcharge_waiver","end_station_data__stations__eightd_has_key_dispenser"])
        df.show(10)
        df.createOrReplaceTempView("raw")
        result=spark.sql("select raw.*,lookup.* from raw inner join lookup on raw.end_station_code=lookup.station_id")
        result.show(10)
        result.foreach (lambda msg: send_to_kafka(msg) )

def process():
    sc = SparkContext ( master = "local[*]" )

    spark = SparkSession ( sc ).builder.appName ( "Spark Curator" ).master ( "local[*]" ).getOrCreate ( )

    ssc = StreamingContext ( sc , 2 )
    lookup_df=loadLookUp(spark)
    lookup_df.createOrReplaceTempView("lookup")
    lookup_df.show(10)
    kafkaStream = KafkaUtils.createStream(ssc, '34.69.50.158:2181', 'enriched_trip', {'enriched_trip':1},
                                              {"auto.offset.reset": "smallest"})
    kafkaStream.map(lambda  msg: msg[1]).foreachRDD(lambda rdd: joinRDD(spark,rdd))
    ssc.start()
    ssc.awaitTermination()

process()