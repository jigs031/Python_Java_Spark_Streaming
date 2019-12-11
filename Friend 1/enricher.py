import subprocess
from kafka import KafkaConsumer
from kafka import KafkaProducer
from pyhive import hive

class Enricher:

    def loadLookup( self ):

        station_map = dict ()
        conn = hive.Connection ( host = "34.69.50.158" , port = 10000 , username = "ravit",database ="bixi_feed" )
        cursor = conn.cursor ( )
        cursor.execute ( "select * from enriched_station_information" )
        for result in cursor.fetchall ( ) :
            print(result)
            station_map[result[0]]=result[0]+","+result[1]+","+result[2]+","+str(result[3])+","+str(result[4])+","+str(result[5])+","+result[6]+","+str(result[7])+","+str(result[8])+","+result[9]+","+str(result[10])+","+str(result[11])+","+str(result[12])+","+result[13]+","+result[14]+","+result[15]+","+result[16]+","+result[17]+","+result[18]+","+result[19]+","+result[20]+","+result[21]+","+result[22]

        print ( station_map )
        return station_map


    def process( self ):
        print("Loading the Lookup...")
        station_map=self.loadLookup()
        consumer = KafkaConsumer ( 'trip',bootstrap_servers=['34.69.50.158:9092'],
             auto_offset_reset='latest',
             enable_auto_commit=True,
             group_id='my-group',
             value_deserializer=lambda x: x.decode('utf-8') )
        producer = KafkaProducer ( bootstrap_servers = '34.69.50.158:9092' )

        '''
        Input Message:
            start_date,start_station_code,end_date,end_station_code,duration_sec,is_member
            2019/11/12,0,2019/11/13,1,120,1
            2019/11/12,1,2019/11/13,0,120,1
            
        Lookup Message:
          data__stations__station_id,data__stations__external_id,data__stations__name,data__stations__short_name,data__stations__lat,data__stations__lon,data__stations__rental_methods__001,data__stations__rental_methods__002,data__stations__capacity,data__stations__electric_bike_surcharge_waiver,data__stations__eightd_has_key_dispenser
          1,0b0fda98-08f3-11e7-a1cb-3863bb33a4e4,Métro Champ-de-Mars (Viger / Sanguinet),6001,45.51035067563650,-73.55650842189790,CREDITCARD,KEY,33,FALSE,FALSE
          2,0b0fdf05-08f3-11e7-a1cb-3863bb33a4e4,Ste-Catherine / Dezery,6002,45.539385081961700,-73.54099988937380,CREDITCARD,KEY,19,FALSE,FALSE
          3,0b0fe114-08f3-11e7-a1cb-3863bb33a4e4,Clark / Evans,6003,45.51100666600310,-73.56760203838350,CREDITCARD,KEY,19,FALSE,FALSE
          4,0b0fe2b5-08f3-11e7-a1cb-3863bb33a4e4,du Champ-de-Mars / Gosford,6004,45.50965520472070,-73.55400860309600,CREDITCARD,KEY,23,FALSE,FALSE

            
        Output Message:
             "2019/11/12,0,2019/11/13,1,120,1", "0,0b0fda98-08f3-11e7-a1cb-3863bb33a4e4,Métro Champ-de-Mars (Viger / Sanguinet),6001,45.51035067563650,-73.55650842189790,CREDITCARD,KEY,33,FALSE,FALSE"  "0,0b0fda98-08f3-11e7-a1cb-3863bb33a4e4,Métro Champ-de-Mars (Viger / Sanguinet),6001,45.51035067563650,-73.55650842189790,CREDITCARD,KEY,33,FALSE,FALSE"
        
        '''
        print("Starting the Kafka Consumer...")
        for message in consumer :
            enriched_value= ",".join( str( message.value).split(","))

            trip_info = enriched_value.split ( ',' )

            print ( trip_info )
            #Enriching for Start station
            if trip_info [ 1 ] in station_map :
                enriched_value=enriched_value+","+station_map[trip_info [ 1 ]]
            else :
                enriched_value=enriched_value+",NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA"

            #Enriching the End Station
            if trip_info [ 3 ] in station_map :
                enriched_value=enriched_value+","+station_map[trip_info [ 3 ]]
            else :
                enriched_value = enriched_value + ",NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA"

            formatted_value=",".join(enriched_value.split(","))
            print(formatted_value)

            producer.send ( 'enriched_trip' , formatted_value.encode('utf-8') )

        producer.flush()
        producer.close()

enricher=Enricher();
enricher.process()