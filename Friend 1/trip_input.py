from kafka import KafkaProducer

file = open("resources/input.csv", "r")
producer = KafkaProducer ( bootstrap_servers = '34.69.50.158:9092' )
i=0
for line in file:
    print(line)
    if i!=0:
        producer.send ( 'trip' , ",".join(line.split(",")).encode ( 'utf-8' ) )
    i=i+1

producer.flush()
producer.close()