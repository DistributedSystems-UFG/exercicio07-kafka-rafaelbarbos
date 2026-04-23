from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    topic_in = sys.argv[1]   
    topic_out = sys.argv[2]  
except:
    print('Usage: python3 processor.py <topic_in> <topic_out>')
    exit(1)

# Inicializa o Consumidor e o Produtor
consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

consumer.subscribe([topic_in])

print(f"Aguardando mensagens em {topic_in} para enviar para {topic_out}...")

for msg in consumer:
    # Lógica de Processamento
    texto_original = msg.value.decode('utf-8')
    texto_processado = f"Processado por Rafael: {texto_original}"
    
    print(f"Transformando: {texto_original} -> {texto_processado}")
    
    # Publica no novo tópico
    producer.send(topic_out, value=texto_processado.encode('utf-8'))
    producer.flush()
