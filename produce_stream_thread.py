import requests
import json
import time
from kafka import KafkaProducer

# Connexion au cluster Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

url = 'https://mastodon.social/api/v1/timelines/tag/IA'

# Bloucle infinie pour streamer les données de l'API
while True:
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Parcours de tous les status
        for status in data:
            
            # Extraction des informations pertinentes du status
            message = {
                'id': status['id'],
                'created_at': status['created_at'],
                'content': status['content'],
                'account': status['account']['username'],
                'tags': status['tags']
            }
            key = message['id']
            key = key.encode('utf-8')
            
            # Envoie du message au topic spark sur kafka
            producer.send('spark', value=message, key=key)
            
            # print(message)
    
    # On atted 10 secondes avant de faire une nouvelle requête
    time.sleep(10)

