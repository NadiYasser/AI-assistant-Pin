from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',  # Adresse du serveur Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
)

def get_5s_timestamp():
    now = int(time.time())
    return now - (now % 5)

locations = [
    {"raw": [35.7595, -5.8340], "semantic": "home"},
    {"raw": [35.7600, -5.8350], "semantic": "street"},
]

while True:
    loc = random.choice(locations)  # Choisir un emplacement aléatoire

    data = {
        "type": "location",
        "timestamp": get_5s_timestamp(),
        "raw": loc["raw"],
        "semantic": loc["semantic"],
        "confidence": round(random.uniform(0.9, 0.99), 2)  
    }

    try:
        # Envoi des données au topic Kafka
        producer.send("location_stream", data)
        producer.flush()  # Assurez-vous que le message est bien envoyé
        print("Location sent:", data)
    except Exception as e:
        print(f"Erreur lors de l'envoi de la localisation : {e}")

    time.sleep(4)  