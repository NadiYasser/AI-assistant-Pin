from kafka import KafkaProducer
import json, time, random

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',  # Serveur Kafka local
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
)

# Fonction pour générer l'horodatage arrondi aux 5 secondes
def get_5s_timestamp():
    now = int(time.time())
    return now - (now % 5)

# Liste de phrases d'audio simulées
audio_transcripts = [
    "I need to leave now",
    "Please remind me about the meeting",
    "Where are my keys?",
    "It's time to go to the store",
    "Don't forget to call John"
]

while True:
    # Choisir un transcript audio aléatoire parmi la liste
    transcript = random.choice(audio_transcripts)

    # Générer les données à envoyer
    data = {
        "type": "audio",
        "timestamp": get_5s_timestamp(),
        "transcript": transcript,
        "keywords": ["leave", "meeting", "keys", "store", "call"],
        "confidence": round(random.uniform(0.85, 0.98), 2)  # Niveau de confiance entre 0.85 et 0.98
    }

    try:
        # Envoi des données au topic Kafka
        producer.send("audio_stream", data)
        producer.flush()  # S'assurer que le message est bien envoyé
        print("Audio sent:", data)
    except Exception as e:
        print(f"Error sending audio message: {e}")

    # Délai avant d'envoyer le prochain message
    time.sleep(3)  # Envoi des données toutes les 3 secondes