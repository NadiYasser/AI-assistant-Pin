import json
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from groq import Groq
from kafka import KafkaProducer


ROOT_DIR = Path(__file__).resolve().parents[1]
AUDIO_DATA_DIR = ROOT_DIR / "Producers" / "audio_data"
SUPPORTED_EXTENSIONS = {".mp3", ".wav", ".m4a", ".flac", ".ogg"}
TRANSCRIPTION_MODEL = "whisper-large-v3"
CHAT_MODEL = "llama-3.3-70b-versatile"
TOPIC_NAME = "audio_stream"
POLL_INTERVAL_SECONDS = 15
AUDIO_SYSTEM_PROMPT = """
You are a safety-focused speech monitoring assistant for child protection.

Your job is to analyze speech, background audio, and short spoken interactions captured near a child and identify possible safety risks.

PRIMARY OBJECTIVE
Detect whether there is a prominent danger to a child in the audio.

WHAT TO WATCH FOR
- Child screaming, crying intensely, panic, repeated calls for help
- Sounds of choking, gasping, struggling to breathe, or unusual silence after distress
- Loud impact sounds, falls, crashes, breaking glass, or furniture tipping
- Fire or environmental danger cues: smoke alarm, fire alarm, crackling fire, explosion-like sounds
- Water danger cues: splashing with distress, bathtub or pool struggle sounds
- Traffic danger cues: horns, screeching tires, nearby moving vehicles, road noise combined with distress
- Human threat cues: yelling, threats, aggressive tone, fighting, forced commands, coercion
- Suspicious adult interactions: unknown adult urging secrecy, isolation, following, or inappropriate commands
- Poisoning or ingestion cues: coughing after swallowing, gagging, adult mentions of chemicals, medicine, batteries, cleaning products
- Animal threat cues: aggressive barking, growling, attack sounds, child distress near animal noise
- Signs of neglect or missing supervision in risky contexts

RULES
- Use only spoken content, sound events, tone, and immediate audio context
- Do not invent words or events not supported by the audio
- Focus on the most prominent child-safety risk in the audio
- If there is no clear danger, say that no immediate child danger is heard
- Keep the output concise and specific
""".strip()


load_dotenv(ROOT_DIR / ".env")

groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    raise RuntimeError("GROQ_API_KEY is missing from the environment.")

client = Groq(api_key=groq_api_key)
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def get_15s_timestamp():
    now = int(time.time())
    return now - (now % POLL_INTERVAL_SECONDS)


def list_audio_files():
    AUDIO_DATA_DIR.mkdir(parents=True, exist_ok=True)

    return sorted(
        path for path in AUDIO_DATA_DIR.iterdir()
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    )


def transcribe_audio(audio_path):
    with open(audio_path, "rb") as audio_file:
        transcription = client.audio.transcriptions.create(
            file=(audio_path.name, audio_file.read()),
            model=TRANSCRIPTION_MODEL,
        )

    return transcription.text.strip()


def analyze_transcript(transcript):
    completion = client.chat.completions.create(
        model=CHAT_MODEL,
        response_format={"type": "json_object"},
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    f"{AUDIO_SYSTEM_PROMPT}\n\n"
                    "Return JSON only with this schema: "
                    "{\"transcript\": string, \"confidence\": float}. "
                    "The transcript field must be one short sentence that states the most prominent danger to a child heard in the audio. "
                    "If no danger is heard, transcript must say: No immediate child danger heard. "
                    "Confidence must be between 0 and 1."
                ),
            },
            {
                "role": "user",
                "content": transcript,
            },
        ],
    )

    parsed = json.loads(completion.choices[0].message.content)
    return {
        "transcript": str(parsed["transcript"]).strip(),
        "confidence": max(0.0, min(1.0, float(parsed["confidence"]))),
    }


def build_message(audio_path):
    transcript = transcribe_audio(audio_path)
    result = analyze_transcript(transcript)
    return {
        "type": "audio",
        "timestamp": get_15s_timestamp(),
        "transcript": result["transcript"],
        "confidence": result["confidence"],
    }


def main():
    audio_index = 0

    while True:
        audio_files = list_audio_files()
        if not audio_files:
            print(f"No audio files found in {AUDIO_DATA_DIR}. Waiting {POLL_INTERVAL_SECONDS}s...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        audio_path = audio_files[audio_index % len(audio_files)]

        try:
            data = build_message(audio_path)
            producer.send(TOPIC_NAME, data)
            producer.flush()
            print(f"Audio sent from {audio_path.name}: {data}")
        except Exception as exc:
            print(f"Error processing {audio_path.name}: {exc}")

        audio_index += 1
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
