import json
import os
import time
from datetime import datetime, timezone
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
You are an ambient audio context extraction assistant for a general-purpose AI assistant.

Your job is to analyze speech, background audio, and short spoken interactions and produce the single most useful short context summary for a downstream assistant.

PRIMARY OBJECTIVE
Capture the most relevant immediate context from the audio for assistant behavior.

PRIORITIZE SIGNALS LIKE
- Meeting context: introductions, agenda items, action items, brainstorming, status updates, deadlines, decision making
- Productivity and time cues: mentions of schedules, tasks, reminders, commute, work blocks, urgency, lateness
- Hydration and activity cues: mentions of drinking water, exercising, walking, resting, fatigue, sedentary behavior
- Contextual questions: direct questions asked near the device, requests for clarification, references to nearby events or objects
- Ambient situation: quiet office, conversation, street noise, cafe, home activity, transit, workout environment
- Important interrupts: alarms, timers, notifications, doorbells, name calls, repeated prompts

RULES
- Use only spoken content, sound events, tone, and immediate audio context
- Do not invent words or events not supported by the audio
- Prefer the one detail that would most help an assistant respond intelligently right now
- If the audio contains a direct user request or question, summarize that request over passive background context
- If there is no meaningful context, say that no actionable audio context was detected
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


def get_next_boundary_timestamp():
    now = time.time()
    return int(now // POLL_INTERVAL_SECONDS + 1) * POLL_INTERVAL_SECONDS


def wait_until(timestamp):
    delay = timestamp - time.time()
    if delay > 0:
        time.sleep(delay)


def format_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "")


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
                    "{\"transcript\": string, \"keywords\": string[], \"confidence\": float}. "
                    "The transcript field must be one short sentence that states the most useful actionable context from the audio. "
                    "Prefer a direct user request, otherwise the most relevant surrounding situation. "
                    "If no meaningful context is present, transcript must say: No actionable audio context detected. "
                    "The keywords field must contain 2 to 5 short keywords or key phrases grounded in the audio. "
                    "If there is no meaningful context, keywords must be an empty array. "
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
        "keywords": [
            str(keyword).strip()
            for keyword in parsed.get("keywords", [])
            if str(keyword).strip()
        ],
        "confidence": max(0.0, min(1.0, float(parsed["confidence"]))),
    }


def build_message(audio_path, timestamp):
    transcript = transcribe_audio(audio_path)
    result = analyze_transcript(transcript)
    return {
        "source": "speech_to_text",
        "timestamp": format_timestamp(timestamp),
        "transcript": result["transcript"],
        "keywords": result["keywords"],
        "confidence": result["confidence"],
        "audio_ref": audio_path.name,
    }


def main():
    audio_index = 0
    next_boundary = get_next_boundary_timestamp()

    while True:
        audio_files = list_audio_files()
        if not audio_files:
            print(f"No audio files found in {AUDIO_DATA_DIR}. Waiting {POLL_INTERVAL_SECONDS}s...")
            wait_until(next_boundary)
            next_boundary += POLL_INTERVAL_SECONDS
            continue

        wait_until(next_boundary)
        audio_path = audio_files[audio_index % len(audio_files)]

        try:
            data = build_message(audio_path, next_boundary)
            producer.send(TOPIC_NAME, data)
            producer.flush()
            print(f"Audio sent from {audio_path.name}: {data}")
        except Exception as exc:
            print(f"Error processing {audio_path.name}: {exc}")

        audio_index += 1
        next_boundary += POLL_INTERVAL_SECONDS


if __name__ == "__main__":
    main()
