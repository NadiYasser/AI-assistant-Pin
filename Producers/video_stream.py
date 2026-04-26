import base64
import json
import mimetypes
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from groq import Groq
from kafka import KafkaProducer


ROOT_DIR = Path(__file__).resolve().parents[1]
VIDEO_DATA_DIR = ROOT_DIR / "Producers" / "video_data"
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
MODEL_NAME = "meta-llama/llama-4-scout-17b-16e-instruct"
TOPIC_NAME = "video_stream"
POLL_INTERVAL_SECONDS = 15
VIDEO_SYSTEM_PROMPT = """
You are a visual context extraction assistant for a general-purpose AI assistant.

Your job is to analyze a single image and produce the single most useful short context summary for a downstream assistant.

PRIMARY OBJECTIVE
Capture the most relevant immediate visual context from the image for assistant behavior.

PRIORITIZE SIGNALS LIKE
- Meeting context: people gathered around a table, presentation screens, whiteboards, laptops, note taking
- Productivity and time cues: clocks, calendars, workstation setup, commuting, waiting areas, transit scenes
- Hydration and activity cues: water bottles, cups, gym equipment, walking, running, stretching, outdoor movement
- Contextual question support: objects, signs, labels, screens, rooms, nearby tools, visible tasks
- Environment classification: office, home, cafe, street, store, vehicle, gym, classroom
- Important visible changes: crowding, interruptions, device use, food, packages, weather conditions

RULES
- Use only visible evidence
- Do not invent details
- Prefer the one detail that would most help an assistant respond intelligently right now
- Mention a concrete object, activity, or setting when possible
- If there is no meaningful context, say that no actionable visual context is visible
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


def list_image_files():
    if not VIDEO_DATA_DIR.exists():
        raise FileNotFoundError(f"Image folder not found: {VIDEO_DATA_DIR}")

    return sorted(
        path for path in VIDEO_DATA_DIR.iterdir()
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    )


def image_to_data_url(image_path):
    mime_type, _ = mimetypes.guess_type(image_path.name)
    if mime_type is None:
        mime_type = "image/jpeg"

    encoded_image = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    return f"data:{mime_type};base64,{encoded_image}"


def describe_image(image_path):
    completion = client.chat.completions.create(
        model=MODEL_NAME,
        response_format={"type": "json_object"},
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    f"{VIDEO_SYSTEM_PROMPT}\n\n"
                    "Return JSON only with this schema: "
                    "{\"objects\": string[], \"scene_description\": string, \"confidence\": float}. "
                    "The scene_description must be one short sentence that states the most useful actionable context visible on screen. "
                    "Prefer the detail most relevant to a direct user question or current activity. "
                    "The objects field must contain 1 to 6 concrete visible objects when possible. "
                    "If no meaningful context is visible, scene_description must say: No actionable visual context visible. "
                    "If no clear objects are visible, objects must be an empty array. "
                    "Confidence must be between 0 and 1."
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Analyze this image and summarize the most useful visible context for a general-purpose AI assistant.",
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": image_to_data_url(image_path)},
                    },
                ],
            },
        ],
    )

    content = completion.choices[0].message.content
    parsed = json.loads(content)

    return {
        "objects": [
            str(item).strip()
            for item in parsed.get("objects", [])
            if str(item).strip()
        ],
        "scene_description": str(parsed["scene_description"]).strip(),
        "confidence": max(0.0, min(1.0, float(parsed["confidence"]))),
    }


def build_message(image_path, timestamp):
    result = describe_image(image_path)
    return {
        "source": "vlm",
        "timestamp": format_timestamp(timestamp),
        "objects": result["objects"],
        "scene_description": result["scene_description"],
        "confidence": result["confidence"],
        "media_ref": image_path.name,
    }


def main():
    image_index = 0
    next_boundary = get_next_boundary_timestamp()

    while True:
        image_files = list_image_files()
        if not image_files:
            print(f"No images found in {VIDEO_DATA_DIR}. Waiting {POLL_INTERVAL_SECONDS}s...")
            wait_until(next_boundary)
            next_boundary += POLL_INTERVAL_SECONDS
            continue

        wait_until(next_boundary)
        image_path = image_files[image_index % len(image_files)]

        try:
            data = build_message(image_path, next_boundary)
            producer.send(TOPIC_NAME, data)
            producer.flush()
            print(f"Video sent from {image_path.name}: {data}")
        except Exception as exc:
            print(f"Error processing {image_path.name}: {exc}")

        image_index += 1
        next_boundary += POLL_INTERVAL_SECONDS


if __name__ == "__main__":
    main()
