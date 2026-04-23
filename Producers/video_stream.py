import base64
import json
import mimetypes
import os
import time
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
You are a safety-focused vision-language monitoring assistant for child protection.

Your job is to analyze a single image from a child's perspective and identify possible safety risks in the environment.

PRIMARY OBJECTIVE
Detect whether there is a prominent danger to a child visible on screen.

WHAT TO WATCH FOR
- Traffic: cars, motorcycles, bicycles, buses, moving vehicles, road crossing without supervision
- Heights: stairs, balconies, windows, ledges, climbing furniture, playground fall risks
- Water: pools, bathtubs, ponds, buckets, open drains, beaches, water edges
- Fire and heat: stoves, ovens, candles, lighters, matches, hot liquids, heaters, irons
- Sharp or harmful objects: knives, scissors, glass, tools, needles
- Choking hazards: small toys, coins, batteries, beads, marbles, plastic bags
- Electrical risks: exposed outlets, wires, chargers, appliances near water
- Poisoning hazards: cleaning products, medicines, chemicals, detergents, alcohol
- Strangulation risks: cords, ropes, blind strings, loose straps
- Animal threats: aggressive dogs, wild animals, insects in dangerous proximity
- Human threats: unfamiliar adults acting suspiciously, physical conflict, crowd risk
- Environmental hazards: smoke, fire, broken glass, unstable furniture, blocked exits
- Child distress indicators: fallen child, trapped child, panic, disorientation

RULES
- Use only visible evidence
- Do not invent details
- Focus on the most prominent child-safety risk in the image
- If there is no clear danger, say that no immediate child danger is visible
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
                    "{\"description\": string, \"confidence\": float}. "
                    "The description must be one short sentence that states the most prominent danger to a child visible on screen. "
                    "If no danger is visible, description must say: No immediate child danger visible. "
                    "Confidence must be between 0 and 1."
                ),
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Analyze this image for child safety risk and summarize the most prominent visible danger.",
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
        "description": str(parsed["description"]).strip(),
        "confidence": max(0.0, min(1.0, float(parsed["confidence"]))),
    }


def build_message(image_path):
    result = describe_image(image_path)
    return {
        "type": "video",
        "timestamp": get_15s_timestamp(),
        "description": result["description"],
        "confidence": result["confidence"],
    }


def main():
    image_index = 0

    while True:
        image_files = list_image_files()
        if not image_files:
            print(f"No images found in {VIDEO_DATA_DIR}. Waiting {POLL_INTERVAL_SECONDS}s...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        image_path = image_files[image_index % len(image_files)]

        try:
            data = build_message(image_path)
            producer.send(TOPIC_NAME, data)
            producer.flush()
            print(f"Video sent from {image_path.name}: {data}")
        except Exception as exc:
            print(f"Error processing {image_path.name}: {exc}")

        image_index += 1
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
