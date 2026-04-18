import os
import json
from typing import Any, Dict
from groq import Groq
from dotenv import load_dotenv
load_dotenv()

client = Groq(api_key=os.environ["GROQ_API_KEY"])


SYSTEM_PROMPT = """
You are a real-time decision model.

Your role is to decide whether the system should act now based on the provided context.

You must output only valid JSON.
Do not use markdown.
Do not wrap the JSON in triple backticks.
Do not add any explanation before or after the JSON.

Allowed decision values:
- ignore
- notify
- notify_urgent

Allowed action_type values:
- missing_item_alert
- late_risk_alert
- context_warning
- none

Return this schema:
{
  "should_act": boolean,
  "decision": string,
  "action_type": string,
  "priority": "low" | "medium" | "high",
  "confidence": float,
  "reason": string,
  "message": string
}
""".strip()

def build_processed_object(topic_object: Dict[str, Any]) -> Dict[str, Any]:
    """Aplatit l'objet de contexte pour le modèle."""
    return {
        "location": topic_object.get("state", {}).get("location", {}).get("value"),
        "activity": topic_object.get("state", {}).get("activity", {}).get("value"),
        "activity_confidence": topic_object.get("state", {}).get("activity", {}).get("confidence"),
        "keys_present": topic_object.get("objects", {}).get("keys", {}).get("present"),
        "keys_confidence": topic_object.get("objects", {}).get("keys", {}).get("confidence"),
        "bag_present": topic_object.get("objects", {}).get("bag", {}).get("present"),
        "bag_confidence": topic_object.get("objects", {}).get("bag", {}).get("confidence"),
        "intent": topic_object.get("intent", {}).get("type"),
        "intent_confidence": topic_object.get("intent", {}).get("confidence"),
        "calendar_event": topic_object.get("constraints", {}).get("calendar_event"),
        "time_to_event_min": topic_object.get("constraints", {}).get("time_to_event_min"),
        "urgency": topic_object.get("constraints", {}).get("urgency"),
        "video_summary": topic_object.get("signals", {}).get("video_summary"),
        "audio_summary": topic_object.get("signals", {}).get("audio_summary"),
    }


def validate_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Validation minimale de la réponse du modèle."""
    allowed_decisions = {"ignore", "notify", "notify_urgent"}
    allowed_action_types = {
        "missing_item_alert",
        "late_risk_alert",
        "context_warning",
        "none",
    }
    allowed_priorities = {"low", "medium", "high"}

    required_fields = {
        "should_act": bool,
        "decision": str,
        "action_type": str,
        "priority": str,
        "confidence": (float, int),
        "reason": str,
        "message": str,
    }

    for field, expected_type in required_fields.items():
        if field not in result:
            raise ValueError(f"Champ manquant: {field}")
        if not isinstance(result[field], expected_type):
            raise TypeError(f"Type invalide pour {field}: {type(result[field])}")

    if result["decision"] not in allowed_decisions:
        raise ValueError(f"decision invalide: {result['decision']}")

    if result["action_type"] not in allowed_action_types:
        raise ValueError(f"action_type invalide: {result['action_type']}")

    if result["priority"] not in allowed_priorities:
        raise ValueError(f"priority invalide: {result['priority']}")

    confidence = float(result["confidence"])
    if confidence < 0.0 or confidence > 1.0:
        raise ValueError("confidence doit être entre 0.0 et 1.0")

    result["confidence"] = confidence
    return result


def decide_with_groq(processed_obj: Dict[str, Any]) -> Dict[str, Any]:
    """Appelle Groq et récupère une décision JSON stricte."""
    user_payload = {
        "context": processed_obj,
        "instruction": "Decide whether the system should act now. Return JSON only."
    }

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)}
        ],
    )

    raw_content = response.choices[0].message.content.strip()

    try:
        parsed = json.loads(raw_content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Réponse non-JSON du modèle:\n{raw_content}") from e

    return validate_result(parsed)


if __name__ == "__main__":
    topic_object = {
        "timestamp_start": "...",
        "timestamp_end": "...",
        "state": {
            "location": {
                "value": "home",
                "confidence": 0.95
            },
            "activity": {
                "value": "preparing_to_leave",
                "confidence": 0.8
            }
        },
        "objects": {
            "keys": {
                "present": 'false',
                "confidence": 0.7
            },
            "bag": {
                "present": 'true',
                "confidence": 0.9
            }
        },
        "intent": {
            "type": "leaving_home",
            "confidence": 0.75
        },
        "constraints": {
            "calendar_event": "meeting",
            "time_to_event_min": 20,
            "urgency": "medium"
        },
        "signals": {
            "video_summary": "...",
            "audio_summary": "...",
            "location": "home"
        }
    }

    processed_obj = build_processed_object(topic_object)
    result = decide_with_groq(processed_obj)

    print(json.dumps(result, indent=2, ensure_ascii=False))