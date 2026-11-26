import json
import os
import sys
from urllib.request import Request, urlopen

def generate_summary(packet):
    body = {
        "model": "nvidia/nemotron-3-ultra-550b-a55b:free",
        "max_tokens": 600,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Write three short lines in English: What happened, "
                    "What is uncertain, First check. Use only the supplied "
                    "evidence. State when the cause is unknown. "
                    "Do not describe an action as completed if the evidence "
                    "only says it will happen."
                ),
            },
            {"role": "user", "content": json.dumps(packet)},
        ],
    }

    try:
        request = Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlopen(request, timeout=60) as response:
            reply = json.load(response)
        choice = reply["choices"][0]
        summary = choice["message"]["content"]
    except (OSError, ValueError, KeyError, IndexError, TypeError):
        return None

    if choice.get("finish_reason") == "stop" and isinstance(summary, str):
        return summary.strip() or None
    return None


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 llm_summary.py TASK_ID")

    report = json.load(sys.stdin)
    task_id = sys.argv[1]
    task = next(
        (item for item in report["tasks"] if item["task_id"] == task_id),
        None,
    )
    if task is None or "llm_input" not in task:
        raise SystemExit(f"No diagnosed task found for {task_id}.")

    packet = task["llm_input"]
    summary = generate_summary(packet)

    if summary is None:
        print("Model summary unavailable. Showing the triage findings.")
        print("What happened:", packet["finding"])
        print("First check:", packet["next_check"])
    else:
        print(summary)

    for section in task.get("retrieved_sections", []):
        url = section.get("reference", {}).get("url")
        if url:
            print("\nSource:", url)
            break


if __name__ == "__main__":
    main()