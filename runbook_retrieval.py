"""Rank troubleshooting sections by signals found in incident evidence."""

import json
import math
import re
from collections import Counter
from pathlib import Path


CORPUS = Path(__file__).resolve().parent / "runbooks" / "sections.json"


def normalized(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def retrieve_sections(evidence: list[str], limit: int = 2) -> list[dict]:
    if not evidence:
        return []

    query = [f" {normalized(text)} " for text in evidence]
    sections = json.loads(CORPUS.read_text(encoding="utf-8"))
    signals_by_section = [
        {normalized(signal) for signal in section["signals"]} for section in sections
    ]
    frequency = Counter(signal for signals in signals_by_section for signal in signals)
    ranked = []
    for section, signals in zip(sections, signals_by_section):
        matched = sorted(
            signal for signal in signals if any(f" {signal} " in text for text in query)
        )
        if not any(frequency[signal] == 1 for signal in matched):
            continue
        # Give more weight to phrases that distinguish fewer sections.
        score = sum(math.log((len(sections) + 1) / (frequency[signal] + 1)) + 1 for signal in matched)
        result = {
            "source": f"runbooks/sections.json#{section['id']}",
            "title": section["title"],
            "matched_signals": matched,
            "steps": section["steps"],
        }
        if "reference" in section:
            result["reference"] = section["reference"]
        ranked.append((score, section["id"], result))
    ranked.sort(key=lambda entry: (-entry[0], entry[1]))
    return [section for _, _, section in ranked[:limit]]
