#!/usr/bin/env python3
import argparse
import copy
import hashlib
import json
from pathlib import Path


API_URL = "https://kudago.com/public-api/v1.4/events/"
OUTPUT = Path("data/source/kudago_spb_summer_2026.json")
SUMMER_START = 1780261200
SUMMER_END = 1788210000


def slice_payload(payload):
    selected = []
    for source_event in payload.get("results", []):
        event = copy.deepcopy(source_event)
        event["dates"] = sorted(
            (
                period
                for period in event.get("dates", [])
                if isinstance(period.get("start"), int)
                and SUMMER_START <= period["start"] < SUMMER_END
            ),
            key=lambda period: period["start"],
        )
        if event["dates"]:
            selected.append(event)
    return {"results": selected}


def download():
    import requests

    params = {
        "location": "spb",
        "actual_since": SUMMER_START,
        "actual_until": SUMMER_END - 1,
        "page_size": 100,
        "order_by": "-publication_date",
        "text_format": "text",
        "fields": (
            "id,publication_date,dates,title,place,location,categories,"
            "is_free,age_restriction,site_url"
        ),
        "expand": "place,location,dates",
    }
    events = []
    source_total = None
    url = API_URL
    while url:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        page = response.json()
        source_total = page["count"]
        events.extend(page["results"])
        url = page.get("next")
        params = None

    sliced = slice_payload({"results": events})["results"]
    sliced.sort(key=lambda event: event["id"])
    return {
        "count": len(sliced),
        "source_total_count": source_total,
        "results": sliced,
    }


def main():
    parser = argparse.ArgumentParser(description="Download the fixed KudaGo summer slice")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()

    rendered = json.dumps(download(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    checksum = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
    payload = json.loads(rendered)
    occurrences = sum(len(event["dates"]) for event in payload["results"])
    print(
        f"saved={args.output} events={payload['count']} "
        f"occurrences={occurrences} sha256={checksum}"
    )


if __name__ == "__main__":
    main()
