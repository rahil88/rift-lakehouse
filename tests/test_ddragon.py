import json

import httpx

from collector.ddragon import fetch_champions, latest_version, write_champions


def test_fetch_and_write_champions(tmp_path):
    def handler(request):
        if request.url.path == "/api/versions.json":
            return httpx.Response(200, json=["16.22.1", "16.21.1"])
        assert request.url.path == "/cdn/16.22.1/data/en_US/champion.json"
        return httpx.Response(
            200,
            json={
                "data": {
                    "MonkeyKing": {
                        "key": "62",
                        "id": "MonkeyKing",
                        "name": "Wukong",
                        "title": "the Monkey King",
                        "tags": ["Fighter", "Tank"],
                    }
                }
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        version = latest_version(http)
        rows = fetch_champions(http, version)
    assert rows == [
        {
            "version": "16.22.1",
            "champion_key": 62,
            "champion_id": "MonkeyKing",
            "name": "Wukong",
            "title": "the Monkey King",
            "tags": ["Fighter", "Tank"],
        }
    ]
    out = write_champions(tmp_path, version, rows)
    assert out == tmp_path / "ddragon_champions" / "version=16.22.1" / "champions.jsonl"
    assert json.loads(out.read_text())["name"] == "Wukong"
