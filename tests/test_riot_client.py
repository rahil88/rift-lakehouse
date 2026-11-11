import httpx
import pytest

from collector.riot_client import RiotAuthError, RiotClient, RiotRetryExhausted

OK_HEADERS = {
    "X-App-Rate-Limit": "20:1,100:120",
    "X-App-Rate-Limit-Count": "1:1,1:120",
    "X-Method-Rate-Limit": "2000:10",
    "X-Method-Rate-Limit-Count": "1:10",
}


def scripted(responses):
    """MockTransport that returns ``responses`` in order and records every request."""
    seen: list[httpx.Request] = []
    queue = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        status, headers, body = queue.pop(0)
        return httpx.Response(status, headers=headers, json=body)

    return httpx.MockTransport(handler), seen


def make_client(transport, clock, **kw):
    return RiotClient(
        "RGAPI-test", transport=transport, clock=clock, sleep=clock.sleep, rng=lambda: 0.0, **kw
    )


def test_sends_key_header_and_returns_json(clock):
    transport, seen = scripted([(200, OK_HEADERS, {"metadata": {"matchId": "NA1_1"}})])
    client = make_client(transport, clock)
    assert client.match("NA1_1") == {"metadata": {"matchId": "NA1_1"}}
    assert seen[0].headers["X-Riot-Token"] == "RGAPI-test"
    assert seen[0].url.host == "americas.api.riotgames.com"


def test_429_with_retry_after_waits_then_succeeds(clock):
    transport, seen = scripted(
        [
            (429, {"Retry-After": "7", "X-Rate-Limit-Type": "application"}, {}),
            (200, OK_HEADERS, ["NA1_1"]),
        ]
    )
    client = make_client(transport, clock)
    t0 = clock()
    assert client.match_ids("p", start_time=0, end_time=1) == ["NA1_1"]
    assert len(seen) == 2
    assert clock() - t0 >= 7.0
    assert client.stats["429_application"] == 1


def test_service_429_without_headers_uses_exponential_backoff(clock):
    # no X-Rate-Limit-Type and no Retry-After: shared service limit -> our own backoff
    transport, seen = scripted([(429, {}, {}), (429, {}, {}), (200, OK_HEADERS, {"ok": 1})])
    client = make_client(transport, clock, backoff_base_s=2.0)
    t0 = clock()
    assert client.match("NA1_1") == {"ok": 1}
    # equal jitter with rng=0 -> half the ceiling: attempt0 = 1s, attempt1 = 2s
    assert clock() - t0 >= 3.0
    assert client.stats["429_service"] == 2


def test_5xx_is_retried(clock):
    transport, seen = scripted([(503, {}, {}), (200, OK_HEADERS, {"ok": 1})])
    client = make_client(transport, clock)
    assert client.match("NA1_1") == {"ok": 1}
    assert client.stats["server_errors"] == 1


def test_auth_error_fails_fast_without_retry(clock):
    transport, seen = scripted([(403, {}, {})])
    client = make_client(transport, clock)
    with pytest.raises(RiotAuthError, match="24h"):
        client.match("NA1_1")
    assert len(seen) == 1


def test_404_returns_none(clock):
    transport, _ = scripted([(404, OK_HEADERS, {})])
    assert make_client(transport, clock).timeline("NA1_1") is None


def test_gives_up_after_max_retries(clock):
    transport, seen = scripted([(500, {}, {})] * 4)
    client = make_client(transport, clock, max_retries=3)
    with pytest.raises(RiotRetryExhausted):
        client.match("NA1_1")
    assert len(seen) == 4


def test_match_ids_paginates(clock):
    page1 = [f"NA1_{i}" for i in range(100)]
    transport, seen = scripted([(200, OK_HEADERS, page1), (200, OK_HEADERS, ["NA1_100"])])
    ids = make_client(transport, clock).match_ids("p", start_time=0, end_time=1)
    assert len(ids) == 101
    assert [r.url.params["start"] for r in seen] == ["0", "100"]


def test_account_by_riot_id_url_encodes_and_uses_regional_host(clock):
    transport, seen = scripted([(200, OK_HEADERS, {"puuid": "p1"})])
    assert make_client(transport, clock).account_by_riot_id("Faker Fan", "KR 1") == {"puuid": "p1"}
    assert seen[0].url.host == "americas.api.riotgames.com"
    assert seen[0].url.raw_path == b"/riot/account/v1/accounts/by-riot-id/Faker%20Fan/KR%201"


def test_league_uses_platform_host(clock):
    transport, seen = scripted([(200, OK_HEADERS, {"tier": "CHALLENGER", "entries": []})])
    make_client(transport, clock).league("challenger")
    assert seen[0].url.host == "na1.api.riotgames.com"
    assert seen[0].url.path == "/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
