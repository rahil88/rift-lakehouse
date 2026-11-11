from collector.rate_limiter import Limit, RateLimiter, TokenBucket, parse_counts, parse_limits


def test_parse_headers():
    assert parse_limits("100:120,20:1") == [Limit(100, 120.0), Limit(20, 1.0)]
    assert parse_counts("3:120,1:1") == {120.0: 3, 1.0: 1}
    assert parse_limits(None) == [] and parse_counts("") == {}


def test_bucket_sized_so_burst_plus_refill_equals_limit():
    bucket = TokenBucket(Limit(100, 120.0), now=0.0)
    assert bucket.capacity == 10
    assert bucket.capacity + bucket.rate * 120.0 == 100


def _max_in_any_window(timestamps: list[float], window: float) -> int:
    """Largest number of calls inside any half-open interval [t, t + window)."""
    best, left = 0, 0
    for right, t in enumerate(timestamps):
        while t - timestamps[left] >= window:
            left += 1
        best = max(best, right - left + 1)
    return best


def test_never_exceeds_any_limit_in_any_window(clock):
    limiter = RateLimiter([Limit(20, 1.0), Limit(100, 120.0)], clock=clock, sleep=clock.sleep)
    calls = []
    for _ in range(400):
        limiter.acquire("americas", "match-v5.match")
        calls.append(clock())
    assert _max_in_any_window(calls, 1.0) <= 20
    assert _max_in_any_window(calls, 120.0) <= 100
    # and it still makes progress at close to the sustained limit (90% of 100/120s)
    rate = (len(calls) - 1) / (calls[-1] - calls[0])
    assert 0.70 < rate <= 100 / 120


def test_hosts_have_independent_buckets(clock):
    limiter = RateLimiter([Limit(5, 10.0)], clock=clock, sleep=clock.sleep, burst_fraction=1.0)
    for _ in range(5):
        limiter.acquire("na1", "league")
    assert limiter.acquire("americas", "match") == 0.0  # na1 usage doesn't slow americas


def test_first_sync_charges_calls_riot_already_counted(clock):
    """A restarted crawler sees Riot's count (95 of 100 used) and waits instead of hitting 429."""
    limiter = RateLimiter([Limit(100, 120.0)], clock=clock, sleep=clock.sleep)
    limiter.acquire("americas", "m")
    limiter.update(
        "americas", "m", {"X-App-Rate-Limit": "100:120", "X-App-Rate-Limit-Count": "95:120"}
    )
    waited = limiter.acquire("americas", "m")
    assert waited > 100  # ~ (95 - 10 + 1) tokens at 0.75/s


def test_limits_come_from_headers_not_defaults(clock):
    limiter = RateLimiter([Limit(20, 1.0)], clock=clock, sleep=clock.sleep)
    limiter.acquire("americas", "m")
    limiter.update(
        "americas",
        "m",
        {
            "X-App-Rate-Limit": "500:10,30000:600",
            "X-App-Rate-Limit-Count": "1:10,1:600",
            "X-Method-Rate-Limit": "2000:10",
            "X-Method-Rate-Limit-Count": "1:10",
        },
    )
    app = limiter._buckets[RateLimiter.app_key("americas")]
    method = limiter._buckets[RateLimiter.method_key("americas", "m")]
    assert [b.limit for b in app] == [Limit(500, 10.0), Limit(30000, 600.0)]
    assert [b.limit for b in method] == [Limit(2000, 10.0)]


def test_block_delays_next_acquire(clock):
    limiter = RateLimiter([Limit(20, 1.0)], clock=clock, sleep=clock.sleep)
    limiter.block(RateLimiter.app_key("na1"), 7.0)
    assert limiter.acquire("na1", "league") >= 7.0
