"""Collector: rate-limited Riot API client and resumable crawl.

Validates each event against the JSON Schema contracts in contracts/ and produces valid events
to `lol.match.raw` and rejected ones to `lol.match.dlq` (Phases 1-2).
"""
