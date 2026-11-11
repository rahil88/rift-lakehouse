"""Consumer: reads `lol.match.raw`, batches events to JSONL, uploads them to a Unity Catalog Volume.

Bronze Delta tables are then loaded from the Volume with COPY INTO (Phase 3).
"""
