{% docs __overview__ %}
# Rift Lakehouse

League of Legends ranked solo matches from the Riot API, validated against a JSON Schema data
contract **before** they enter Kafka, then landed in Unity Catalog and modeled here.

* **bronze** (`rift_bronze`): raw JSON payloads + ingestion metadata, loaded by COPY INTO
* **silver** (`rift_silver`): typed, deduped on natural keys, incremental MERGE
* **gold** (`rift_gold`): facts, dimensions, the SCD2 rank snapshot, and the win-rate mart
{% enddocs %}

{% docs patch %}
Game patch as `major.minor` (e.g. `16.22`), parsed from `gameVersion` (`16.22.834.8910`).
Sort with `dim_patch.patch_sort_key`, not as a string: `16.3` < `16.22`.
{% enddocs %}

{% docs role %}
Riot's `teamPosition`: TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY (support). Null when Riot could not
assign a role (usually remakes); those rows are excluded from the win-rate mart.
{% enddocs %}

{% docs winrate_mart %}
Champion win rate by patch and role, with sample size (`games`) and a 95% **Wilson score
interval**. The interval is the point: a 3–0 champion shows a 100% win rate but an interval of
roughly [0.44, 1.00], so nobody mistakes it for a strong pick. Remakes are excluded.
Recommended filter for display: `games >= 20`.
{% enddocs %}
