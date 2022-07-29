# delta-cdc

Playground repo to demonstrate capabilities of Delta Lake for CDC (Change Data Capture) pipelines with Structured Streaming.

Entry point: src/test/scala/org/github/kevinwallimann/MySparkTest

- "append only": Plain Streaming into Delta Format.
- "upsert the rows": Initialize Delta Table with first write, then use merge feature to upsert rows.
