"""Unit tests for board sharding — the disjoint/complete guarantee is what
keeps 2-way parallelism inside the polite-client amendment."""

from jobops.ingest.common import shard_tokens

TOKENS = ["stripe", "figma", "duolingo", "reddit", "waymo", "discord",
          "janestreet", "brex", "okta", "lyft", "vercel"]


def test_no_env_returns_all(monkeypatch):
    monkeypatch.delenv("JOBOPS_SHARD", raising=False)
    assert shard_tokens(TOKENS) == TOKENS


def test_shards_partition_disjoint_and_complete(monkeypatch):
    monkeypatch.setenv("JOBOPS_SHARD", "0/2")
    s0 = shard_tokens(TOKENS)
    monkeypatch.setenv("JOBOPS_SHARD", "1/2")
    s1 = shard_tokens(TOKENS)
    assert set(s0) & set(s1) == set()          # never the same board twice
    assert sorted(s0 + s1) == sorted(TOKENS)   # nothing dropped
    assert s0 and s1                            # both shards non-trivial


def test_sharding_is_stable_across_calls(monkeypatch):
    monkeypatch.setenv("JOBOPS_SHARD", "0/2")
    assert shard_tokens(TOKENS) == shard_tokens(TOKENS)


def test_order_preserved_within_shard(monkeypatch):
    monkeypatch.setenv("JOBOPS_SHARD", "1/2")
    s1 = shard_tokens(TOKENS)
    assert s1 == [t for t in TOKENS if t in set(s1)]
