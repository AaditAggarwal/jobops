"""Unit tests for board sharding — the disjoint/complete guarantee is what
keeps 2-way parallelism inside the polite-client amendment."""

from jobops.ingest.common import shard_tokens

TOKENS = ["stripe", "figma", "duolingo", "reddit", "waymo", "discord",
          "janestreet", "brex", "okta", "lyft", "vercel"]


def test_no_env_returns_all(monkeypatch):
    # unsharded still rotates, but must return every token exactly once
    monkeypatch.delenv("JOBOPS_SHARD", raising=False)
    out = shard_tokens(TOKENS)
    assert sorted(out) == sorted(TOKENS)
    assert out in [TOKENS[i:] + TOKENS[:i] for i in range(len(TOKENS))]


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


def test_relative_order_preserved_within_shard(monkeypatch):
    # rotation may shift the starting point, but relative circular order holds
    monkeypatch.setenv("JOBOPS_SHARD", "1/2")
    s1 = shard_tokens(TOKENS)
    base = [t for t in TOKENS if t in set(s1)]
    assert s1 in [base[i:] + base[:i] for i in range(len(base))]


def test_rotation_covers_all_offsets():
    from jobops.ingest.common import rotate_tokens

    tokens = ["a", "b", "c"]
    starts = {rotate_tokens(tokens, now=1800 * i)[0] for i in range(6)}
    assert starts == {"a", "b", "c"}  # every board leads some cycle


def test_rotation_preserves_membership():
    from jobops.ingest.common import rotate_tokens

    assert sorted(rotate_tokens(TOKENS, now=12345.0)) == sorted(TOKENS)
    assert rotate_tokens([], now=0.0) == []
