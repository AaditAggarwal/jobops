"""Policy invariants for retention windows.

The SQL itself is exercised against a real database; these guard the constants,
where a plausible edit silently disables a whole step.
"""

from __future__ import annotations

from jobops.enrich import retention


def test_new_grad_rows_are_kept_longer_than_the_rest():
    # New-grad postings are the targets and feed funnel metrics.
    assert retention.NEW_GRAD_RETENTION_DAYS > retention.RETENTION_DAYS


def test_description_grace_is_shorter_than_retention():
    # If the grace period ever exceeded the delete window, prune_descriptions
    # would never match a row and the largest space saving would silently stop.
    assert retention.DESCRIPTION_GRACE_DAYS < retention.RETENTION_DAYS


def test_grace_period_outlasts_a_polling_cycle():
    # is_new_grad is refined from the JD by a follow-up detail fetch after
    # insert; pruning inside that window would corrupt the classification.
    assert retention.DESCRIPTION_GRACE_DAYS >= 1


def test_size_alert_fires_below_the_cap():
    assert 0 < retention.SIZE_WARN_FRACTION < 1
    assert retention.SIZE_LIMIT_MB > 0
