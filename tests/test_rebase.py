"""decide_rebase properties: the correctness core, exhaustively probed."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ducklake_serverless.models import (
    Abort,
    Changeset,
    ConflictPolicy,
    Replay,
    Statement,
    StatementClass,
)
from ducklake_serverless.rebase import decide_rebase

statement_strategy = st.builds(
    Statement,
    sql=st.just("SQL"),
    params=st.just(()),  # pyright: ignore[reportAny]  # hypothesis just() is loosely typed
    statement_class=st.sampled_from(StatementClass),
)
changeset_strategy = st.builds(
    Changeset, statements=st.tuples() | st.lists(statement_strategy, max_size=6).map(tuple)
)


def make_changeset(*classes: StatementClass) -> Changeset:
    return Changeset(statements=tuple(Statement(sql="SQL", statement_class=c) for c in classes))


@given(changeset=changeset_strategy, policy=st.sampled_from(ConflictPolicy))
def test_exhausted_attempts_always_abort(changeset: Changeset, policy: ConflictPolicy) -> None:
    decision = decide_rebase(changeset, policy, attempt=5, max_attempts=5)
    assert isinstance(decision, Abort)


@given(changeset=changeset_strategy, policy=st.sampled_from(ConflictPolicy))
def test_ddl_always_aborts(changeset: Changeset, policy: ConflictPolicy) -> None:
    if changeset.has_ddl:
        decision = decide_rebase(changeset, policy, attempt=1, max_attempts=5)
        assert isinstance(decision, Abort)


@given(changeset=changeset_strategy)
def test_abort_all_policy_never_replays(changeset: Changeset) -> None:
    decision = decide_rebase(changeset, ConflictPolicy.ABORT_ALL, attempt=1, max_attempts=5)
    assert isinstance(decision, Abort)


@given(changeset=changeset_strategy, policy=st.sampled_from(ConflictPolicy))
def test_state_dependent_dml_never_replays_by_default(
    changeset: Changeset, policy: ConflictPolicy
) -> None:
    """THE safety property: only replay_all may replay state-dependent DML."""
    decision = decide_rebase(changeset, policy, attempt=1, max_attempts=5)
    has_sd = any(
        s.statement_class is StatementClass.STATE_DEPENDENT_DML for s in changeset.statements
    )
    if isinstance(decision, Replay) and has_sd:
        assert policy is ConflictPolicy.REPLAY_ALL


def test_blind_appends_replay_under_default_policy() -> None:
    changeset = make_changeset(StatementClass.BLIND_APPEND, StatementClass.BLIND_APPEND)
    decision = decide_rebase(
        changeset, ConflictPolicy.APPEND_ONLY_REPLAY, attempt=1, max_attempts=5
    )
    assert isinstance(decision, Replay)


def test_state_dependent_replays_under_replay_all() -> None:
    changeset = make_changeset(StatementClass.STATE_DEPENDENT_DML)
    decision = decide_rebase(changeset, ConflictPolicy.REPLAY_ALL, attempt=1, max_attempts=5)
    assert isinstance(decision, Replay)


def test_empty_changeset_aborts() -> None:
    decision = decide_rebase(
        Changeset(statements=()), ConflictPolicy.REPLAY_ALL, attempt=1, max_attempts=5
    )
    assert isinstance(decision, Abort)


def test_read_plus_append_aborts_under_default_policy() -> None:
    """A recorded READ means later writes may depend on stale state —

    replaying only the writes launders write skew through the append path.
    """
    changeset = make_changeset(StatementClass.READ, StatementClass.BLIND_APPEND)
    decision = decide_rebase(
        changeset, ConflictPolicy.APPEND_ONLY_REPLAY, attempt=1, max_attempts=5
    )
    assert isinstance(decision, Abort)


def test_read_plus_append_replays_under_replay_all() -> None:
    changeset = make_changeset(StatementClass.READ, StatementClass.BLIND_APPEND)
    decision = decide_rebase(changeset, ConflictPolicy.REPLAY_ALL, attempt=1, max_attempts=5)
    assert isinstance(decision, Replay)
