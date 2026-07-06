"""The rebase decide-function: pure, no I/O, exhaustively testable.

Called when a commit loses the root CAS. Decides whether the changeset can
be safely re-executed against the winner's generation or must abort to the
application. The safety rule: only blind appends have state-independent
meaning; everything else made decisions based on reads at the old
generation, and re-executing it against unseen state is write skew.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ducklake_serverless.models import (
    Abort,
    ConflictPolicy,
    RebaseDecision,
    Replay,
    StatementClass,
)

if TYPE_CHECKING:
    from ducklake_serverless.models import Changeset


def decide_rebase(  # noqa: PLR0911  # decision table: one return per rule
    changeset: Changeset,
    policy: ConflictPolicy,
    attempt: int,
    max_attempts: int,
) -> RebaseDecision:
    """Decide replay-vs-abort for one lost CAS race.

    Order matters: attempt exhaustion and DDL abort unconditionally, before
    any policy can say replay.
    """
    if attempt >= max_attempts:
        return Abort(reason=f"exhausted {max_attempts} commit attempts")
    if changeset.has_ddl:
        return Abort(
            reason="changeset contains DDL — schema changes never auto-replay; serialize migrations"
        )
    if policy is ConflictPolicy.ABORT_ALL:
        return Abort(reason="policy is abort_all")
    if not changeset.statements:
        return Abort(reason="empty changeset — nothing to replay")

    has_state_dependent = any(
        s.statement_class is StatementClass.STATE_DEPENDENT_DML for s in changeset.statements
    )
    if has_state_dependent and policy is not ConflictPolicy.REPLAY_ALL:
        return Abort(
            reason="changeset contains state-dependent DML (UPDATE/DELETE/"
            "lake-reading INSERT) — re-execution against unseen state is "
            "write skew; re-read and re-decide, or opt into replay_all"
        )
    if changeset.has_reads and policy is not ConflictPolicy.REPLAY_ALL:
        # The caller read lake state in this transaction; its appends may
        # encode decisions from that read (SELECT max(id) -> INSERT literal).
        # Replaying only the writes against newer state launders write skew
        # through the blind-append path — the same dependency expressed as
        # one INSERT…SELECT would already abort.
        return Abort(
            reason="changeset mixes reads with writes — the writes may depend "
            "on state a replay target no longer has; re-read and re-decide, "
            "or opt into replay_all"
        )
    return Replay()
