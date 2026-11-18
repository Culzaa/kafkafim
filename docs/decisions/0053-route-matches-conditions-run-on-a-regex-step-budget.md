# ADR-0053: Route `matches` conditions run on a regex step budget

Status: accepted · Date: 2026-11-18 · Refines the `matches` guard of DESIGN §5.3

## Context

A route `matches` condition runs a user-written `java.util.regex` pattern on a string from
the document. Backtracking makes a pattern such as `(.*a){12}` (or `(a+)+$` on older JDKs) take
unbounded time on a value like fifty `a` and a `!`, so one hostile or malformed record could
keep a consumer thread busy until Kafka declares it dead and rebalances the group (#377). The
only guard was catching a `StackOverflowError`, which does not bound time. #159 had chosen to
document that guard and leave it silent (kafkafim stays light, and `route` may not use SLF4J or
Micrometer, ADR-0003).

## Decision

- `matches` evaluates the regex on a `CharSequence` wrapper that counts `charAt` calls and
  throws past a budget of 10,000,000 plus 1,000 per character of the value. The budget is
  generous: ordinary patterns read a few characters per character of input.
- An overrun counts as no match, like the stack-overflow case.
- `Router.select` takes an optional callback that runs once per overrun, so `route` stays
  free of Micrometer. The supervisor passes one that increments
  `kafkafim_route_regex_budget_exceeded_total{pipeline,kafka_cluster}`. There is no new log
  line.

## Consequences

- Recent JDKs memoize some nested-quantifier patterns (`(a+)+$` is polynomial on JDK 27),
  but not all: `(.*a){12}` on a few dozen `a` still never finishes. The budget covers
  whatever the engine does not.
- Worst case per `matches` condition and record, measured on JDK 27: about 80 ms on a short
  value, about 300 ms at 100,000 characters (the budget grows with the value, so a megabyte
  value allows about 3 s). Bounded and far below the poll interval.
- A pattern that needs more than the budget on a legitimate value silently stops matching;
  the counter is how an operator notices. A rising counter means a pattern to simplify.
- The budget is a constant, not configuration (kafkafim stays light); changing it is a code
  change.
- This supersedes the "deliberately silent" wording of DESIGN §5.3 for the budget case. The
  stack-overflow case stays silent.
