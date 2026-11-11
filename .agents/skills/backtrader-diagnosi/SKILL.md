---
name: backtrader-diagnosi
description: Diagnose operational failures, degraded services, data flows, and runtime discrepancies in the Backtrader workspace; use for investigating a concrete incident or reported malfunction.
---

# Diagnosi operativa Backtrader

Identify the affected behavior, establish its cause as far as evidence permits, and resolve it when the user's request authorizes a fix. Follow the workspace AGENTS.md and the user's scope and existing authorizations.

## Coordinate the investigation

- Establish the symptom, time window, affected environment and expected behavior. Inspect the actual checkout, running service, configuration and data path involved; do not assume development and production are the same.
- When delegation is available and authorized, use independent agents only when investigations can proceed separately or an independent challenge adds value. For example, split runtime/log evidence from code/configuration tracing; give each agent a bounded question, relevant paths, read/write limits and expected evidence. The coordinator checks their findings and resolves contradictions. If delegation is unavailable, do the same work directly and do not claim an independent review.
- Gather bounded, relevant evidence before changing state. Limit log time windows and file paths; avoid recursive searches across large archives or operational data. Account for the load diagnostic commands themselves can impose.
- Trace the observed failure through the relevant layers and test competing explanations where practical. Distinguish measured facts, inference, unverified hypotheses and missing evidence. Service readiness alone does not establish end-to-end data flow.
- If a correction is in scope, implement the smallest effective fix, preserve unrelated work and operational state, and verify the original symptom and affected dependencies. For live-order behavior use replay or backtest coverage before deployment as required by AGENTS.md.
- Respect existing authorizations for restarts, production changes, publication, commits and external actions. Prepare a concrete, reviewable intervention if a required action is outside the authorized scope; do not stop for approval where the user already authorized it.

Report the outcome and supporting evidence, the correction and verification if performed, and remaining uncertainty or blocked checks. Do not present a correlation as a proven cause.
