---
name: backtrader-sviluppo
description: Coordinate and implement feature development and code changes across Backtrader services, including integration, verification, and review; also use for fixes that proceed to implementation, not for strategy research.
---

# Pipeline di sviluppo Backtrader

Deliver the requested behavior end to end. Follow the workspace AGENTS.md, the user's scope and existing authorizations. Scale the pipeline to the task: use direct execution for a small change; coordinate parallel work and independent review when complexity or risk warrants it and delegation is available and authorized.

## 1. Define the change

Identify the requested behavior, acceptance checks, affected environment and any constraints. Inspect relevant nested repositories, existing working-tree changes, implementation, contracts and tests. Identify the authoritative producer of data or calculations, affected consumers and saved-configuration compatibility. Preserve unrelated changes and operational state. If the user requests a plan before editing, provide it and wait as instructed; otherwise resolve routine choices and proceed.

## 2. Assign work and implement

The coordinator owns scope, integration and the final outcome. Reuse existing components and contracts where appropriate; keep backtest, paper and live scope explicit. If parallel work is useful and permitted, assign bounded tasks to implementation agents with non-overlapping file ownership, required context, acceptance checks, permitted actions and expected test evidence. Split by genuinely independent components or investigation, not by arbitrary quotas. Agents must report files changed, checks and unresolved issues; the coordinator integrates their work, checks shared contracts and preserves pre-existing changes. Avoid simultaneous edits to shared files and coordinate dependencies before integration.

## 3. Verify and review

Add meaningful regression coverage for fixes, especially the areas named in AGENTS.md. Run focused tests first, then the full affected service suites required there, and `git diff --check` in affected repositories. Check the requested behavior across service boundaries and, for UI changes, the actual user flow where practical, including served or authenticated behavior when relevant. An SPA build does not update the production PWA; publication requires the applicable authorization.

For complex or high-risk changes, when delegation is available and authorized, ask an independent reviewer who did not implement the change to inspect the actual diff, requirements and verification evidence for correctness, regressions and missing cases. Give the reviewer read-only scope and ask for actionable findings with file locations and severity. The coordinator evaluates the findings, fixes valid issues, reruns affected checks and rechecks the final diff; seek another review when a material change warrants it. When no independent review occurs, say so rather than implying one.

## 4. Deliver

Compare the final result against the acceptance checks. Report the behavior delivered, relevant verification and any failing, skipped or blocked checks separately, plus material limitations. Never claim a full suite passed based on focused tests. Continue through authorized reversible work; if a required external action falls outside the user's existing authorization, prepare a concrete reviewable result before requesting it. Commits, pushes, restarts, deployment and live-order changes remain within their expressly authorized scope. Apply the replay or backtest coverage required by AGENTS.md before deployment of live-order changes.
