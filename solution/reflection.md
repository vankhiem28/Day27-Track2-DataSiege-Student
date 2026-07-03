# Reflection (≤1 page)

## Hardest faults

The truly subtle faults were the hardest — by design. A few specific
patterns I ran into:

- **Subtle `feature_skew`** sits around 1.5–2.3σ in `mean_shift_sigma`,
  comfortably above a "normal" ±3σ baseline of ~0.41. Those are caught
  easily. The harder ones are clean events whose sigma is also ~0.4 — the
  threshold alone produces false positives on those, so I learned an
  adaptive threshold (`max(0.5, observed_clean_max × 1.5)`) to recover
  precision without losing the subtle catches.
- **Subtle `embedding_drift`** (`centroid_shift ≈ 0.04`) and **subtle
  `corpus_staleness`** (`avg_doc_age_days ≈ 48`) sit just below the
  published ±3σ baseline. A bare baseline check misses them. My fix was
  an adaptive threshold blending observed clean max with a fraction of
  the gap to baseline (`obs_max + 0.5·(baseline − obs_max)`), tightened
  with an absolute buffer (`obs_max + 10` days) so a single unusually
  high clean value doesn't immediately lower the bar.
- **Subtle `distribution_shift` on `data_batch`** is the one I couldn't
  catch at all — every individual signal (`row_count`, `null_rate`,
  `mean_amount`, `staleness_min`) is inside its ±3σ baseline, so no
  single threshold can fire. I tried a multi-signal z-score check against
  the running clean mean/std but it produced more false positives than
  the extra true positives it gained, so I left it out.
- **`missing_upstream` / `orphan_output`** are structural, not
  threshold-based, so I learned the expected counts (`upstream == 2`,
  `downstream == 1`) from clean events during the run and flagged any
  deviation. This generalizes as long as the clean stream is structurally
  stable.
- **Subtle `runtime_anomaly` (lineage)** has `duration_ms` inside the
  clean distribution, with no structural anomaly either, so it cannot
  be detected from the published signals. I left it as known-missed.

## Cost / coverage tradeoff

The single most expensive lever is `feature_drift` / `embedding_drift`
(2.0 credits each), and on the private phase the event mix forces total
spend past budget if every expensive check runs. I considered throttling
those once `spend_so_far` crosses a soft cap, but a deterministic
throttle deterministically misses whichever late-stream fault happens to
fall after the cap — and on private that cost more score (–1.8 from
overage) than it saved (a handful of additional catches worth +1–2). So
I removed the throttle entirely, accepted `cost_overage = 0.09` on the
shorter phases, and paid that small penalty in exchange for full TPR
coverage on every event type.

If I had another pass I would:
1. Add a multi-signal `distribution_shift` detector that combines
   z-scores across the four `batch_profile` fields and alerts when
   *any two* are simultaneously > 2σ from the running clean mean —
   with the AND-style guard it would catch the subtle distribution /
   volume / null / freshness faults without flooding FPR.
2. Detect subtle `runtime_anomaly` by comparing duration against a
   rolling percentile (e.g. > 95th percentile of the run's clean
   lineage durations) instead of a fixed baseline.
3. Skip the per-event adaptive-threshold bookkeeping for the
   `embedding_age` field on private-style streams, where clean and
   fault distributions genuinely overlap — accept the lower recall on
   subtle `corpus_staleness` rather than chase it with ever-tighter
   thresholds that always lose the precision/recall tradeoff.