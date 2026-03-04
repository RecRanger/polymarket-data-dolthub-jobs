## Relationship Cheat Sheet for Data Collectors

---

### `complement`
Same market, Yes and No side. Every binary market has exactly one. No searching needed — auto-populate from market structure.

---

### `equivalent`
Two markets in different events whose resolution criteria describe the same real-world fact. Look for near-duplicate market titles across events. Read both resolution criteria and confirm they'd resolve identically.

---

### `implies`
Two markets where one outcome is a *stricter version* of another. Look for markets on the same topic where one adds an extra condition ("wins" vs "wins by 10+", "cuts rates" vs "cuts by 50bps"). Ask: can A be Yes while B is No? If no, A implies B.

---

### `mutex`
Two outcomes that cannot both be true. Look for markets sharing a single winner, title, seat, or location. Ask: is there a rule — legal, physical, or logical — preventing both from resolving Yes?

---

### `composite`
One market whose resolution criteria contains an explicit AND of two other trackable conditions. Look for the word "both", "and", or multiple conditions listed in the resolution text.

---

### `disjunction`
One market whose resolution criteria contains an explicit OR. Look for "either", "or", "at least one of" in resolution text.

---

### `upper_bound` / `lower_bound`
Two markets on the same numeric metric with different thresholds. Look for the same variable (inflation, rate cuts, price targets) appearing in multiple markets with different cutoff numbers.

---

### `conditional`
Two markets where one is a known causal driver of the other. Look for macro indicators paired with their downstream effects (recession → rate cuts, earnings miss → CEO departure). Requires domain judgment — flag and review, don't auto-populate.
