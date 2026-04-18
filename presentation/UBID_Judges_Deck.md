---
marp: true
theme: default
paginate: true
size: 16:9
---

# UBID Platform
## Karnataka Unified Business Intelligence
### Judges Evaluation Deck

**Team:** UBID Platform  
**Date:** 18 Apr 2026

---

# 1. Problem We Solve

Government business data is fragmented across departments.

- Same business appears differently in each system
- Manual matching is slow and error-prone
- Risky entities are missed due to identity fragmentation
- Inspection and compliance action is not prioritized by evidence

**Impact:** low enforcement efficiency, high review workload, delayed decisions.

---

# 2. Why This Matters Now

Karnataka needs a **single, trusted business view** for:

- Better inspection targeting
- Faster compliance interventions
- Reduced duplicate effort across departments
- Leadership decisions grounded in live signals

**Goal:** move from static records to operational intelligence.

---

# 3. Our Solution

UBID creates one explainable identity per business and powers action workflows.

Core capabilities:

- Cross-department identity resolution (UBID)
- Human-in-the-loop review and escalation
- Activity and compliance intelligence
- Risk graph and shell-pattern detection
- Trust Score + Policy-to-Action workflows
- Department and executive dashboards

---

# 4. What Is Unique

1. **Identity + Activity + Action in one loop**
2. **Human-governed learning** (no blind auto-override)
3. **Explainability by default** (evidence for every major decision)
4. **Public-sector operational workflows**, not just analytics

**Positioning:** This is not just MDM; it is an execution system for governance.

---

# 5. Architecture Snapshot

Data Layer:

- Department source records + events
- UBID master + linkages + audit logs

Intelligence Layer:

- Matching and review engine
- Network graph analytics
- Trust Score engine
- Watchlist and policy simulation

Action Layer:

- Inspection priority queue
- Renewal risk queue
- Shell review bundles
- Scorecards and executive views

---

# 6. Live Evidence (Current Run)

From running APIs in this environment:

- **231** unified businesses (UBIDs)
- **296** source records linked
- **634** activity events processed
- **22.0%** deduplication impact
- **54** pending human reviews

Graph intelligence:

- **18** suspicious clusters
- **21** potential shell pairs

---

# 7. Live Evidence (Operational Intelligence)

- Departments scored: **4**
- Lowest department score: **35.12**
- Active businesses: **76 / 231**
- Watchlist alerts currently detected: **22**
- Inspection priority queue size: **22**
- Top trust score observed: **97.89**

These are generated from live endpoints, not static slide assumptions.

---

# 8. Judge Demo Flow (6 minutes)

1. Search a business across messy identifiers
2. Open evidence view for why records are linked
3. Show network graph suspicious clusters
4. Show Trust Score breakdown for one UBID
5. Open inspection priority workflow queue
6. Show department scorecards and executive heatmap

**Message:** we convert fragmented records into explainable, actionable governance decisions.

---

# 9. Measurable Outcomes We Target

Pilot success metrics:

- Reduction in duplicate business fragmentation
- Increase in inspection hit-rate on risky entities
- Reduction in review effort per 1,000 records
- Faster detection of renewal/compliance risk
- Lower false-merge rate with human-controlled learning

---

# 10. Governance and Safety

- Human approval remains authoritative
- Learning updates only when explicitly applied
- Every critical action is auditable
- Explainable scoring and workflow reasons available

**Principle:** AI assists officers; officers decide outcomes.

---

# 11. Market Reality and Our Edge

Yes, category tools exist.

Our edge is execution in public-sector context:

- Department-aware identity fusion
- Risk graph + trust scoring + action queues
- Human-governed continuous learning
- Audit-ready decisions for government use

---

# 12. Ask to Judges

Evaluate us on 3 hard questions:

1. Did we unify fragmented data into trusted identity?
2. Did we convert intelligence into actionable workflows?
3. Is the system explainable and safe for real governance use?

**We believe UBID passes all three.**

---

# Backup: Key Endpoints Used in Demo

- `/api/search/universal`
- `/api/ubid/<ubid>/evidence`
- `/api/graph/network`
- `/api/trust-score/<ubid>`
- `/api/workflows/inspection-priority`
- `/api/workflows/renewal-risk`
- `/api/workflows/shell-review-bundles`
- `/api/department/scorecards`
- `/api/executive/dashboard`
