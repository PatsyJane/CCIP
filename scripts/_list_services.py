"""Throwaway helper: list the Phase 2 / Phase 3 / Phase 5 service sets for Oct 5
from the persisted per-account metadata store (no network)."""
from __future__ import annotations
import json, os, sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
store = os.path.join(_ROOT, "metadata", "ccip_metadata_Pulse_647096474516.json")
md = json.load(open(store, encoding="utf-8"))

TARGET = "2026-10-05"
svc_hist = md["service_history"]

# Phase 3: the 35 window services (all services in service_history, excl. UNKNOWN).
window_services = sorted(s for s in svc_hist if s != "UNKNOWN")

# Phase 2: the 29 services with a line item ON the target day (service_costs keys).
target_day_services = {}
for s, rows in svc_hist.items():
    if s == "UNKNOWN":
        continue
    for r in rows:
        if str(r.get("day", ""))[:10] == TARGET:
            target_day_services[s] = round(float(r.get("cost", 0.0)), 2)
            break

# Phase 5: contributors = target-day services with cost > 0 (baseline 0 cold start),
# ordered by descending delta (== target cost here). Mirror drilldown logic.
contributors = sorted(
    ((s, c) for s, c in target_day_services.items() if c > 0),
    key=lambda kv: kv[1], reverse=True,
)

print("=== PHASE 2: services present on the target day (service_costs) ===")
print("count:", len(target_day_services))
for i, (s, c) in enumerate(sorted(target_day_services.items()), 1):
    print(f"{i:>2}. {s}  = ${c}")

print("\n=== PHASE 3: window services / new_services_detected ===")
print("count:", len(window_services))
for i, s in enumerate(window_services, 1):
    print(f"{i:>2}. {s}")

print("\n=== PHASE 5: contributors (service: +delta, baseline 0.0 -> current) ===")
print("count:", len(contributors))
for i, (s, c) in enumerate(contributors, 1):
    print(f"{i:>2}. {s}  +{c}  (0.0 -> {c})")

# Services in the window but NOT billed on the target day (explains 35 vs 29).
not_on_target = sorted(set(window_services) - set(target_day_services))
print("\n=== Window services with NO line item on the target day (35 - 29) ===")
print("count:", len(not_on_target))
for s in not_on_target:
    print(" -", s)

# Target-day services that are zero-cost (present but $0; in 29 but not in 26).
zero_on_target = sorted(s for s, c in target_day_services.items() if c <= 0)
print("\n=== Target-day services with $0 cost (in Phase 2's 29 but not Phase 5's 26) ===")
print("count:", len(zero_on_target))
for s in zero_on_target:
    print(" -", s)


# --- Diagnostic: show the tail of history for the window-only + zero services. ---
print("\n\n=== Tail of history for selected services (last 4 recorded days) ===")
for s in [
    "AWS CloudShell", "AWS X-Ray", "Amazon Athena", "Amazon GuardDuty",
    "Amazon QuickSight", "Amazon Simple Email Service",
    "Amazon Lightsail", "Amazon Polly", "Amazon Simple Notification Service",
]:
    rows = svc_hist.get(s, [])
    tail = rows[-4:]
    has_target = any(str(r.get("day", ""))[:10] == TARGET for r in rows)
    shown = ", ".join(f"{r['day']}=${round(float(r['cost']),2)}" for r in tail)
    print(f"\n{s}")
    print(f"  total recorded days: {len(rows)} | has Oct-05 row: {has_target}")
    print(f"  last days: {shown if shown else '(none)'}")
