# Project Structure & Conventions

## Layout
```
.kiro/
  specs/                      # authoritative requirements/design/tasks per feature
    billing-usage-ingestion/  # requirements.md, design.md, tasks.md
    ccip-langgraph-anomaly/   # requirements.md, design.md, tasks.md
  settings/mcp.json           # MCP server config
  steering/                   # this guidance

src/connector/                # importable package: `connector`
  server.py                   # bootstrap: FastMCP server + wiring + run entry point
  config.py                   # .env loader / AppConfig (fail-fast, credential redaction)
  http_client.py              # shared httpx.AsyncClient wrapper
  validation.py               # date / range / granularity validation
  dimensions.py               # account/service/environment tagging + normalization
  numeric.py                  # Cost_Value coercion + range enforcement
  errors.py                   # structured-error factory + ErrorCategory enum
  response_adapter.py         # defensive Cost_API response parsing
  tools/aws_cur_cost.py       # the AWS CUR Cost_Tool pipeline
  ccip/                       # CCIP LangGraph anomaly workflow
    config.py                 # config.yaml + ${VAR} loader / CcipConfig
    state.py                  # CcipState TypedDict + initial-state builder
    models.py                 # frozen models + DataMode enum
    history.py                # history counting + data-mode classification
    metadata_store.py         # single JSON store, atomic writes
    ingest.py                 # Phase 1 fetch + Phase 2 pure normalize
    stl_model.py              # statsmodels STL + residual thresholds
    detection.py              # cold-start + statistical detection
    drilldown.py              # service contributor isolation
    narrative.py              # Gemini narrative + deterministic fallback
    llm_client.py             # Gemini (LangChain) adapter
    email.py                  # payload build + SMTP delivery (dry-run)
    nodes.py                  # the six LangGraph nodes
    routers.py                # the two conditional routers
    graph.py                  # build_graph(): compiled StateGraph
    redaction.py              # secret redaction helper
    runner.py                 # dependency wiring + run_once

config.yaml                   # CCIP thresholds + STL params (${VAR} injected)
scripts/run_ccip_check.py     # CCIP workflow CLI entry point
tests/                        # pytest + pytest-asyncio + hypothesis
  ccip/                       # unit, property (test_properties.py), graph smoke
```

## Design-component mapping
Each core module corresponds to a numbered design component in the matching
spec's `design.md` (e.g. `server.py` is "design component 1", `errors.py` is
"component 8"). When editing a module, consult its spec design section first and
keep the component boundary intact.

## Coding conventions
- Start modules with `from __future__ import annotations`.
- Every module, class, and public function gets a docstring. Docstrings and
  inline comments **cite the governing requirement IDs** (e.g. `Req 10.3`).
  Preserve these citations; add new ones when implementing to a requirement.
- Prefer **immutable** domain models: `@dataclass(frozen=True)`.
- Declare the module's public surface with `__all__`.
- Use explicit, named exceptions (`ConfigError`, `ToolRegistrationError`, …);
  messages must name the offending value/tool.
- MCP tools and I/O-bound functions are **async** (`async def`). The AWS CUR
  Cost_Tool is registered as a coroutine (Req 1.3).
- **Errors over exceptions at tool boundaries:** Ingestion_Tools return
  `structured_error(message, ErrorCategory.X, **extra)` objects instead of
  raising/crashing. Only use the four `ErrorCategory` values.
- **Secret safety:** never put credentials/keys/passwords in logs, repr, or tool
  output. Follow the pattern in `config.py` (fields excluded from `repr`,
  overridden `__repr__` returning `***`) and `ccip/redaction.py`.

## Testing conventions
- `pytest.ini_options` sets `asyncio_mode = "auto"` and `pythonpath = ["src"]`;
  async tests need no explicit marker, and `connector` imports directly.
- Property-based tests live in `test_*_property.py` using Hypothesis; shared
  Hypothesis strategies/property tests go in `tests/ccip/test_properties.py`,
  fakes/stubs in `tests/ccip/fakes.py`.
- Mock outbound HTTP with `respx` rather than hitting real endpoints.
- Separate unit (`test_*_unit.py`), property, integration
  (`test_integration_*`), and smoke (`test_*_smoke.py`) suites.
- Do not add tests unless the task calls for them, but when fixing a bug or
  adding behavior that is tested elsewhere, follow the existing suite patterns.

## Spec workflow
`.kiro/specs/<feature>/` holds `requirements.md`, `design.md`, and `tasks.md`.
Treat these as the source of truth: implement against the requirements, keep
code aligned with the design components, and work tasks in order.
