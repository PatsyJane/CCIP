# Tech Stack & Commands

## Language & runtime
- **Python >= 3.11** (uses `from __future__ import annotations`, PEP 604 unions,
  `tuple[...]` / `dict[...]` builtins generics).
- Packaging: `setuptools` via `pyproject.toml`, `src/` layout. Importable
  package is `connector` (configured through `[tool.setuptools.packages.find]`
  with `where = ["src"]`).

## Core dependencies (pinned ranges — keep pins intact)
- `fastmcp>=2.12,<3` — MCP server framework (the Connector).
- `httpx>=0.28,<0.29` — async HTTP client for the Cost_API.
- `python-dotenv>=1.0,<2` — `.env` loading.
- `langgraph>=0.2,<2` — StateGraph orchestration of the CCIP workflow.
- `statsmodels>=0.14,<0.15` — STL seasonal-trend decomposition (CCIP detection).
- `pyyaml>=6,<7` — `config.yaml` loader (CCIP, with `${VAR}` injection).
- `langchain-google-genai>=2,<3` — LLM adapter (Gemini) for the CCIP narrative.

## Dev / test dependencies
- `pytest>=8.3,<9`
- `pytest-asyncio>=0.24,<0.25` (`asyncio_mode = "auto"` — no need to decorate
  async tests)
- `hypothesis>=6.112,<7` — property-based testing
- `respx>=0.21,<0.22` — httpx request mocking

## Environment setup (Windows PowerShell)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
Copy-Item .env.example .env   # then fill in required values
```
On Unix: `source .venv/bin/activate`.

## Common commands
```powershell
pytest                                  # run all tests
pytest tests/ccip                       # run the CCIP subset
pytest -k detection                     # run by keyword
connector                               # run the MCP server (console script)
python -m connector.server              # equivalent entry point
python scripts/run_ccip_check.py --account-id A --no-live  # CCIP, zero network
python scripts/run_ccip_check.py --account-id A            # live CCIP run
```

## Configuration
- All runtime config comes from `.env` (loaded via `python-dotenv`); see
  `.env.example` for the full contract. Do not commit `.env`.
- Required Connector keys: `AWS_ACCOUNT_ID`, `AWS_ACCOUNT_NAME`, `AWS_REGION`,
  `COST_API_BASE_URL`, `PROVIDER`, `PROJECT`. Startup aborts (non-zero exit)
  naming the first missing/blank required key. Empty or whitespace-only values
  count as missing.
- AWS credentials (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) are optional
  and loaded only where needed; providing only one is a misconfiguration.
- CCIP thresholds/STL params live in `config.yaml` with `${VAR}` placeholders
  injected from `.env`; CCIP env keys are prefixed `CCIP_` (LLM, SMTP, detection
  tuning, metadata path). `config.yaml` supports `${VAR}` and `${VAR:-default}`.

## HTTP conventions
- Use the single shared `HttpClient` wrapper (`http_client.py`) built around one
  `httpx.AsyncClient`; reuse it across invocations rather than creating clients
  ad hoc. Default timeout is **30s** (Req 11.1).
