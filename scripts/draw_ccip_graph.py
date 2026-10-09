#!/usr/bin/env python
"""Render the CCIP LangGraph workflow as a Mermaid diagram.

Builds the compiled LangGraph with safe in-memory fakes (zero network, no
secrets) and calls ``.get_graph().draw_mermaid()`` to emit the diagram source.
By default the Mermaid text is written to ``docs/ccip_graph.mmd`` at the repo
root; pass ``--stdout`` to print it instead, or ``--output PATH`` to choose a
different destination.

Examples
--------
    python scripts/draw_ccip_graph.py              # write docs/ccip_graph.mmd
    python scripts/draw_ccip_graph.py --stdout      # print to stdout
    python scripts/draw_ccip_graph.py -o diagram.mmd
"""

from __future__ import annotations

import argparse
import os
import sys

# Make ``src`` importable when run as a plain script.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from connector.ccip.config import CcipConfig  # noqa: E402
from connector.ccip.graph import build_graph  # noqa: E402
from connector.ccip.runner import build_fake_deps  # noqa: E402

_DEFAULT_OUTPUT = os.path.join(_ROOT, "docs", "ccip_graph.mmd")


def render_mermaid() -> str:
    """Build the compiled graph with fakes and return its Mermaid source."""
    deps = build_fake_deps(CcipConfig())
    graph = build_graph(deps)
    return graph.get_graph().draw_mermaid()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render the CCIP LangGraph as Mermaid.")
    parser.add_argument(
        "--output", "-o", default=_DEFAULT_OUTPUT,
        help="File to write the Mermaid source to (default: docs/ccip_graph.mmd).",
    )
    parser.add_argument(
        "--stdout", action="store_true",
        help="Print the Mermaid source to stdout instead of writing a file.",
    )
    args = parser.parse_args(argv)

    mermaid = render_mermaid()

    if args.stdout:
        print(mermaid)
        return 0

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(mermaid)
        if not mermaid.endswith("\n"):
            handle.write("\n")
    print(f"Wrote Mermaid diagram to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
