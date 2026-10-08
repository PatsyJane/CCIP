"""Smoke tests confirming the package scaffolding is importable.

These verify Task 1 wiring only (module layout, pytest configuration). Behavior
for each module is covered by later tasks.
"""

import importlib


def test_connector_package_imports() -> None:
    module = importlib.import_module("connector")
    assert module is not None


def test_all_stub_modules_import() -> None:
    for name in (
        "connector.server",
        "connector.config",
        "connector.http_client",
        "connector.validation",
        "connector.dimensions",
        "connector.numeric",
        "connector.errors",
        "connector.response_adapter",
        "connector.tools",
        "connector.tools.aws_cur_cost",
    ):
        assert importlib.import_module(name) is not None
