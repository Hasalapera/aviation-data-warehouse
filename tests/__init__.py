"""Test package bootstrap.

This module is imported before any test module (package discovery order),
so the disposable test-warehouse name is forced BEFORE any application
module resolves its connection settings. load_dotenv() never overrides an
already-set environment variable, therefore no test can ever open the live
aviation_dw for writing.
"""

import os

os.environ["DW_NAME"] = "aviation_dw_ci"

TEST_DB_NAME = "aviation_dw_ci"
