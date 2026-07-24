import os
import sys
import types

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

for module_dir in ('fetch', 'process'):
    path = os.path.join(REPO_ROOT, module_dir)
    if path not in sys.path:
        sys.path.insert(0, path)

# psycopg2 is deployed to Lambda via a separate layer (see psycopg2-layer/),
# not pip-installed, and its binary wheel isn't available on every dev/CI
# platform. Tests only ever exercise it through a mock, so stub it out here
# when the real package isn't installed rather than requiring a compiler.
try:
    import psycopg2  # noqa: F401
except ImportError:
    stub = types.ModuleType('psycopg2')
    stub.connect = lambda **kwargs: None
    sys.modules['psycopg2'] = stub

os.environ.setdefault('DB_SECRET_ARN', 'arn:aws:secretsmanager:eu-west-2:000000000000:secret:test-db-credentials')
