import importlib.util
import os


def load_lambda_module(function_dir: str, env: dict | None = None):
    """Load a Lambda function's app.py as an isolated module.

    Each function directory has its own app.py; loading by file path under a
    module name derived from the directory (rather than `import app`) keeps
    modules from different functions from colliding in sys.modules.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    module_path = os.path.join(repo_root, function_dir, "app.py")

    for key, value in (env or {}).items():
        os.environ.setdefault(key, value)

    spec = importlib.util.spec_from_file_location(f"{function_dir}_app", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
