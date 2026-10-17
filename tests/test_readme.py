import ast
from pathlib import Path
import re
import tomllib
import unittest


class ReadmeTests(unittest.TestCase):
    def test_public_quickstart_matches_registry_version(self):
        root = Path(__file__).resolve().parents[1]
        readme = (root / "README.md").read_text()
        version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
        self.assertIn(f"python -m pip install swarm-sdk=={version}", readme)
        self.assertIn("spaces = client.list_spaces()", readme)
        self.assertIn('token=os.environ["SWARM_TOKEN"]', readme)
        self.assertNotIn("does not announce a published release", readme)
        self.assertNotIn("have not been published", readme)
        quickstart = re.search(r"```python\n(.*?)```", readme, re.S)
        self.assertIsNotNone(quickstart)
        ast.parse(quickstart.group(1))
