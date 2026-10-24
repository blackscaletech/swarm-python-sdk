"""Offline sdist-to-installed-wheel gate; uses pre-provisioned pinned build tools."""
import argparse
import ast
import base64
import csv
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import typing
import unittest
import zipfile

TEST_FILES = ("sdk_test_support.py", "test_attempt_idempotency.py", "test_verified_artifact_content.py",
              "test_authority_normalization.py", "test_operation_policy.py", "test_public_contract.py", "test_query_merge.py")
ROOT_FILES = ("pyproject.toml", "MANIFEST.in", "README.md", "LICENSE", "CHANGELOG.md",
              "SECURITY.md", "CONTRIBUTING.md", ".gitignore")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def offline_audit(event, args):
    if event.startswith("socket.") and event != "socket.gethostname":
        raise RuntimeError("Packaged consumer prohibits network and DNS")


def worker(mode):
    cfg = json.load(sys.stdin)
    sys.addaudithook(offline_audit)
    assert sys.dont_write_bytecode
    assert sys.flags.optimize == 0, "This gate requires assertions enabled"
    if mode == "sdist":
        from setuptools.build_meta import build_sdist
        print("PACKAGED_RESULT=" + json.dumps({"sdist": build_sdist(cfg["dist"])}))
        return 0
    if mode == "build":
        from setuptools.build_meta import build_wheel
        print("PACKAGED_RESULT=" + json.dumps({"wheel": build_wheel(cfg["dist"])}))
        return 0
    if mode == "install":
        # Pip's public CLI can target a pip-free venv without injecting module paths.
        sys.argv = ["pip", "--python", cfg["python"], "--isolated", "install", "--no-index",
                    "--no-deps", "--no-cache-dir", "--no-compile", cfg["wheel"]]
        runpy.run_module("pip", run_name="__main__")
        return 0
    repo = Path(cfg["repo"]).resolve()
    owned = Path(cfg["owned"]).resolve()
    allowed = (Path(cfg["tests"]).resolve(), owned / "tmp",
               Path(sys.base_prefix).resolve(), Path(sys.prefix).resolve())

    def outside_repo(event, args):
        if event == "open" and args and isinstance(args[0], (str, bytes)):
            path = Path(os.fsdecode(args[0])).resolve()
            if path not in (Path(os.devnull), owned / "consumer.py") and not any(path.is_relative_to(root) for root in allowed):
                raise RuntimeError("Installed consumer prohibits original-source and private-source reads: " + str(path))

    assert all(not Path(p).resolve().is_relative_to(repo) or Path(p).resolve().is_relative_to(owned)
               for p in sys.path if p)
    sys.addaudithook(outside_repo)

    def must_be_denied(probe):
        try:
            probe()
        except RuntimeError:
            return
        raise AssertionError("Isolation probe was not refused")

    must_be_denied(lambda: (repo / "README.md").read_bytes())
    must_be_denied(lambda: (owned / "source/README.md").read_bytes())
    must_be_denied(lambda: socket.getaddrinfo("synthetic.invalid", 443))
    must_be_denied(lambda: socket.socket())
    import swarm_sdk
    from swarm_sdk.client import HTTPResponse

    package = Path(swarm_sdk.__file__).resolve().parent
    assert package.is_relative_to(Path(sys.prefix).resolve())
    assert sorted(swarm_sdk.__all__) == sorted(cfg["exports"])
    assert all(callable(getattr(swarm_sdk, name)) for name in swarm_sdk.__all__)
    assert set(typing.get_type_hints(HTTPResponse)) == {"status", "headers", "body"}
    assert set(typing.get_type_hints(swarm_sdk.ExecutionParticipant)) == {
        "id", "executor_identity_id", "name", "kind", "is_self", "can_execute", "created_at",
    }
    for name, expected in cfg["package_hashes"].items():
        assert digest((package.parent / name).read_bytes()) == expected, name
    assert {p.relative_to(package.parent).as_posix() for p in package.rglob("*") if p.is_file()} == set(cfg["package_hashes"])
    dist = importlib.metadata.distribution("swarm-sdk")
    assert dist.version == cfg["version"] and dist.metadata["Requires-Python"] == ">=3.11"
    assert "Tag: py3-none-any" in dist.read_text("WHEEL")
    assert dist.metadata["License-Expression"] == "Apache-2.0"
    assert dist.metadata.get_all("License-File") == ["LICENSE"]
    assert not dist.requires
    assert any(url.startswith("Repository, https://github.com/") for url in dist.metadata.get_all("Project-URL"))
    # Resolve annotations in the installed modules, not only their source ASTs.
    assert typing.get_type_hints(swarm_sdk.SwarmClient.__init__)["base_url"] is str
    assert typing.get_type_hints(swarm_sdk.VerifiedArtifactDownload.read)["return"] is bytes

    class Raw(io.BytesIO):
        def __init__(self, body, expected=None):
            super().__init__(body)
            self.status = 200
            raw = body if expected is None else expected
            sha = digest(raw)
            self.headers = {"content-type": "application/octet-stream", "x-swarm-artifact-id": "art_packaged",
                            "x-swarm-content-id": "sha256:" + sha, "x-swarm-content-sha256": sha,
                            "x-swarm-content-length": str(len(raw)), "content-length": str(len(raw)),
                            "etag": '"sha256:' + sha + '"', "cache-control": "no-store, no-transform"}

        def read(self, size=-1):
            return super().read(min(size, 37001) if size >= 0 else 37001)

    class Transport:
        def __init__(self, body, expected=None):
            self.body, self.expected = body, expected
            self.response = None

        def stream(self, method, url, headers, timeout):
            assert method == "GET" and headers["Accept"] == "application/octet-stream"
            self.response = Raw(self.body, self.expected)
            return self.response

    class InstalledBinaryConsumer(unittest.TestCase):
        def test_complete_multichunk_and_empty_bytes(self):
            for body in (bytes(range(256)) * 8192 + b"complete-tail\x00\xff", b""):
                transport = Transport(body)
                client = swarm_sdk.SwarmClient(base_url="https://synthetic.invalid", transport=transport)
                with client.download_artifact("art_packaged", expected_sha256=digest(body)) as result:
                    self.assertEqual(result.read(), body)
                    self.assertEqual((result.sha256, result.size_bytes), (digest(body), len(body)))
                self.assertTrue(result.closed and transport.response.closed)

        def test_tamper_and_byte_boundary_close_resources(self):
            body = b"synthetic exact bytes" * 4096
            for transport, limit, error in (
                (Transport(body[:-1] + b"X", body), len(body), swarm_sdk.SwarmArtifactIntegrityError),
                (Transport(body), len(body) - 1, swarm_sdk.SwarmResponseLimitError),
            ):
                client = swarm_sdk.SwarmClient(base_url="https://synthetic.invalid", transport=transport)
                with self.assertRaises(error):
                    client.download_artifact("art_packaged", max_artifact_bytes=limit)
                self.assertTrue(transport.response.closed)

        def test_typed_denial_and_no_authority_widening(self):
            canary = "swc_v2." + "a" * 16 + "." + "A" * 43

            class Denied:
                def request(self, method, url, headers, body, timeout):
                    return HTTPResponse(403, {"content-type": "application/json"}, json.dumps({"error": "denied", "connection_token": canary}).encode())
            client = swarm_sdk.SwarmClient(base_url="https://synthetic.invalid", transport=Denied())
            with self.assertRaises(swarm_sdk.SwarmAPIError) as failure:
                client.get("/v1/spaces")
            self.assertEqual(failure.exception.status, 403)
            self.assertNotIn(canary, str(failure.exception))
            with self.assertRaises(ValueError):
                client.launch_space_run("sp_packaged", title="No dispatch", run_requirements={"invented_authority": ["identity"]})

    original = unittest.defaultTestLoader.discover(cfg["tests"], pattern="test_*.py")
    original_count = original.countTestCases()
    assert original_count == cfg["original_test_count"] and original_count > 0
    suite = unittest.TestSuite([original, unittest.defaultTestLoader.loadTestsFromTestCase(InstalledBinaryConsumer)])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    origins = {}
    for name, module in sys.modules.items():
        if name == "swarm_sdk" or name.startswith("swarm_sdk."):
            origin = Path(module.__file__).resolve()
            assert origin.is_relative_to(Path(sys.prefix).resolve()), name
            origins[name] = str(origin)
    assert all(not Path(p).resolve().is_relative_to(repo) or Path(p).resolve().is_relative_to(owned)
               for p in sys.path if p)
    report = {"original_installed_tests": original_count, "binary_consumer_tests": 3, "tests": result.testsRun,
              "failures": len(result.failures), "errors": len(result.errors), "skipped": len(result.skipped),
              "module_origins": origins, "runtime": sys.version.split()[0], "bytecode_disabled": True,
              "type_hints_resolved": True, "original_source_reads_denied": True, "network_denied": True}
    print("PACKAGED_RESULT=" + json.dumps(report, sort_keys=True))
    return 0 if result.wasSuccessful() and not result.skipped else 1


def main():
    sdk = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, help="Optional fresh receipt directory; never replaces prior evidence")
    parser.add_argument("--dist-dir", type=Path, help="Optional fresh directory for the verified wheel and sdist")
    args = parser.parse_args()
    if sys.flags.optimize:
        parser.error("assertions must be enabled (do not use -O or PYTHONOPTIMIZE)")

    def fresh_output(value):
        if value is None:
            return None
        path = (sdk / value).resolve()
        if not path.is_relative_to(sdk) or path == sdk or path.exists():
            parser.error("output directories must be fresh and inside this checkout")
        return path

    evidence, dist_dir = fresh_output(args.evidence), fresh_output(args.dist_dir)
    if evidence and dist_dir and (evidence.is_relative_to(dist_dir) or dist_dir.is_relative_to(evidence)):
        parser.error("evidence and distribution directories must not overlap")
    if evidence:
        evidence.mkdir(parents=True, mode=0o700)
    sys.dont_write_bytecode = True
    sys.addaudithook(offline_audit)
    try:
        pins = dict(line.strip().split("==") for line in (sdk / "scripts/requirements-packaging.txt").read_text().splitlines()
                    if line.strip() and not line.startswith("#"))
        versions = {name: importlib.metadata.version(name) for name in pins}
        if versions != pins:
            raise RuntimeError("Pre-provisioned build tools must match scripts/requirements-packaging.txt: " + str(versions))
    except (ImportError, importlib.metadata.PackageNotFoundError) as error:
        raise SystemExit("Offline packaging runtime must already provide the pinned build tools; nothing will be downloaded: " + str(error))
    package_files = sorted(p for p in (sdk / "swarm_sdk").iterdir() if p.is_file())
    assert all(p.suffix == ".py" or p.name in ("client-contract.json", "py.typed") for p in package_files)
    inputs = [*package_files, *[sdk / name for name in ROOT_FILES], *sorted((sdk / "tests").glob("*.py")),
              *sorted((sdk / "scripts").glob("*.py")), *sorted((sdk / "scripts").glob("*.txt")),
              *sorted((sdk / ".github/workflows").glob("*.yml"))]
    assert all(not p.is_symlink() for p in inputs)
    before = {p.relative_to(sdk).as_posix(): digest(p.read_bytes()) for p in inputs}
    version = tomllib.loads((sdk / "pyproject.toml").read_text())["project"]["version"]
    report = {"passed": False, "runtime": sys.version.split()[0], "packaging_runtime": versions,
              "steps": [], "source_before": before, "scope": "Offline sdist roundtrip and installed wheel subset/exports/resources/types/hash/byte-boundary; not full SDK, live API, provider, release or platform acceptance"}
    report["harness_sha256"] = digest(Path(__file__).read_bytes())
    work = sdk / ".packaged-work"; work.mkdir(exist_ok=True)
    owned = Path(tempfile.mkdtemp(prefix="gate-", dir=work))
    try:
        home = owned / "home"; home.mkdir()
        tmp = owned / "tmp"; tmp.mkdir()
        env = {"PATH": "/usr/bin:/bin", "HOME": str(home), "TMPDIR": str(tmp), "TMP": str(tmp), "TEMP": str(tmp),
               "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUTF8": "1", "PIP_NO_INDEX": "1", "PIP_CONFIG_FILE": os.devnull,
               "PIP_DISABLE_PIP_VERSION_CHECK": "1", "SOURCE_DATE_EPOCH": "315532800",
               "PYTHONHASHSEED": "0", "NO_PROXY": "*"}
        copied = owned / "consumer.py"; shutil.copyfile(__file__, copied)

        def run(name, executable, mode, cwd, cfg):
            with tempfile.TemporaryFile(dir=tmp) as output:
                process = subprocess.Popen([str(executable), "-I", "-B", str(copied), mode], cwd=cwd, env=env,
                                           stdin=subprocess.PIPE, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
                process.stdin.write(json.dumps(cfg).encode()); process.stdin.close()
                deadline = time.monotonic() + 120
                while process.poll() is None:
                    if time.monotonic() > deadline or os.fstat(output.fileno()).st_size > 256 * 1024:
                        if os.name == "posix":
                            os.killpg(process.pid, signal.SIGKILL)
                        else:
                            process.kill()
                        process.wait()
                        raise RuntimeError("Packaged check exceeded120s/256KiB bound")
                    time.sleep(0.02)
                output.seek(0); data = output.read(256 * 1024 + 1)
                if len(data) > 256 * 1024:
                    raise RuntimeError("Packaged check output exceeds256KiB")
            raw = data.decode("utf-8", "replace")
            report["steps"].append({"name": name, "exit_code": process.returncode, "output_sha256": digest(data)})
            if evidence:
                (evidence / (name + ".log")).write_text(raw)
            if process.returncode:
                raise RuntimeError(name + " failed: " + raw[-2000:])
            lines = [line.removeprefix("PACKAGED_RESULT=") for line in raw.splitlines() if line.startswith("PACKAGED_RESULT=")]
            return json.loads(lines[-1]) if lines else None

        source = owned / "source"; source.mkdir()
        for p in inputs:
            target = source / p.relative_to(sdk)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(p, target)
        package = source / "swarm_sdk"
        tests = owned / "tests"; tests.mkdir()
        for name in TEST_FILES:
            shutil.copyfile(sdk / "tests" / name, tests / name)
        original_count = sum(method.name.startswith("test_") for name in TEST_FILES if name.startswith("test_")
                             for cls in ast.parse((tests / name).read_text()).body if isinstance(cls, ast.ClassDef)
                             for method in cls.body if isinstance(method, ast.FunctionDef))
        wheels = owned / "wheels"; wheels.mkdir()
        built_sdist = run("sdist", sys.executable, "sdist", source, {"dist": str(wheels)})
        sdist = wheels / built_sdist["sdist"]
        extracted = owned / "extracted"; extracted.mkdir()
        with tarfile.open(sdist) as archive:
            members = archive.getmembers()
            names = [member.name for member in members]
            assert len(set(names)) == len(names)
            prefix = "swarm_sdk-" + version
            for member in members:
                path = Path(member.name)
                assert not path.is_absolute() and ".." not in path.parts and path.parts[0] == prefix
                assert member.isfile() or member.isdir(), "No links or special files in sdist"
                if member.isfile():
                    relative = path.relative_to(prefix).as_posix()
                    assert relative in before or relative == "PKG-INFO" or relative == "setup.cfg" or (
                        relative.startswith("swarm_sdk.egg-info/") and len(Path(relative).parts) == 2), relative
                    if relative in before:
                        assert digest(archive.extractfile(member).read()) == before[relative], relative
            assert set(before).issubset({Path(name).relative_to(prefix).as_posix() for name in names})
            archive.extractall(extracted, filter="data")
        report["sdist"] = {"filename": sdist.name, "sha256": digest(sdist.read_bytes()),
                           "all_public_inputs_verified": True, "tests_included": True}
        built = run("build", sys.executable, "build", extracted / prefix, {"dist": str(wheels)})
        wheel = wheels / built["wheel"]
        hashes = {"swarm_sdk/" + p.name: digest(p.read_bytes()) for p in package_files}
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            assert len(set(names)) == len(names)
            assert {n for n in names if n.startswith("swarm_sdk/")} == set(hashes)
            for name in names:
                assert not name.startswith("/") and ".." not in Path(name).parts
                assert name.startswith("swarm_sdk/") or ".dist-info/" in name
            for name, expected in hashes.items():
                assert digest(archive.read(name)) == expected
            license_name = next(n for n in names if n.endswith(".dist-info/licenses/LICENSE"))
            assert digest(archive.read(license_name)) == before["LICENSE"]
            record = next(n for n in names if n.endswith(".dist-info/RECORD"))
            for name, expected, size in csv.reader(io.StringIO(archive.read(record).decode())):
                if name == record:
                    continue
                raw = archive.read(name)
                assert expected == "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).decode().rstrip("=")
                assert int(size) == len(raw)
        report["wheel"] = {"filename": wheel.name, "sha256": digest(wheel.read_bytes()), "package_hashes": hashes, "record_verified": True}
        venv = owned / "venv"
        subprocess.run([sys.executable, "-I", "-B", "-m", "venv", "--without-pip", str(venv)], cwd=owned, env=env, check=True, timeout=60)
        python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        run("install", sys.executable, "install", owned, {"python": str(python), "wheel": str(wheel)})
        exports = next(ast.literal_eval(n.value) for n in ast.parse((package / "__init__.py").read_text()).body
                       if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "__all__" for t in n.targets))
        report["consumer"] = run("consumer", python, "consumer", owned, {"repo": str(sdk), "owned": str(owned),
                                      "tests": str(tests), "exports": exports, "package_hashes": hashes,
                                      "version": version, "original_test_count": original_count})
        report["source_after"] = {p.relative_to(sdk).as_posix(): digest(p.read_bytes()) for p in inputs}
        assert report["source_after"] == before
        assert digest(Path(__file__).read_bytes()) == report["harness_sha256"]
        assert not list(owned.rglob("*.pyc"))
        if dist_dir:
            dist_dir.mkdir(parents=True, mode=0o700)
        for output in (evidence, dist_dir):
            if output:
                for artifact in (sdist, wheel):
                    shutil.copyfile(artifact, output / artifact.name)
        report["passed"] = True
    except Exception as error:
        report["error"] = str(error)
    finally:
        shutil.rmtree(owned)
        report["owned_temp_cleaned"] = not owned.exists()
        if evidence:
            (evidence / "receipt.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in ("sdist", "build", "install", "consumer"):
        raise SystemExit(worker(sys.argv[1]))
    raise SystemExit(main())
