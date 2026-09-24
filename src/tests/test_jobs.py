from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import time

from click.testing import CliRunner

from team_harness.cli import main


def _wait_status(
    runner: CliRunner, job_id: str, status: str, timeout: float = 5.0
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = runner.invoke(main, ["jobs", "status", job_id])
        assert result.exit_code == 0, result.output
        job = json.loads(result.output)
        if job["status"] == status:
            return job
        time.sleep(0.03)
    raise AssertionError(f"job {job_id} did not reach {status}")


def _setup(tmp_path: Path, monkeypatch, provider_script: str) -> tuple[CliRunner, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ditto = bin_dir / "ditto"
    ditto.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' "$@" >> "$DITTO_ARGS"\n'
        '[ "$1" = run ] || exit 71\nshift\nshift\n'
        '[ "$1" = -- ] || exit 72\nshift\nexec "$@"\n'
    )
    ditto.chmod(0o755)
    provider = bin_dir / "codex"
    provider.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' "$@" >> "$JOB_ARGS"\n' + provider_script + "\n"
    )
    provider.chmod(0o755)
    config_dir = tmp_path / ".team-harness"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        '[agents.codex]\ncommand = ["codex", "exec"]\n'
        'shared_flags = ["--json"]\n'
        '[agents.codex.session_capture]\nstrategy = "stream_json_event"\n'
        'match = { type = "thread.started" }\nfield_path = ["thread_id"]\n'
    )
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("JOB_ARGS", str(tmp_path / "args.log"))
    monkeypatch.setenv("DITTO_ARGS", str(tmp_path / "ditto-args.log"))
    monkeypatch.setenv("TEAM_HARNESS_JOBS_DIR", str(tmp_path / "jobs"))
    return CliRunner(), tmp_path


def test_spawn_wait_subscribe_and_native_resume_use_ditto(
    tmp_path: Path, monkeypatch
) -> None:
    runner, cwd = _setup(
        tmp_path,
        monkeypatch,
        'printf \'%s\\n\' \'{"type":"thread.started","thread_id":"codex-session-7"}\'',
    )
    spawned = runner.invoke(
        main,
        [
            "jobs",
            "spawn",
            "--harness",
            "codex",
            "--task",
            "first",
            "--cwd",
            str(cwd),
            "--parent-id",
            "orch-1",
        ],
    )
    assert spawned.exit_code == 0, spawned.output
    job = json.loads(spawned.output)
    waited = runner.invoke(main, ["jobs", "wait", job["id"]])
    assert waited.exit_code == 0, waited.output
    record = json.loads(waited.output)
    assert record["job"]["status"] == "completed"
    assert record["job"]["parentId"] == "orch-1"
    assert record["result"]["providerSessionId"] == "codex-session-7"
    events = runner.invoke(main, ["jobs", "subscribe", job["id"]])
    assert events.exit_code == 0
    assert json.loads(events.output.splitlines()[-1])["type"] == "completed"

    resumed = runner.invoke(main, ["jobs", "send", job["id"], "--message", "follow up"])
    assert resumed.exit_code == 0, resumed.output
    next_job = json.loads(resumed.output)
    _wait_status(runner, next_job["id"], "completed")
    args = (tmp_path / "args.log").read_text()
    assert "resume" in args.splitlines()
    assert "codex-session-7" in args.splitlines()
    assert "follow up" in args
    assert "--model" not in args
    assert "run\ncodex\n--\ncodex\nexec" in (tmp_path / "ditto-args.log").read_text()


def test_cancel_terminates_the_ditto_worker_process_group(
    tmp_path: Path, monkeypatch
) -> None:
    runner, cwd = _setup(tmp_path, monkeypatch, "sleep 30")
    spawned = runner.invoke(
        main,
        ["jobs", "spawn", "--harness", "codex", "--task", "slow", "--cwd", str(cwd)],
    )
    assert spawned.exit_code == 0, spawned.output
    job_id = json.loads(spawned.output)["id"]
    _wait_status(runner, job_id, "running")
    cancelled = runner.invoke(main, ["jobs", "cancel", job_id])
    assert cancelled.exit_code == 0, cancelled.output
    assert json.loads(cancelled.output)["status"] == "cancelled"


def test_reconcile_marks_a_crashed_supervisor_lost_and_reaps_worker(
    tmp_path: Path, monkeypatch
) -> None:
    runner, cwd = _setup(tmp_path, monkeypatch, "sleep 30")
    spawned = runner.invoke(
        main,
        ["jobs", "spawn", "--harness", "codex", "--task", "orphan", "--cwd", str(cwd)],
    )
    assert spawned.exit_code == 0, spawned.output
    job = json.loads(spawned.output)
    job = _wait_status(runner, job["id"], "running")
    os.killpg(job["supervisorPid"], signal.SIGKILL)

    recovered = runner.invoke(main, ["jobs", "inspect", job["id"]])
    assert recovered.exit_code == 0, recovered.output
    view = json.loads(recovered.output)
    assert view["job"]["status"] == "lost"
    assert view["result"]["status"] == "lost"
    assert view["events"][-1]["type"] == "lost"
