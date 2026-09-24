"""Standalone worker jobs backed by Team Harness' existing worker adapter."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from datetime import timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

from team_harness.agents.process_identity import capture_starttime
from team_harness.agents.process_identity import kill_group
from team_harness.agents.process_identity import probe_group
from team_harness.agents.registry import resolve_template
from team_harness.agents.session_capture import capture_session_id_from_path
from team_harness.agents.spawner import spawn as spawn_worker
from team_harness.config import load_config
from team_harness.tracking.persistence import write_json_atomic

TERMINAL = {"completed", "failed", "cancelled", "lost"}


def _root() -> Path:
    return (
        Path(
            os.environ.get(
                "TEAM_HARNESS_JOBS_DIR", Path.home() / ".team-harness" / "jobs"
            )
        )
        .expanduser()
        .resolve()
    )


def _dir(job_id: str) -> Path:
    if not job_id.startswith("job_") or not job_id[4:].isalnum():
        raise ValueError(f"invalid job id: {job_id}")
    return _root() / job_id


def _paths(job_id: str) -> dict[str, Path]:
    directory = _dir(job_id)
    return {
        "dir": directory,
        "job": directory / "job.json",
        "result": directory / "result.json",
        "events": directory / "events.jsonl",
        "stdout": directory / "stdout.log",
        "stderr": directory / "stderr.log",
        "lock": directory / ".lock",
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _locked(job_id: str):
    class Lock:
        def __enter__(self):
            path = _paths(job_id)["lock"]
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.handle = path.open("a+")
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
            return self

        def __exit__(self, *_args: object) -> None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()

    return Lock()


def _read(job_id: str) -> dict:
    path = _paths(job_id)["job"]
    if not path.exists():
        raise ValueError(f"job not found: {job_id}")
    return json.loads(path.read_text())


def _event_locked(job: dict, kind: str, data: dict | None = None) -> None:
    event = {
        "id": uuid.uuid4().hex,
        "jobId": job["id"],
        "parentId": job.get("parentId"),
        "type": kind,
        "at": _now(),
        "data": data or {},
    }
    with _paths(job["id"])["events"].open("a") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


def _update(
    job_id: str, mutate: Callable[[dict], None], event: tuple[str, dict] | None = None
) -> dict:
    with _locked(job_id):
        job = _read(job_id)
        if job["status"] in TERMINAL:
            return job
        mutate(job)
        job["updatedAt"] = _now()
        write_json_atomic(path=_paths(job_id)["job"], payload=job)
        if event:
            _event_locked(job, event[0], event[1])
        return job


def _publish(
    job_id: str, status: str, *, exit_code: int | None = None, error: str | None = None
) -> dict:
    with _locked(job_id):
        job = _read(job_id)
        if job["status"] in TERMINAL:
            return job
        job.update(status=status, exitCode=exit_code, error=error, updatedAt=_now())
        result = {
            "jobId": job_id,
            "status": status,
            "exitCode": exit_code,
            "providerSessionId": job.get("providerSessionId"),
            "error": error,
            "completedAt": job["updatedAt"],
        }
        write_json_atomic(path=_paths(job_id)["result"], payload=result)
        write_json_atomic(path=_paths(job_id)["job"], payload=job)
        _event_locked(
            job,
            status,
            {
                "exitCode": exit_code,
                "error": error,
                "providerSessionId": job.get("providerSessionId"),
            },
        )
        return job


def spawn_job(
    *,
    harness: str,
    task: str,
    cwd: str | None = None,
    parent_id: str | None = None,
    model: str | None = None,
    continuation_of: str | None = None,
    provider_session_id: str | None = None,
) -> dict:
    workdir = Path(cwd or Path.cwd()).expanduser().resolve()
    if not workdir.is_dir():
        raise ValueError(f"working directory does not exist: {workdir}")
    if not task.strip():
        raise ValueError("task must not be empty")
    if shutil.which("ditto") is None:
        raise ValueError("Ditto wrapper not found on PATH: ditto")
    job_id = "job_" + uuid.uuid4().hex
    paths = _paths(job_id)
    paths["dir"].mkdir(parents=True, mode=0o700)
    paths["stdout"].touch(mode=0o600)
    paths["stderr"].touch(mode=0o600)
    created = _now()
    job = {
        "schemaVersion": 1,
        "id": job_id,
        "parentId": parent_id,
        "continuationOf": continuation_of,
        "harness": harness,
        "task": task,
        "cwd": str(workdir),
        "model": model,
        "status": "queued",
        "createdAt": created,
        "updatedAt": created,
        "supervisorPid": None,
        "supervisorStarttime": None,
        "pid": None,
        "pgid": None,
        "processStarttime": None,
        "providerSessionId": provider_session_id,
        "exitCode": None,
        "error": None,
    }
    write_json_atomic(path=paths["job"], payload=job)
    _event_locked(job, "queued", {"harness": harness, "cwd": str(workdir)})
    command = [sys.executable, "-m", "team_harness", "jobs", "__supervise", job_id]
    try:
        proc = subprocess.Popen(
            command,
            cwd=workdir,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as exc:
        _publish(job_id, "failed", error=f"could not start job supervisor: {exc}")
        return _read(job_id)
    supervisor_starttime = None
    for _attempt in range(10):
        supervisor_starttime = capture_starttime(proc.pid)
        if supervisor_starttime is not None:
            break
        time.sleep(0.01)
    job["supervisorPid"] = proc.pid
    job["supervisorStarttime"] = supervisor_starttime
    return _update(
        job_id,
        lambda record: record.update(
            supervisorPid=proc.pid, supervisorStarttime=job["supervisorStarttime"]
        ),
    )


async def _supervise(job_id: str) -> None:
    while not _read(job_id).get("supervisorPid"):
        await asyncio.sleep(0.02)
    job = _read(job_id)
    if job["status"] in TERMINAL:
        return
    if job.get("cancelRequested"):
        _publish(job_id, "cancelled")
        return
    paths = _paths(job_id)
    config = load_config(cwd=job["cwd"])
    template = resolve_template(job["harness"], config)
    ditto_harness = "agy" if job["harness"] == "antigravity" else job["harness"]
    command = template.command
    if Path(command[0]).name != "ditto":
        command = ("ditto", "run", ditto_harness, "--", *command)
    template = replace(
        template,
        command=command,
        # Ditto's selected profile owns the default model. Team Harness'
        # upstream Codex template pins its own default, so use only an
        # explicit per-job model for this standalone surface.
        default_model=job.get("model"),
    )
    config.agent_templates[job["harness"]] = template
    config.run_dir = paths["dir"]
    spawn = await spawn_worker(
        agent_id=job_id,
        agent_type=job["harness"],
        prompt=job["task"],
        cwd=Path(job["cwd"]),
        config=config,
        log_dir=paths["dir"],
        model=job.get("model"),
        stdout_path=paths["stdout"],
        stderr_path=paths["stderr"],
        mode="resume" if job.get("providerSessionId") else "fresh",
        resume_session_id=job.get("providerSessionId"),
    )

    def running(record: dict) -> None:
        record.update(
            status="running",
            pid=spawn.pid,
            pgid=spawn.pgid,
            processStarttime=spawn.starttime,
            command=spawn.command,
        )

    started = _update(
        job_id,
        running,
        (
            "started",
            {
                "pid": spawn.pid,
                "harness": job["harness"],
                "model": spawn.effective_model,
            },
        ),
    )
    if started.get("cancelRequested"):
        group_id = spawn.pgid if spawn.pgid is not None else spawn.pid
        if group_id is not None:
            kill_group(group_id, spawn.starttime, grace_s=1.0)
    stop_capture = asyncio.Event()
    capture = asyncio.create_task(
        capture_session_id_from_path(
            stdout_path=paths["stdout"],
            template=spawn.template,
            pre_generated_uuid=spawn.generated_uuid,
            stop_event=stop_capture,
        )
    )
    try:
        exit_code = await spawn.proc.wait()
        stop_capture.set()
        session_id = await capture
        if session_id:
            _update(
                job_id,
                lambda record: record.update(providerSessionId=session_id),
                ("session", {"providerSessionId": session_id}),
            )
        current = _read(job_id)
        status = (
            "cancelled"
            if current.get("cancelRequested")
            else "completed"
            if exit_code == 0
            else "failed"
        )
        _publish(job_id, status, exit_code=exit_code)
    except BaseException as exc:
        stop_capture.set()
        capture.cancel()
        await asyncio.gather(capture, return_exceptions=True)
        _publish(job_id, "failed", error=f"{type(exc).__name__}: {exc}")


def _events(job_id: str) -> list[dict]:
    path = _paths(job_id)["events"]
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def inspect_job(job_id: str) -> dict:
    job = reconcile(job_id)
    paths = _paths(job_id)
    result_path = paths["result"]
    return {
        "job": job,
        "result": json.loads(result_path.read_text()) if result_path.exists() else None,
        "events": _events(job_id),
        "artifacts": {key: str(value) for key, value in paths.items() if key != "lock"},
    }


def reconcile(job_id: str) -> dict:
    job = _read(job_id)
    if job["status"] not in {"queued", "running"}:
        return job
    try:
        supervisor = (
            probe_group(job["supervisorPid"], job.get("supervisorStarttime"))
            if job.get("supervisorPid")
            else None
        )
    except Exception:
        return job
    # A positive process-table result is enough to keep waiting; the token is
    # needed before signalling, but lack of a token must not turn a live worker
    # into a false lost result.
    if supervisor is not None and supervisor.alive:
        return job
    if job.get("pid") and job.get("pgid"):
        try:
            worker = probe_group(job["pgid"], job.get("processStarttime"))
            if worker.alive and worker.verdict == "ours":
                kill_group(job["pgid"], job.get("processStarttime"), grace_s=1.0)
        except Exception:
            pass
    status = "cancelled" if job.get("cancelRequested") else "lost"
    error = (
        "worker supervisor disappeared after cancellation was requested"
        if status == "cancelled"
        else "worker supervisor disappeared before publishing a result"
    )
    return _publish(job_id, status, error=error)


def list_jobs() -> list[dict]:
    root = _root()
    if not root.exists():
        return []
    return sorted(
        (reconcile(path.name) for path in root.glob("job_*/job.json")),
        key=lambda job: job["createdAt"],
        reverse=True,
    )


def cancel_job(job_id: str) -> dict:
    job = reconcile(job_id)
    if job["status"] in TERMINAL:
        return job
    with _locked(job_id):
        job = _read(job_id)
        if job["status"] not in TERMINAL:
            job["cancelRequested"] = True
            job["updatedAt"] = _now()
            write_json_atomic(path=_paths(job_id)["job"], payload=job)
            _event_locked(job, "cancel_requested")
    if job.get("pid") and job.get("pgid") and job.get("processStarttime"):
        kill_group(job["pgid"], job["processStarttime"], grace_s=1.0)
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        current = _read(job_id)
        if current["status"] in TERMINAL:
            return current
        if (
            current.get("pid")
            and current.get("pgid")
            and current.get("processStarttime")
        ):
            kill_group(current["pgid"], current["processStarttime"], grace_s=1.0)
        time.sleep(0.05)
    return reconcile(job_id)


def wait_job(job_id: str) -> dict:
    while True:
        job = reconcile(job_id)
        if job["status"] in TERMINAL:
            return inspect_job(job_id)
        time.sleep(0.15)


def resume_job(job_id: str, task: str) -> dict:
    job = reconcile(job_id)
    if job["status"] not in TERMINAL:
        raise ValueError(f"job is still {job['status']}")
    if not job.get("providerSessionId"):
        raise ValueError(f"job {job_id} has no captured provider session id")
    return spawn_job(
        harness=job["harness"],
        task=task,
        cwd=job["cwd"],
        parent_id=job.get("parentId") or job_id,
        continuation_of=job_id,
        model=job.get("model"),
        provider_session_id=job["providerSessionId"],
    )


async def supervise_entry(job_id: str) -> None:
    try:
        await _supervise(job_id)
    except BaseException as exc:
        _publish(job_id, "failed", error=f"{type(exc).__name__}: {exc}")
