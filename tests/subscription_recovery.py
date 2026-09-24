#!/usr/bin/env python3
"""Real-Engine restart/fault tests. Requires the fault-injection binary, never a test HTTP endpoint."""
import argparse
import base64
import concurrent.futures
import http.server
import json
import os
import pathlib
import queue
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from conformance.provision_signing import provision

DEFAULT_TOKEN = object()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Receiver:
    def __init__(self):
        self.requests = queue.Queue()
        self.status = 200
        self.done = False
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["content-length"])))
                owner.requests.put((time.monotonic(), body))
                self.send_response(owner.status)
                self.send_header("content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"done": owner.done}).encode())

            def log_message(self, *_args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/hook"

    def next(self, timeout=25):
        return self.requests.get(timeout=timeout)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class Server:
    def __init__(self, binary, engine, logs):
        self.binary = str(pathlib.Path(binary).resolve())
        self.engine = engine
        self.namespace = os.environ.get("RIVET_NAMESPACE", "default")
        self.engine_token = os.environ["RIVET_TOKEN"]
        self.logs = pathlib.Path(logs)
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}/v1/stream"
        self.tenant = "recovery-" + uuid.uuid4().hex
        self.pool = "subscription-recovery-" + uuid.uuid4().hex
        self.process = None
        self.starts = 0
        self.marker = self.logs / "checkpoint"
        self.release_marker = self.logs / "checkpoint.release"

    def start(self, fault=None, pause=None):
        self.starts += 1
        self.marker.unlink(missing_ok=True)
        self.release_marker.unlink(missing_ok=True)
        env = dict(os.environ, RIVET_ENDPOINT=self.engine, RIVET_NAMESPACE=self.namespace, RIVET_TOKEN=self.engine_token,
                   RIVET_POOL_NAME=self.pool, DS_TENANT_SCOPE=self.tenant,
                   DS_SUBSCRIPTIONS_MANAGEMENT_TOKEN="recovery-test-management",
                   DS_SUBSCRIPTIONS_ALLOW_UNAUTHENTICATED="0", DS_SUBSCRIPTIONS_ALLOW_LOCAL_HTTP="1",
                   PUBLIC_ORIGIN=f"http://127.0.0.1:{self.port}", RUST_LOG="warn")
        env.pop("DS_SUBSCRIPTION_FAULT", None)
        env.pop("DS_SUBSCRIPTION_PAUSE_POINT", None)
        env.pop("DS_SUBSCRIPTION_PAUSE_MARKER", None)
        if fault:
            env.update(DS_SUBSCRIPTION_FAULT=fault, DS_SUBSCRIPTION_FAULT_MARKER=str(self.marker))
        if pause:
            env.update(DS_SUBSCRIPTION_PAUSE_POINT=pause, DS_SUBSCRIPTION_PAUSE_MARKER=str(self.marker))
        with open(self.logs / f"server-{self.starts}.log", "wb") as log:
            self.process = subprocess.Popen([self.binary, "--host", "0.0.0.0", "--port", str(self.port)],
                                            env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.poll(lambda: self.request("GET", "ready", expected=(404,))[0] == 404, timeout=45)
        self.poll(lambda: self.request("GET", "__ds/jwks.json", token=None)[0] == 200, timeout=60)

    def stop(self):
        if self.process and self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=10)

    def restart(self, fault=None, pause=None):
        self.stop()
        self.start(fault, pause)

    def request(self, method, path, body=None, token=DEFAULT_TOKEN, expected=(200, 201, 204)):
        data = None if body is None else json.dumps(body).encode()
        headers = {"content-type": "application/json"}
        if token is DEFAULT_TOKEN:
            token = "recovery-test-management" if path.startswith("__ds/") else self.engine_token
        if token:
            headers["authorization"] = "Bearer " + token
        url = path if path.startswith("http") else self.base + "/" + path
        request = urllib.request.Request(url, method=method, data=data, headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=35)
        except urllib.error.HTTPError as error:
            response = error
        raw = response.read()
        try:
            value = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            value = raw.decode(errors="replace")
        error_code = value.get("error", {}).get("code") if isinstance(value, dict) and isinstance(value.get("error"), dict) else None
        assert response.status in expected, (method, path, response.status, error_code)
        return response.status, value

    @staticmethod
    def poll(condition, timeout=25):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            try:
                result = condition()
                if result:
                    return result
            except (OSError, AssertionError) as error:
                last = error
            time.sleep(0.1)
        raise AssertionError(f"Polling timed out: {last}")

    def sub(self, name, stream, receiver=None, lease=30000):
        config = {"type": "webhook" if receiver else "pull-wake", "streams": [stream], "lease_ttl_ms": lease}
        if receiver:
            config["webhook"] = {"url": receiver.url}
        else:
            config["wake_stream"] = "wake/" + name
            self.request("PUT", config["wake_stream"], [])
        self.request("PUT", "__ds/subscriptions/" + name, config)

    def crash_request(self, checkpoint, method, path, body):
        self.poll(lambda: self.request("HEAD", path)[0] == 200, timeout=60)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(self.request, method, path, body)
            self.poll(lambda: self.marker.exists(), timeout=30)
            assert self.marker.read_text() == checkpoint
            try:
                future.result(timeout=40)
            except (OSError, AssertionError):
                pass
        # The faulted actor can exit separately from the HTTP facade. Once the
        # checkpoint is confirmed, stop any surviving facade for a cold restart.
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.start()


def run(binary, engine, logs):
    s = Server(binary, engine, logs)
    receiver = Receiver()
    try:
        s.start()
        # Public management is denied without the explicitly configured credential.
        s.request("PUT", "__ds/subscriptions/unauthorized", {}, token=None, expected=(401,))
        s.sub("append-crash", "events/append-crash", receiver)
        # Explicit operations and configuration conflicts must not depend on
        # unrelated runner pools (the preceding Rust e2e pool has stopped).
        s.request("POST", "__ds/subscriptions/append-crash/streams", {"streams": ["events/append-crash"]})
        s.request("PUT", "__ds/subscriptions/append-crash", {
            "type": "webhook", "pattern": "events/**", "webhook": {"url": receiver.url},
        }, expected=(409,))
        s.request("PUT", "events/append-crash", [])
        keys = s.request("GET", "__ds/jwks.json", token=None)[1]
        s.restart("after_stream_commit")
        s.crash_request("after_stream_commit", "POST", "events/append-crash", {"after": "commit"})
        _, wake = receiver.next()
        assert wake["subscription_id"] == "append-crash"
        assert keys == s.request("GET", "__ds/jwks.json", token=None)[1]
        ack = {"wake_id": wake["wake_id"], "generation": wake["generation"], "done": True,
               "acks": [{"stream": "events/append-crash", "offset": wake["streams"][0]["tail_offset"]}]}
        s.request("POST", wake["callback_url"], ack, token=wake["callback_token"])
        print("PASS restart after stream commit before notification; signing key persisted", flush=True)
        s.request("DELETE", "__ds/subscriptions/append-crash")

        s.sub("wake-crash", "events/wake-crash", receiver)
        s.request("PUT", "events/wake-crash", [])
        s.restart("after_wake_persist")
        s.crash_request("after_wake_persist", "POST", "events/wake-crash", {"n": 1})
        # The persisted 60-second dispatch reservation fences an attempt whose
        # process disappeared. Recovery must wait for it without a control call.
        _, wake = receiver.next(timeout=75)
        assert wake["subscription_id"] == "wake-crash"
        s.request("POST", wake["callback_url"], {"wake_id": wake["wake_id"], "generation": wake["generation"], "done": True,
                  "acks": [{"stream": "events/wake-crash", "offset": wake["streams"][0]["tail_offset"]}]}, token=wake["callback_token"])
        print("PASS restart with persisted webhook wake before delivery", flush=True)
        s.request("DELETE", "__ds/subscriptions/wake-crash")

        s.sub("pull-crash", "events/pull-crash")
        s.request("PUT", "events/pull-crash", [])
        s.restart("after_wake_append_commit")
        s.crash_request("after_wake_append_commit", "POST", "events/pull-crash", {"n": 1})
        events = s.poll(lambda: s.request("GET", "wake/pull-crash")[1])
        assert len(events) == 1, events
        claimed = s.request("POST", "__ds/subscriptions/pull-crash/claim", {"worker": "first"})[1]
        s.restart()
        busy = s.request("POST", "__ds/subscriptions/pull-crash/claim", {"worker": "second"}, expected=(409,))[1]
        assert busy["error"]["code"] == "ALREADY_CLAIMED"
        s.request("POST", "__ds/subscriptions/pull-crash/ack", {"wake_id": claimed["wake_id"], "generation": claimed["generation"], "done": True,
                  "acks": [{"stream": "events/pull-crash", "offset": claimed["streams"][0]["tail_offset"]}]}, token=claimed["token"])
        assert len(s.request("GET", "wake/pull-crash")[1]) == 1
        print("PASS restart after pull wake append before completion; deduplication and active claim survive", flush=True)
        s.request("DELETE", "__ds/subscriptions/pull-crash")

        s.sub("expired-claim", "events/expired-claim", lease=3000)
        s.request("PUT", "events/expired-claim", {"n": 1})
        claimed = s.poll(lambda: s.request("POST", "__ds/subscriptions/expired-claim/claim", {"worker": "old"})[1])
        acknowledged = claimed["streams"][0]["tail_offset"]
        s.request("POST", "__ds/subscriptions/expired-claim/ack", {
            "wake_id": claimed["wake_id"], "generation": claimed["generation"],
            "acks": [{"stream": "events/expired-claim", "offset": acknowledged}],
        }, token=claimed["token"])
        s.request("POST", "events/expired-claim", {"n": 2})
        # Stop immediately so graceful draining cannot process lease expiry;
        # the persisted deadline must be enforced by the restarted owner.
        os.killpg(s.process.pid, signal.SIGKILL)
        s.process.wait(timeout=10)
        time.sleep(3.1)
        s.start()
        stale = s.request("POST", "__ds/subscriptions/expired-claim/ack", {
            "wake_id": claimed["wake_id"], "generation": claimed["generation"], "done": True,
        }, token=claimed["token"], expected=(409,))[1]
        assert stale["error"]["code"] == "FENCED"
        current = s.poll(lambda: s.request("POST", "__ds/subscriptions/expired-claim/claim", {"worker": "new"})[1])
        assert current["generation"] > claimed["generation"] and current["wake_id"] != claimed["wake_id"]
        assert current["streams"][0]["acked_offset"] == acknowledged
        assert current["streams"][0]["has_pending"]
        s.request("POST", "__ds/subscriptions/expired-claim/ack", {
            "wake_id": current["wake_id"], "generation": current["generation"], "done": True,
            "acks": [{"stream": "events/expired-claim", "offset": current["streams"][0]["tail_offset"]}],
        }, token=current["token"])
        print("PASS persisted claim expires while offline; replacement fences old token and retains cursor", flush=True)
        s.request("DELETE", "__ds/subscriptions/expired-claim")

        receiver.status = 503
        s.sub("retry", "events/retry", receiver, lease=600000)
        s.request("PUT", "events/retry", {"n": 1})
        first_time, first = receiver.next()
        second_time, second = receiver.next()
        third_time, third = receiver.next()
        assert first["wake_id"] == second["wake_id"] == third["wake_id"]
        assert second_time - first_time >= .7 and third_time - second_time >= 1.4
        s.poll(lambda: s.request("GET", "__ds/subscriptions/retry")[1]["status"] == "failed")
        s.restart()
        receiver.status, receiver.done = 200, True
        fourth_time, fourth = receiver.next()
        assert fourth["wake_id"] == third["wake_id"]
        assert fourth_time - third_time >= 3.0, "Restart reset retry backoff"
        s.poll(lambda: s.request("GET", "__ds/subscriptions/retry")[1]["streams"][0]["acked_offset"] == fourth["streams"][0]["tail_offset"])
        print("PASS failed delivery retry count/deadline and wake identity survive restart", flush=True)
        s.request("DELETE", "__ds/subscriptions/retry")

        # Missing signing grants are permanent failures. Restoring credentials
        # alone must not silently retry; the operator explicitly re-confirms.
        normal_token = s.engine_token
        s.stop()
        s.engine_token = provision(engine, s.namespace, os.environ["CONFORMANCE_ENGINE_ADMIN_TOKEN"], application_tokens=False)
        s.start()
        receiver.done = False
        s.sub("signing-denied", "events/signing-denied", receiver)
        s.request("PUT", "events/signing-denied", {"n": 1})

        def signing_failed():
            subscription = s.request("GET", "__ds/subscriptions/signing-denied")[1]
            error = subscription.get("delivery_error") or {}
            return subscription if subscription["status"] == "failed" and error.get("status") == 403 else None

        failed = s.poll(signing_failed)
        assert failed["delivery_error"]["retry_at"] is None
        assert receiver.requests.empty(), "Denied signer delivered an unsigned webhook"
        s.engine_token = normal_token
        s.restart()
        time.sleep(1.2)
        assert signing_failed(), "Restart cleared the persisted signing failure"
        assert receiver.requests.empty(), "Corrected credential retried without operator re-confirmation"
        s.sub("signing-denied", "events/signing-denied", receiver)
        _, delivered = receiver.next()
        s.request("POST", delivered["callback_url"], {
            "wake_id": delivered["wake_id"], "generation": delivered["generation"], "done": True,
            "acks": [{"stream": "events/signing-denied", "offset": delivered["streams"][0]["tail_offset"]}],
        }, token=delivered["callback_token"])
        assert s.request("GET", "__ds/subscriptions/signing-denied")[1]["delivery_error"] is None
        print("PASS permanent Engine403 persists across restart; corrected credentials resume on same-config PUT", flush=True)
        s.request("DELETE", "__ds/subscriptions/signing-denied")

        s.restart(pause="after_application_issuance")
        s.sub("claim-race", "events/claim-race", lease=60000)
        s.request("PUT", "events/claim-race", {"original": True})
        original_event = s.poll(lambda: s.request("GET", "wake/claim-race")[1])[0]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            claiming = executor.submit(s.request, "POST", "__ds/subscriptions/claim-race/claim",
                                       {"worker": "before-recreation"}, expected=(409,))
            s.poll(lambda: s.marker.exists(), timeout=8)
            assert s.marker.read_text() == "after_application_issuance"
            s.request("DELETE", "events/claim-race")
            s.request("PUT", "events/claim-race", {"recreated": True})
            # Ordinary wake-stream reads establish that catalog fanout observed
            # the new incarnation without a subscription control operation.
            replacement_event = s.poll(lambda: next((event for event in s.request("GET", "wake/claim-race")[1]
                if event["generation"] > original_event["generation"]), None), timeout=5)
            s.release_marker.write_text("release")
            assert claiming.result(timeout=15)[1]["error"]["code"] == "FENCED"
        claimed = s.request("POST", "__ds/subscriptions/claim-race/claim", {"worker": "after-recreation"})[1]
        assert claimed["generation"] == replacement_event["generation"]
        s.request("POST", "__ds/subscriptions/claim-race/ack", {
            "wake_id": claimed["wake_id"], "generation": claimed["generation"], "done": True,
            "acks": [{"stream": "events/claim-race", "offset": claimed["streams"][0]["tail_offset"]}],
        }, token=claimed["token"])
        s.request("DELETE", "__ds/subscriptions/claim-race")
        print("PASS source recreation during Engine issuance fences old claim and publishes replacement", flush=True)

        s.restart(pause="after_webhook_signing")
        s.sub("webhook-race", "events/webhook-race", receiver, lease=60000)
        s.request("PUT", "events/webhook-race", {"original": True})
        s.poll(lambda: s.marker.exists(), timeout=8)
        assert s.marker.read_text() == "after_webhook_signing"
        assert receiver.requests.empty(), "Webhook was delivered before its signing barrier"
        s.request("DELETE", "events/webhook-race")
        s.request("PUT", "events/webhook-race", {"recreated": True})
        assert s.request("GET", "events/webhook-race")[1] == [{"recreated": True}]
        # Source mutations await the durable catalog notification. The pump
        # remains paused; no subscription control operation repairs its state.
        s.release_marker.write_text("release")
        # No subscription control call or additional write may rescue this wake.
        _, replacement = receiver.next()
        assert replacement["generation"] > 1, "Old signed snapshot reached the receiver"
        s.request("POST", replacement["callback_url"], {
            "wake_id": replacement["wake_id"], "generation": replacement["generation"], "done": True,
            "acks": [{"stream": "events/webhook-race", "offset": replacement["streams"][0]["tail_offset"]}],
        }, token=replacement["callback_token"])
        s.request("DELETE", "__ds/subscriptions/webhook-race")
        print("PASS source recreation during Engine webhook signing suppresses old delivery and wakes replacement", flush=True)

        # Same IDs and paths in another tenant resolve to different actors.
        prior_keys = s.request("GET", "__ds/jwks.json", token=None)[1]
        s.stop()
        s.tenant += "-other"
        s.start()
        s.request("GET", "__ds/subscriptions/retry", expected=(404,))
        s.request("GET", "events/retry", expected=(404,))
        # Webhook keys belong to the Engine namespace; DS tenants still fence
        # capabilities and isolate actors while sharing its public signer.
        assert s.request("GET", "__ds/jwks.json", token=None)[1] == prior_keys
        print("PASS tenant isolation and default management authorization", flush=True)
    finally:
        s.stop()
        receiver.close()


def run_migration(legacy_binary, binary, engine, logs):
    logs = pathlib.Path(logs) / "migration"
    logs.mkdir(exist_ok=True)
    s = Server(legacy_binary, engine, logs)
    # Keep the legacy catalog scan bounded independently of the thousands of
    # stream actors left by the official suite in its own namespace.
    s.namespace = "ds-migration-" + uuid.uuid4().hex
    s.engine_token = provision(engine, s.namespace, os.environ["CONFORMANCE_ENGINE_ADMIN_TOKEN"])
    receiver = Receiver()

    def token_header(token):
        part = token.split(".")[0]
        return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))

    try:
        s.start()
        # Register patterns before creating sources: the preserved old SDK
        # cannot authenticate keyed actor lookups against a secured Engine.
        s.request("PUT", "__ds/subscriptions/legacy-hook", {
            "type": "webhook", "pattern": "events/legacy-hook", "lease_ttl_ms": 600000,
            "webhook": {"url": receiver.url},
        })
        s.request("PUT", "wake/legacy-claim", [])
        s.request("PUT", "__ds/subscriptions/legacy-claim", {
            "type": "pull-wake", "pattern": "events/legacy-claim", "lease_ttl_ms": 600000,
            "wake_stream": "wake/legacy-claim",
        })
        old_keys = s.request("GET", "__ds/jwks.json", token=None)[1]["keys"]
        s.request("PUT", "events/legacy-hook", {"legacy": True})
        _, callback = receiver.next()
        s.request("PUT", "events/legacy-claim", {"legacy": True})
        claimed = s.poll(lambda: s.request("POST", "__ds/subscriptions/legacy-claim/claim", {"worker": "legacy"})[1])
        assert token_header(callback["callback_token"])["alg"] == "HS256"
        assert token_header(claimed["token"])["alg"] == "HS256"
        s.stop()
        s.binary = str(pathlib.Path(binary).resolve())
        s.start()
        new_keys = s.request("GET", "__ds/jwks.json", token=None)[1]["keys"]
        assert all(key in new_keys for key in old_keys), "Upgrade discarded in-flight webhook verification keys"
        assert len(new_keys) > len(old_keys), "Engine webhook keys were not published after cutover"
        callback_body = {
            "wake_id": callback["wake_id"], "generation": callback["generation"], "done": True,
            "acks": [{"stream": "events/legacy-hook", "offset": callback["streams"][0]["tail_offset"]}],
        }
        s.request("POST", callback["callback_url"], callback_body, token=callback["callback_token"])
        assert s.request("POST", callback["callback_url"], callback_body, token=callback["callback_token"], expected=(409,))[1]["error"]["code"] == "FENCED"
        s.request("POST", "__ds/subscriptions/legacy-claim/ack", {
            "wake_id": claimed["wake_id"], "generation": claimed["generation"], "done": True,
            "acks": [{"stream": "events/legacy-claim", "offset": claimed["streams"][0]["tail_offset"]}],
        }, token=claimed["token"])
        s.request("POST", "events/legacy-claim", {"engine": True})
        fresh = s.poll(lambda: s.request("POST", "__ds/subscriptions/legacy-claim/claim", {"worker": "engine"})[1])
        assert token_header(fresh["token"])["typ"] == "rivet-app+jwt"
        assert fresh["generation"] > claimed["generation"]
        s.request("POST", "events/legacy-hook", {"engine": True})
        _, fresh_callback = receiver.next()
        assert token_header(fresh_callback["callback_token"])["typ"] == "rivet-app+jwt"
        print("PASS live legacy callback and claim survive cutover; all new capabilities use Engine JWTs", flush=True)
    finally:
        s.stop()
        receiver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/rivet-durable-streams")
    parser.add_argument("--engine", default="http://127.0.0.1:17420")
    parser.add_argument("--logs", default=None)
    parser.add_argument("--legacy-binary", default=os.environ.get("CONFORMANCE_LEGACY_BINARY"))
    args = parser.parse_args()
    if args.logs:
        pathlib.Path(args.logs).mkdir(parents=True, exist_ok=True)
        run(args.binary, args.engine, args.logs)
        if args.legacy_binary:
            run_migration(args.legacy_binary, args.binary, args.engine, args.logs)
    else:
        with tempfile.TemporaryDirectory(prefix="ds-subscription-recovery-") as logs:
            run(args.binary, args.engine, logs)
            if args.legacy_binary:
                run_migration(args.legacy_binary, args.binary, args.engine, logs)
