#!/usr/bin/env python3
"""Provision one isolated Engine namespace credential; stdout is the secret token."""
import json
import os
import sys
import urllib.parse
import urllib.request


def provision(endpoint, namespace, admin, application_tokens=True):
    def request(path, body=None):
        request = urllib.request.Request(
            endpoint.rstrip("/") + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"authorization": "Bearer " + admin, "content-type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    namespaces = request("/namespaces?name=" + urllib.parse.quote(namespace))["namespaces"]
    if namespaces:
        namespace_id = namespaces[0]["namespace_id"]
    else:
        namespace_id = request("/namespaces", {"name": namespace, "display_name": "DS conformance"})["namespace"]["namespace_id"]
    grants = [
        {"resource": "webhook_signing", "target": "any", "operations": ["create"]},
        {"resource": "namespace", "target": "any", "operations": ["read"]},
        {"resource": "datacenter", "target": "any", "operations": ["list"]},
        *[{"resource": resource, "target": "any", "operations": ["read", "list", "create", "update", "delete"]}
          for resource in ("actor", "runner", "runner_config", "actor_gateway", "actor_kv")],
        {"resource": "actor_stream", "target": "any", "operations": ["read", "list", "create", "append", "close", "delete"]},
        {"resource": "durable_stream", "target": "any", "operations": ["read", "list", "create", "update", "append", "close", "delete"]},
    ]
    if application_tokens:
        grants.append({"resource": "application_token", "target": "any", "operations": ["create"]})
    return request("/auth/tokens", {"namespace": namespace, "duration": 86400, "grants": grants})["token"]


if __name__ == "__main__":
    try:
        print(provision(os.environ["RIVET_ENDPOINT"], os.environ["RIVET_NAMESPACE"], os.environ["CONFORMANCE_ENGINE_ADMIN_TOKEN"]))
    except Exception as error:
        # HTTP exception text omits response bodies and credentials.
        print(f"Engine signing credential provisioning failed: {error}", file=sys.stderr)
        sys.exit(1)
