#!/usr/bin/env bash
set -Eeuo pipefail

readonly RIVET_VERSION="${CONFORMANCE_ENGINE_VERSION:-}"
readonly ENGINE_PORT=17420
readonly SERVER_PORT=17878
readonly RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rivet-durable-streams-conformance.XXXXXX")"
engine_pid=""
server_pid=""

stop_group() {
	local pid="$1"
	[[ -n "$pid" ]] || return
	kill -TERM -- "-$pid" 2>/dev/null || true
	for _ in {1..20}; do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.1
	done
	kill -KILL -- "-$pid" 2>/dev/null || true
	wait "$pid" 2>/dev/null || true
}

cleanup() {
	local status="$?"
	trap - EXIT
	trap '' INT TERM
	stop_group "$server_pid"
	stop_group "$engine_pid"
	if (( status == 0 )) && [[ "${CONFORMANCE_KEEP_LOGS:-0}" != 1 ]]; then
		rm -rf -- "$RUN_DIR"
	else
		echo "Logs retained in $RUN_DIR" >&2
	fi
	exit "$status"
}
trap cleanup EXIT INT TERM

for port in "$ENGINE_PORT" "$((ENGINE_PORT + 1))" "$((ENGINE_PORT + 10))" "$SERVER_PORT"; do
	if ss -H -ltn "sport = :$port" | grep -q .; then
		echo "error: port $port is already in use" >&2
		exit 1
	fi
done

wait_ready() {
	local name="$1" url="$2" pid="$3" log="$4"
	for _ in {1..300}; do
		curl -sS -o /dev/null "$url" 2>/dev/null && return
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.1
	done
	echo "error: $name did not become ready" >&2
	tail -n 100 "$log" >&2 || true
	return 1
}

case "$(uname -s)-$(uname -m)" in
	Linux-x86_64) engine_artifact=rivet-engine-x86_64-unknown-linux-musl ;;
	Linux-aarch64 | Linux-arm64) engine_artifact=rivet-engine-aarch64-unknown-linux-musl ;;
	*) echo "error: unsupported platform $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

if [[ -n "${CONFORMANCE_ENGINE_BINARY:-}" ]]; then
	engine_binary="$(realpath "$CONFORMANCE_ENGINE_BINARY")"
	expected_checksum="${CONFORMANCE_ENGINE_SHA256:?Set CONFORMANCE_ENGINE_SHA256 for the supplied Engine binary}"
	[[ -x "$engine_binary" && "$(sha256sum "$engine_binary" | awk '{ print $1 }')" == "$expected_checksum" ]] \
		|| { echo "error: supplied Engine binary checksum mismatch" >&2; exit 1; }
elif [[ -n "${CONFORMANCE_ENGINE_URL:-}" ]]; then
	expected_checksum="${CONFORMANCE_ENGINE_SHA256:?Set CONFORMANCE_ENGINE_SHA256 for the supplied Engine URL}"
	engine_binary="$RUN_DIR/$engine_artifact"
	curl --fail --silent --show-error "$CONFORMANCE_ENGINE_URL" -o "$engine_binary"
	[[ "$(sha256sum "$engine_binary" | awk '{ print $1 }')" == "$expected_checksum" ]] \
		|| { echo "error: supplied Engine download checksum mismatch" >&2; exit 1; }
	chmod 755 "$engine_binary"
else
	[[ -n "$RIVET_VERSION" ]] || {
		echo "error: Engine signing APIs require a compatible build; set CONFORMANCE_ENGINE_BINARY or CONFORMANCE_ENGINE_URL with CONFORMANCE_ENGINE_SHA256, or pin CONFORMANCE_ENGINE_VERSION" >&2
		exit 1
	}
	engine_cache="$PWD/target/conformance/engine-$RIVET_VERSION"
	engine_binary="$engine_cache/$engine_artifact"
	release_base="https://releases.rivet.dev/rivet/$RIVET_VERSION/engine"
	manifest="$(curl --fail --silent --show-error "$release_base/SHA256SUMS")"
	expected_checksum="$(awk -v artifact="$engine_artifact" '$2 == artifact { print $1 }' <<<"$manifest")"
	[[ -n "$expected_checksum" ]] || { echo "error: missing Engine checksum" >&2; exit 1; }
	if [[ ! -x "$engine_binary" ]] \
		|| [[ "$(sha256sum "$engine_binary" | awk '{ print $1 }')" != "$expected_checksum" ]]; then
		downloaded_engine="$RUN_DIR/$engine_artifact"
		curl --fail --silent --show-error "$release_base/$engine_artifact" -o "$downloaded_engine"
		received_checksum="$(sha256sum "$downloaded_engine" | awk '{ print $1 }')"
		[[ "$received_checksum" == "$expected_checksum" ]] \
			|| { echo "error: Engine checksum mismatch" >&2; exit 1; }
		mkdir -p "$engine_cache"
		install -m 755 "$downloaded_engine" "$engine_binary"
	fi
fi

echo "Using checksum-verified Engine: $expected_checksum"
"$engine_binary" --version
cargo build --quiet --locked --bin rivet-durable-streams --features subscription-fault-injection
npm --prefix tests/conformance ci --no-audit --no-fund

export RIVET_ENDPOINT="http://127.0.0.1:$ENGINE_PORT" RIVET_NAMESPACE=default
export CONFORMANCE_ENGINE_ADMIN_TOKEN="${CONFORMANCE_ENGINE_ADMIN_TOKEN:-dev}"
RIVET__AUTH__ADMIN_TOKEN="$CONFORMANCE_ENGINE_ADMIN_TOKEN" \
RIVET__TOPOLOGY__DATACENTER_LABEL=1 \
RIVET__TOPOLOGY__DATACENTERS__DEFAULT__DATACENTER_LABEL=1 \
RIVET__TOPOLOGY__DATACENTERS__DEFAULT__IS_LEADER=true \
RIVET__TOPOLOGY__DATACENTERS__DEFAULT__PUBLIC_URL="$RIVET_ENDPOINT" \
RIVET__TOPOLOGY__DATACENTERS__DEFAULT__PEER_URL="http://127.0.0.1:$((ENGINE_PORT + 1))" \
RIVET__GUARD__HOST=127.0.0.1 \
RIVET__GUARD__PORT="$ENGINE_PORT" \
RIVET__API_PEER__HOST=127.0.0.1 \
RIVET__API_PEER__PORT="$((ENGINE_PORT + 1))" \
RIVET__METRICS__HOST=127.0.0.1 \
RIVET__METRICS__PORT="$((ENGINE_PORT + 10))" \
RIVET__FEATURES__GUARD_GATEWAY_V3__MODE=on \
RIVET__FEATURES__GUARD_GATEWAY_V3__PERCENTAGE=100 \
RIVET__PEGBOARD__BASE_RETRY_TIMEOUT=250 \
RIVET__PEGBOARD__RESCHEDULE_BACKOFF_MAX_EXPONENT=1 \
RIVET__FILE_SYSTEM__PATH="$RUN_DIR/engine" \
	setsid "$engine_binary" start >"$RUN_DIR/engine.log" 2>&1 &
engine_pid="$!"
wait_ready "Rivet Engine" "http://127.0.0.1:$ENGINE_PORT/health" "$engine_pid" "$RUN_DIR/engine.log"
RIVET_TOKEN="$(python3 tests/conformance/provision_signing.py)"
export RIVET_TOKEN

RIVET_POOL_NAME=durable-streams-conformance \
PUBLIC_ORIGIN="http://127.0.0.1:$SERVER_PORT" RUST_LOG="${RUST_LOG:-warn}" \
DS_SUBSCRIPTIONS_ALLOW_LOCAL_HTTP=1 DS_SUBSCRIPTIONS_ALLOW_UNAUTHENTICATED=1 \
	setsid target/debug/rivet-durable-streams --host 0.0.0.0 --port "$SERVER_PORT" \
	>"$RUN_DIR/server.log" 2>&1 &
server_pid="$!"
wait_ready "Durable Streams" \
	"http://127.0.0.1:$SERVER_PORT/v1/stream/__conformance_readiness" \
	"$server_pid" "$RUN_DIR/server.log"

# The official CLI omits subscriptions; our isolated harness enables all groups.
if [[ "${CONFORMANCE_RECOVERY_ONLY:-0}" != "1" ]]; then
    CONFORMANCE_TEST_URL="http://127.0.0.1:$SERVER_PORT" \
        npm --prefix tests/conformance test -- "$@" 2>&1 | tee "$RUN_DIR/conformance.log"
    RIVET_TEST_ENDPOINT="http://127.0.0.1:$ENGINE_PORT" \
        cargo test --locked --features subscription-fault-injection --test e2e -- --ignored --nocapture 2>&1 | tee "$RUN_DIR/e2e.log"
    RIVET_TEST_ENDPOINT="http://127.0.0.1:$ENGINE_PORT" \
        cargo test --locked --features subscription-fault-injection --lib maximum_snapshot_persists_through_real_engine -- --ignored --nocapture 2>&1 | tee "$RUN_DIR/large-snapshot.log"
fi
if [[ "${CONFORMANCE_SKIP_RECOVERY:-0}" != "1" ]]; then
    python3 tests/subscription_recovery.py --engine "http://127.0.0.1:$ENGINE_PORT" \
        --logs "$RUN_DIR/recovery" 2>&1 | tee "$RUN_DIR/recovery.log"
fi
