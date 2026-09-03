#!/usr/bin/env bash
set -Eeuo pipefail

readonly RIVET_VERSION=2.3.12
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
	trap - EXIT INT TERM
	stop_group "$server_pid"
	stop_group "$engine_pid"
	if (( status == 0 )); then
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

echo "Using Rivet Engine $RIVET_VERSION"
cargo build --quiet --locked --bin rivet-durable-streams

RIVET__GUARD__HOST=127.0.0.1 \
RIVET__GUARD__PORT="$ENGINE_PORT" \
RIVET__API_PEER__HOST=127.0.0.1 \
RIVET__API_PEER__PORT="$((ENGINE_PORT + 1))" \
RIVET__METRICS__HOST=127.0.0.1 \
RIVET__METRICS__PORT="$((ENGINE_PORT + 10))" \
RIVET__FILE_SYSTEM__PATH="$RUN_DIR/engine" \
	setsid "$engine_binary" start >"$RUN_DIR/engine.log" 2>&1 &
engine_pid="$!"
wait_ready "Rivet Engine" "http://127.0.0.1:$ENGINE_PORT/health" "$engine_pid" "$RUN_DIR/engine.log"

RIVET_ENDPOINT="http://127.0.0.1:$ENGINE_PORT" \
RIVET_NAMESPACE=default RIVET_TOKEN=dev RIVET_POOL_NAME=durable-streams-conformance \
	setsid target/debug/rivet-durable-streams --host 0.0.0.0 --port "$SERVER_PORT" \
	>"$RUN_DIR/server.log" 2>&1 &
server_pid="$!"
wait_ready "Durable Streams" \
	"http://127.0.0.1:$SERVER_PORT/v1/stream/__conformance_readiness" \
	"$server_pid" "$RUN_DIR/server.log"

# The 0.3.6 CLI's runner filename misses Vitest 4's default include pattern.
# A temporary copy plus this config runs the published tests unchanged.
CONFORMANCE_RUN_DIR="$RUN_DIR" CONFORMANCE_URL="http://127.0.0.1:$SERVER_PORT" \
	npx --yes --package=@durable-streams/server-conformance-tests@0.3.6 --call '
		runner=$(readlink -f "$(command -v server-conformance-tests)")
		package=$(dirname "$(dirname "$runner")")
		cp -R "$package" "$CONFORMANCE_RUN_DIR/conformance"
		ln -s "$(dirname "$(dirname "$package")")" "$CONFORMANCE_RUN_DIR/conformance/node_modules"
		cd "$CONFORMANCE_RUN_DIR/conformance"
		printf %s\\n "export default { test: { include: [\"dist/test-runner.js\"], testTimeout: 60000 } }" > vitest.config.js
		./dist/cli.js --run "$CONFORMANCE_URL"
	' 2>&1 | tee "$RUN_DIR/conformance.log"
