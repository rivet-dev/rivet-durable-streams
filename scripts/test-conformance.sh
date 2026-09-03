#!/usr/bin/env bash
set -Eeuo pipefail

readonly RIVET_REVISION=89c31e9438cdd0f2aa387dd6224b9145697a03f8
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

if [[ -n "${RIVET_ROOT:-}" ]]; then
	rivet_root="$RIVET_ROOT"
else
	rivet_root="$PWD/target/conformance/rivet-${RIVET_REVISION:0:12}"
	if ! git -C "$rivet_root" cat-file -e "$RIVET_REVISION^{commit}" 2>/dev/null; then
		mkdir -p "$rivet_root"
		if ! git -C "$rivet_root" rev-parse --git-dir >/dev/null 2>&1; then
			git -C "$rivet_root" init --quiet
		fi
		if ! git -C "$rivet_root" remote get-url origin >/dev/null 2>&1; then
			git -C "$rivet_root" remote add origin https://github.com/rivet-dev/rivet.git
		fi
		git -C "$rivet_root" fetch --quiet --depth 1 origin "$RIVET_REVISION"
	fi
	git -C "$rivet_root" switch --quiet --detach "$RIVET_REVISION"
fi
readonly RIVET_ROOT="$rivet_root"
[[ "$(git -C "$RIVET_ROOT" rev-parse HEAD)" == "$RIVET_REVISION" ]]

echo "Building Rivet Engine at $RIVET_REVISION"
(cd "$RIVET_ROOT" && LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}" \
	cargo build --quiet --locked --profile quick -p rivet-engine)
cargo build --quiet --locked --bin rivet-durable-streams

RIVET__GUARD__HOST=127.0.0.1 \
RIVET__GUARD__PORT="$ENGINE_PORT" \
RIVET__API_PEER__HOST=127.0.0.1 \
RIVET__API_PEER__PORT="$((ENGINE_PORT + 1))" \
RIVET__METRICS__HOST=127.0.0.1 \
RIVET__METRICS__PORT="$((ENGINE_PORT + 10))" \
RIVET__FILE_SYSTEM__PATH="$RUN_DIR/engine" \
	setsid "$RIVET_ROOT/target/quick/rivet-engine" start >"$RUN_DIR/engine.log" 2>&1 &
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
