set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# Run Rust tests.
test:
    cargo test --locked

# Run the official Durable Streams server conformance suite.
test-conformance:
    bash scripts/test-conformance.sh
