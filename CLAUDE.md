# Durable Streams constraints

## Authentication: hard boundary

- **User -> DS HTTP -> DS storage actor MUST use the user's Rivet token.**
  Actor lookup/creation and data requests must preserve the caller's authority
  so existing Rivet authorization applies. Never substitute or fall back to the
  DS process token, including when caller credentials are missing or rejected.
- **DS -> subscriber actor wake uses the DS process token.** Delivery is
  asynchronous service work authorized by trusted deployed subscription
  metadata; it must not depend on retaining a producer's credentials.
- Before injecting the process token, verify that the source stream and resolved
  destination match a subscription declaration from the configured namespace's
  `/actors/names` metadata. Build the exact actor wake URL on the configured
  gateway origin. A matching gateway hostname or actor name alone is insufficient.
  Never forward the process token to arbitrary webhooks or through redirects.
- The process token may also authenticate internal service operations such as
  metadata discovery and subscription reconciliation. These are distinct from
  executing a user's stream request.

This separation prevents the public DS API from borrowing service privileges to
bypass caller permissions, while allowing durable delivery after a producer's
request and credentials are gone.

The HTTP facade uses a caller-scoped data connection for lookup, creation, and
requests. Wake injection is authorized from existing adopted routing records
and verified cached destinations. Preserve these checks in future changes.
