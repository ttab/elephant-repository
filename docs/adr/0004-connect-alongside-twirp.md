# Connect is served alongside Twirp, in the standard Connect encoding

Since v1.9.0 every RPC is mounted twice: Twirp at
`POST /twirp/elephant.repository.<Service>/<Method>` and Connect at
`POST /elephant.repository.<Service>/<Method>`, the latter also serving gRPC and
gRPC-Web in-cluster. Both mounts are registered from one
`elephantine.ServiceOptions` onto one `elephantine.APIServer`, so they share the
handlers, the authentication middleware, the scope and ACL checks and the RPC
metrics. The Twirp paths are removed in a future major release, not on a traffic
timer.

## Why

Connect is where the fleet is going — it gets us gRPC and gRPC-Web for free, and
generated clients that the rest of the platform already uses. But this
repository is the thing every other elephant service talks to, so a cutover
would have to be coordinated across every consumer at once. Serving both from
one registration makes the migration a per-consumer decision instead of a
platform-wide event, and makes divergence between the two mounts structurally
impossible rather than a thing to keep in step by review.

The alternative — running the two stacks as separate chains with separate
middleware — is what the previous shape did for Twirp alone, and it is what
makes "the Connect mount forgot a scope check" possible. One registration means
a new service gets both mounts or neither.

## Consequences

- **The Connect mount does not install a `UseProtoNames` codec to make its JSON
  look like Twirp's.** Connect marshals with protojson's defaults, so a field
  declared `ref_type` comes back as `refType` where Twirp spells it `ref_type`.
  Making it match would break every Connect runtime and generated client, which
  all assume the standard encoding. A caller that changes only the path prefix
  and reads the JSON by hand gets a `200` and `undefined` for every multi-word
  field. Requests are unaffected — protojson accepts either spelling.
- **Every handler codes its own errors at the return site**, a failed query or a
  marshalling failure included, because the two stacks default an *uncoded*
  error differently: Twirp to `internal`, Connect to `unknown`. Nothing
  downstream can fix that up, because only the handler knows which is right.
  The error vocabulary is `elephantine/rpc`; nothing outside the tests may
  import `twitchtv/twirp`.
- Three codes are answered with a different HTTP status on Connect —
  `failed_precondition` 400 rather than 412 is the one that matters, since
  document locks, system locks and workflow rule violations return it. Anything
  keyed on 412 has to read the RPC code instead.
- Error metadata moves from Twirp's `meta` map into an `elephantine.rpc.ErrorMeta`
  detail on Connect. `TestIntegrationErrorParity`, `TestIntegrationErrorBodies`
  and `TestIntegrationSuccessBodies` are what hold the two renderings
  equivalent; the API suite runs against one stack at a time, chosen by
  `TEST_RPC_STACK`, and CI runs it both ways.
- The plaintext listener speaks HTTP/2 as well as HTTP/1.1, which is what makes
  gRPC reachable at all. That reach ends at the cluster: the ingress speaks
  HTTP/1.1 to its targets, so gRPC is for service-to-service calls and is not
  offered externally.
