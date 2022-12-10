package temporal.claims


default allow := false

allow {
    input.claims.subject = "test-user-sub"
    input.target.namespace = "test-namespace"
}
