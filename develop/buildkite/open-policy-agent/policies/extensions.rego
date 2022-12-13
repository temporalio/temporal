package temporal.extensions


default allow := false

allow {
    input.Claims.Extensions.ForceAllow = true
    input.Target.Namespace = "test-namespace"
}
