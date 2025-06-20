// Package goro provides utilities for spawning and subsequently managing the
// liftime(s) of one or more goroutines. This package relies heavily on the
// context package to provide consistent cancellation semantics for long-lived
// goroutines. The goal of this package is to provide a unified way to cancel
// and wait on running goroutines as is often seen in "service" or "daemon"
// types with Start()/Stop() lifecycle functions and to unify the multiplicity
// of approaches that have been adopted over time.
//
//	Note: If you're looking for a short-lived (e.g.  request-scoped) group of
//	transient goroutines, you probably want `errgroup.Group` from
//	https://golang.org/x/sync/errgroup.
package goro
