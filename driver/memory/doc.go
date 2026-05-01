// Package memory provides an in-memory persistence [Driver].
//
// Data is organized into named silos. A silo is an isolated group of in-memory
// stores that persists for the lifetime of the process. Multiple drivers that
// reference the same silo name share the same underlying data.
package memory
