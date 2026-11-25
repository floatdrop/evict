package ref

// Backstop does what the garbage collector does with a Ref that has become
// unreachable.
func Backstop[T any](r *Ref[T]) { r.s.backstop() }
