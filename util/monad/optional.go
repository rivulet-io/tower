package monad

type Optional[T any] struct {
	value *T
	isSet bool
}

func Some[T any](value T) Optional[T] {
	return Optional[T]{value: &value, isSet: true}
}

func None[T any]() Optional[T] {
	return Optional[T]{value: nil, isSet: false}
}

func (o Optional[T]) IsSome() bool {
	return o.isSet
}

func (o Optional[T]) IsNone() bool {
	return !o.isSet
}

func (o Optional[T]) Unwrap() T {
	if !o.isSet {
		panic("called Unwrap on a None value")
	}

	return *o.value
}

func (o Optional[T]) UnwrapOr(defaultValue T) T {
	if o.isSet {
		return *o.value
	}

	return defaultValue
}
