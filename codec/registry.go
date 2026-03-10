package codec

import (
	"fmt"

	"github.com/asger-noer/pebbles"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// upcastStep is one edge in the upcaster graph: decoding a message named
// fromName and feeding it through fn produces a message named toName.
type upcastStep struct {
	toName string
	fn     func(any) (any, error)
}

// Registrar is the interface implemented by *Registry[A]. It is consumed by the
// fold package, stream.Repository, projection drivers, and test helpers — any
// caller that needs stream identification, hydration, or encoding but does not
// need to name the concrete aggregate type A.
type Registrar interface {
	StreamType() string
	Encode(e pebbles.Event) ([]byte, string, error)
	Hydrate(name string, data []byte) (any, error)
}

// Registry stores protobuf message types and upcasters keyed by fully-qualified
// message name, and provides the encode / decode / hydrate operations on top.
// Upcasters are optional — a message with no newer shape needs none.
//
// A is the stream type this registry is scoped to; its StreamType() method
// provides the stream name so callers never pass a raw string.
//
// Registration is expected to happen at startup; the Registry is safe for
// concurrent reads but not for concurrent writes.
type Registry[A pebbles.StreamTyped] struct {
	types     map[string]protoreflect.MessageType
	upcasters map[string]upcastStep
}

// NewRegistry constructs a Registry scoped to A's stream type. Panics if A's
// StreamType() returns an empty string.
func NewRegistry[A pebbles.StreamTyped]() *Registry[A] {
	var zero A
	if zero.StreamType() == "" {
		panic("codec: stream type must not be empty")
	}
	return &Registry[A]{
		types:     map[string]protoreflect.MessageType{},
		upcasters: map[string]upcastStep{},
	}
}

// StreamType returns the stream type this registry is scoped to.
func (r *Registry[A]) StreamType() string {
	var zero A
	return zero.StreamType()
}

// Register associates T's protobuf message type with the fully-qualified name
// reported by its zero value's descriptor.
func Register[T pebbles.Event, A pebbles.StreamTyped](r *Registry[A]) {
	var zero T
	r.types[messageName(zero)] = zero.ProtoReflect().Type()
}

// RegisterUpcaster registers a transform from From to To, keyed by From's
// message name. Hydrate walks these name -> name steps until no upcaster is
// registered for the current name, which is how "latest" is determined — there
// is no explicit version number.
//
// Panics if From and To share the same message name.
func RegisterUpcaster[From, To pebbles.Event, A pebbles.StreamTyped](r *Registry[A], fn func(From) (To, error)) {
	var zeroFrom From
	var zeroTo To
	fromName := messageName(zeroFrom)
	toName := messageName(zeroTo)

	if fromName == toName {
		panic(fmt.Sprintf("codec: upcaster must change message name, got %q -> %q", fromName, toName))
	}

	r.upcasters[fromName] = upcastStep{
		toName: toName,
		fn: func(in any) (any, error) {
			from, ok := in.(From)
			if !ok {
				return nil, fmt.Errorf("codec: %s upcaster expected %T, got %T", fromName, zeroFrom, in)
			}
			return fn(from)
		},
	}
}

// Encode marshals e as protobuf. Returns the bytes plus e's fully-qualified
// message name.
func (r *Registry[A]) Encode(e pebbles.Event) ([]byte, string, error) {
	name := messageName(e)
	data, err := proto.Marshal(e)
	if err != nil {
		return nil, "", fmt.Errorf("codec: encode %s: %w", name, err)
	}
	return data, name, nil
}

// Decode unmarshals data as the message registered under name and returns the
// value at its stored shape — no upcasting. Useful for tooling that wants to
// inspect events in their original shape.
func (r *Registry[A]) Decode(name string, data []byte) (any, error) {
	t, ok := r.types[name]
	if !ok {
		return nil, fmt.Errorf("codec: no message type for %s", name)
	}
	msg := t.New().Interface()
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("codec: decode %s: %w", name, err)
	}
	return msg, nil
}

// Hydrate decodes data and walks the upcaster chain until no upcaster is
// registered for the current message name. Callers assert the returned any to
// their aggregate's event interface.
func (r *Registry[A]) Hydrate(name string, data []byte) (any, error) {
	payload, err := r.Decode(name, data)
	if err != nil {
		return nil, err
	}

	current := name
	visited := map[string]bool{current: true}
	for {
		step, ok := r.upcasters[current]
		if !ok {
			return payload, nil
		}
		if visited[step.toName] {
			return nil, fmt.Errorf("codec: upcaster cycle detected at %s -> %s", current, step.toName)
		}
		payload, err = step.fn(payload)
		if err != nil {
			return nil, fmt.Errorf("codec: %s -> %s: %w", current, step.toName, err)
		}
		current = step.toName
		visited[current] = true
	}
}

func messageName(e pebbles.Event) string {
	return string(e.ProtoReflect().Descriptor().FullName())
}
