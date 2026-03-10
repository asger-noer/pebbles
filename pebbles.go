package pebbles

import (
	"errors"
	"fmt"
	"slices"
	"time"

	metav1 "github.com/asger-noer/pebbles/gen/pebbles/meta/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Deleted is a tombstone event appended to a stream when it is permanently
// removed. It carries no payload; the envelope (StreamID, At) is the record.
type Deleted = metav1.Deleted

// DeletedName is the fully-qualified protobuf message name identifying
// [Deleted], used wherever a stored event's Name is checked for the tombstone.
var DeletedName = string((&Deleted{}).ProtoReflect().Descriptor().FullName())

// ErrDeleted is returned when an operation targets a stream that has been
// permanently deleted.
var ErrDeleted = errors.New("pebbles: stream has been deleted")

// Headers represents a collection of key-value pairs, where each key can have
// multiple values. This is useful for storing metadata associated with events,
// such as TraceId, timestamps, or any other relevant information.
type Headers map[string][]string

// Event is a domain event. Its identity is the fully-qualified name of the
// underlying protobuf message descriptor (e.g. "myapp.order.v1.OrderPlaced"),
// derived via ProtoReflect().Descriptor().FullName() — there is no separate
// Kind or Version to keep in sync with the schema.
type Event interface {
	proto.Message
}

// StreamTyped reports the stream type it belongs to. Both [Aggregate] and
// [Stream] satisfy it, so either can name a stream for [Subscribe].
type StreamTyped interface {
	StreamType() string
}

// Aggregate bundles a stream type with the rule that folds its events into
// state.
//   - S is the aggregate state
//   - E is the sum interface of events the aggregate produces.
type Aggregate[S any, E Event] interface {
	StreamTyped
	// Initial returns the zero state for a fresh stream.
	Initial() S
	// Apply folds an event into state and returns the next state.
	Apply(state S, event E) S
}

// Stream folds a single stream's events into a read model M, over the
// event sum interface E. It has the same shape as [Aggregate] but expresses
// a read model rather than write-side state.
type Stream[M any, E Event] interface {
	StreamTyped
	// Initial returns the zero read model.
	Initial() M
	// Apply folds an event into the model and returns the next model.
	Apply(model M, event E) M
}

// Projector folds events from many streams into a read model M, over a sum
// interface E (a projection-specific union of the event types it consumes).
// It mirrors [Aggregate]: typed events, no envelope. When you need the raw
// [Pebble] (metadata) or events that do not share a sum interface, use the
// lower-level [projection.Handler] instead.
type Projector[M any, E Event] interface {
	// Name identifies the projection; used as the checkpoint / snapshot key.
	Name() string
	// Subscriptions declares the bounded set of events the projection folds.
	// It must be non-empty and every stream type must list at least one kind.
	Subscriptions() Subscriptions
	// Initial returns the zero read model.
	Initial() M
	// Apply folds an event into the model and returns the next model.
	Apply(state M, event E) M
}

// KeyedProjector folds events into one model M per grouping key (a uuid), over a
// sum interface E. It is the common, managed form: Key names the key when an
// event carries it (typically the creation event), and the framework records
// streamID -> key and inherits it for the rest of that stream. Events whose Key
// returns ok=false are routed to the key already learned for their stream.
type KeyedProjector[M any, E Event] interface {
	Projector[M, E]
	// Key returns the grouping key when the event carries it. ok=false means
	// "resolve from the key already learned for this event's stream".
	Key(event E) (key uuid.UUID, ok bool)
}

// RoutedProjector is the explicit form of a keyed projection: Route resolves the
// grouping key(s) itself via idx — learning keys from creation events and looking
// up payload foreign keys carried by events on other streams. It may route one
// event to several keys. Use it when the managed [KeyedProjector.Key] is not
// enough (cross-stream FKs); otherwise prefer [KeyedProjector].
type RoutedProjector[M any, E Event] interface {
	Projector[M, E]
	// Route returns the grouping key(s) the event applies to, using idx to learn
	// and resolve keys. An empty result skips the event.
	Route(p Pebble[E], idx Index) []uuid.UUID
}

// Pebble is a stored event paired with the metadata needed to place it in a
// stream. T is typically a sum interface for one aggregate (e.g. OrderEvent).
type Pebble[T Event] struct {
	ID         uuid.UUID
	StreamType string
	StreamID   uuid.UUID
	Sequence   int64
	At         time.Time
	Headers    Headers
	Event      T
}

// Subscriptions binds a projection to the exact (stream type, message name)
// pairs it consumes, e.g. {"order": {"myapp.order.v1.Created"}}. Message names
// are globally unique, but the binding stays per stream type so a projection's
// coverage reads the same shape as its events' stream layout.
type Subscriptions map[string][]string

// Includes reports whether the (streamType, name) pair is within the binding.
func (s Subscriptions) Includes(streamType, name string) bool {
	return slices.Contains(s[streamType], name)
}

// Validate ensures the binding is non-empty and every stream type lists at
// least one name, so wiring code can reject misconfiguration at startup.
func (s Subscriptions) Validate() error {
	if len(s) == 0 {
		return fmt.Errorf("pebbles: subscriptions must not be empty")
	}
	for streamType, names := range s {
		if len(names) == 0 {
			return fmt.Errorf("pebbles: subscriptions for stream type %q list no message names", streamType)
		}
	}
	return nil
}

// Subscribe builds a [Subscriptions] binding from zero values instead of string
// literals: the stream type comes from a [StreamTyped] (an aggregate or projector)
// and each message name from an [Event]'s protobuf descriptor — both read only
// from the value's methods, so the zero value suffices. Chain [Subscriptions.And]
// to bind events from more streams. This keeps the binding in sync with the real
// types: a renamed message or StreamType is a compile error, not a silent
// mismatch.
//
//	pebbles.Subscribe(OrderAggregate{}, &OrderPlaced{}, &ItemAdded{}).
//		And(CustomerAggregate{}, &CustomerRegistered{})
func Subscribe(stream StreamTyped, events ...Event) Subscriptions {
	return Subscriptions{}.And(stream, events...)
}

// And binds the events of another stream into the [Subscriptions] and returns it,
// for chaining after [Subscribe].
func (s Subscriptions) And(stream StreamTyped, events ...Event) Subscriptions {
	streamType := stream.StreamType()
	for _, e := range events {
		name := string(e.ProtoReflect().Descriptor().FullName())
		if !slices.Contains(s[streamType], name) {
			s[streamType] = append(s[streamType], name)
		}
	}
	return s
}

// Index learns and resolves grouping keys across events for a keyed projection
// (e.g. orderStreamID -> customerID). It is in scope for the duration of a run:
// Put records a mapping learned from an event, Get resolves one learned earlier.
type Index interface {
	Get(id uuid.UUID) (uuid.UUID, bool)
	Put(id, key uuid.UUID)
}
