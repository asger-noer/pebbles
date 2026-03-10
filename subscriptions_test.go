package pebbles_test

import (
	"testing"

	"github.com/asger-noer/pebbles"
	orderv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v1"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	paymentv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/payment/v1"
)

type orderAgg struct{}

func (orderAgg) StreamType() string { return "order" }

type paymentAgg struct{}

func (paymentAgg) StreamType() string { return "payment" }

func name(e pebbles.Event) string {
	return string(e.ProtoReflect().Descriptor().FullName())
}

func TestSubscribeDerivesFromZeroValues(t *testing.T) {
	subs := pebbles.Subscribe(orderAgg{}, &orderv1.CartMade{}, &orderv2.ItemAdded{}).
		And(paymentAgg{}, &paymentv1.PaymentCaptured{})

	if err := subs.Validate(); err != nil {
		t.Fatalf("valid binding rejected: %v", err)
	}
	placedName := name(&orderv1.CartMade{})
	itemName := name(&orderv2.ItemAdded{})
	capturedName := name(&paymentv1.PaymentCaptured{})
	for _, c := range []struct {
		streamType, kind string
		want             bool
	}{
		{"order", placedName, true},
		{"order", itemName, true},
		{"payment", capturedName, true},
		{"order", capturedName, false}, // name bound to a different stream
		{"payment", placedName, false},
	} {
		if got := subs.Includes(c.streamType, c.kind); got != c.want {
			t.Errorf("Includes(%q, %q) = %v, want %v", c.streamType, c.kind, got, c.want)
		}
	}
}

func TestSubscribeDedupesKinds(t *testing.T) {
	subs := pebbles.Subscribe(orderAgg{}, &orderv1.CartMade{}, &orderv1.CartMade{})
	if len(subs["order"]) != 1 {
		t.Fatalf("want 1 name after dedupe, got %v", subs["order"])
	}
}
