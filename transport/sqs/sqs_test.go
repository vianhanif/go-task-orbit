package sqs

import (
	"testing"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	cfg.defaults()

	if cfg.MaxMessages != 10 {
		t.Errorf("expected MaxMessages 10, got %d", cfg.MaxMessages)
	}
	if cfg.WaitTime != 20 {
		t.Errorf("expected WaitTime 20, got %d", cfg.WaitTime)
	}
	if cfg.VisibilityTimeout != 30 {
		t.Errorf("expected VisibilityTimeout 30, got %d", cfg.VisibilityTimeout)
	}
}

func TestConfigDefaultsPreservesSetValues(t *testing.T) {
	cfg := Config{
		MaxMessages:       5,
		WaitTime:          10,
		VisibilityTimeout: 60,
	}
	cfg.defaults()

	if cfg.MaxMessages != 5 {
		t.Errorf("expected MaxMessages 5, got %d", cfg.MaxMessages)
	}
	if cfg.WaitTime != 10 {
		t.Errorf("expected WaitTime 10, got %d", cfg.WaitTime)
	}
	if cfg.VisibilityTimeout != 60 {
		t.Errorf("expected VisibilityTimeout 60, got %d", cfg.VisibilityTimeout)
	}
}

func TestAttributeConversion(t *testing.T) {
	input := map[string]string{
		"key1": "val1",
		"key2": "val2",
	}
	attrs := toSQSAttributes(input)
	if len(attrs) != 2 {
		t.Fatalf("expected 2 attributes, got %d", len(attrs))
	}
	if *attrs["key1"].StringValue != "val1" {
		t.Errorf("expected val1, got %s", *attrs["key1"].StringValue)
	}

	roundtrip := fromSQSAttributes(attrs)
	if roundtrip["key1"] != "val1" {
		t.Errorf("roundtrip failed for key1: got %s", roundtrip["key1"])
	}
	if roundtrip["key2"] != "val2" {
		t.Errorf("roundtrip failed for key2: got %s", roundtrip["key2"])
	}
}

func TestNilAttributeConversion(t *testing.T) {
	if attrs := toSQSAttributes(nil); attrs != nil {
		t.Error("expected nil for nil input")
	}
	if attrs := fromSQSAttributes(nil); attrs != nil {
		t.Error("expected nil for nil input")
	}
}
