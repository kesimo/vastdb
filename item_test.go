package vastdb

import "testing"

type mockType struct {
	Key       string
	Workspace string
	Num       int
}

// Test correctness of all item methods
func TestValueToString(t *testing.T) {
	// create a new dbItem
	item := mockType{
		Key:       "key1",
		Workspace: "ws1",
		Num:       10,
	}
	str, err := valueToString[mockType](item)
	if err != nil {
		t.Errorf("Error converting value to string: %v", err)
	}
	t.Logf("String value: %v", str)
}

func TestStringToValue(t *testing.T) {
	// create a new dbItem
	item := mockType{
		Key:       "key1",
		Workspace: "ws1",
		Num:       10,
	}
	str, err := valueToString[mockType](item)
	if err != nil {
		t.Errorf("Error converting value to string: %v", err)
	}
	// convert back to value
	var result mockType
	err = valueFromString(str, &result)
	if err != nil {
		t.Errorf("Error converting string to value: %v", err)
	}
	if result != item {
		t.Errorf("Error converting string to value: %v", err)
	}
}
