// This file is part of the vastDB project.
// Last modified : Kevin Eder
// Creation date: 14.10.2022
// License: MIT
// Use of this source code is governed by a MIT license that can be found in the LICENSE file
// at https://github.com/kesimo/vastdb/blob/main/LICENSE

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
