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
func TestStructToString(t *testing.T) {
	// create a new dbItem
	item := mockType{
		Key:       "key1",
		Workspace: "ws1",
		Num:       10,
	}
	str, err := valueToString[mockType](item)
	if err != nil {
		t.Errorf("Error converting value to string: %V", err)
	}
	t.Logf("String value: %V", str)
}

func TestStructFromString(t *testing.T) {
	// create a new dbItem
	item := mockType{
		Key:       "key1",
		Workspace: "ws1",
		Num:       10,
	}
	str, err := valueToString[mockType](item)
	if err != nil {
		t.Errorf("Error converting value to string: %V", err)
	}
	// convert back to value
	var result mockType
	err = valueFromString(str, &result)
	if err != nil {
		t.Errorf("Error converting string to value: %V", err)
	}
	if result != item {
		t.Errorf("Error converting string to value: %V", err)
	}
}

// Test methods on built-in types
func TestToAndFromString(t *testing.T) {
	// test int type
	i := 10
	str, _ := valueToString[int](i)
	var result int
	err := valueFromString(str, &result)
	if err != nil || result != i {
		t.Error("int: Error converting string to value")
	}
	// test int8 type
	i8 := int8(10)
	str, _ = valueToString[int8](i8)
	var result8 int8
	err = valueFromString(str, &result8)
	if err != nil || result8 != i8 {
		t.Error("int8: Error converting string to value")
	}
	// test int16 type
	i16 := int16(10)
	str, _ = valueToString[int16](i16)
	var result16 int16
	err = valueFromString(str, &result16)
	if err != nil || result16 != i16 {
		t.Error("int16: Error converting string to value")
	}
	// test int32 type
	i32 := int32(10)
	str, _ = valueToString[int32](i32)
	var result32 int32
	err = valueFromString(str, &result32)
	if err != nil || result32 != i32 {
		t.Error("int32: Error converting string to value")
	}
	// test int64 type
	i64 := int64(10)
	str, _ = valueToString[int64](i64)
	var result64 int64
	err = valueFromString(str, &result64)
	if err != nil || result64 != i64 {
		t.Error("int64: Error converting string to value")
	}
	// test uint type
	ui := uint(10)
	str, _ = valueToString[uint](ui)
	var resultui uint
	err = valueFromString(str, &resultui)
	if err != nil || resultui != ui {
		t.Error("uint: Error converting string to value")
	}
	// test uint8 type
	ui8 := uint8(10)
	str, _ = valueToString[uint8](ui8)
	var resultui8 uint8
	err = valueFromString(str, &resultui8)
	if err != nil || resultui8 != ui8 {
		t.Error("uint8: Error converting string to value")
	}
	// test uint16 type
	ui16 := uint16(10)
	str, _ = valueToString[uint16](ui16)
	var resultui16 uint16
	err = valueFromString(str, &resultui16)
	if err != nil || resultui16 != ui16 {
		t.Error("uint16: Error converting string to value")
	}
	// test uint32 type
	ui32 := uint32(10)
	str, _ = valueToString[uint32](ui32)
	var resultui32 uint32
	err = valueFromString(str, &resultui32)
	if err != nil || resultui32 != ui32 {
		t.Error("uint32: Error converting string to value")
	}
	// test uint64 type
	ui64 := uint64(10)
	str, _ = valueToString[uint64](ui64)
	var resultui64 uint64
	err = valueFromString(str, &resultui64)
	if err != nil || resultui64 != ui64 {
		t.Error("uint64: Error converting string to value")
	}
	// test float32 type
	f32 := float32(10.5)
	str, _ = valueToString[float32](f32)
	var resultf32 float32
	err = valueFromString(str, &resultf32)
	if err != nil || resultf32 != f32 {
		t.Error("float32: Error converting string to value")
	}
	// test float64 type
	f64 := float64(10.5)
	str, _ = valueToString[float64](f64)
	var resultf64 float64
	err = valueFromString(str, &resultf64)
	if err != nil || resultf64 != f64 {
		t.Error("float64: Error converting string to value")
	}
	// test string type
	s := "test"
	str, _ = valueToString[string](s)
	var results string
	err = valueFromString(str, &results)
	if err != nil || results != s {
		t.Error("string: Error converting string to value")
	}
	// test bool type
	b := true
	str, _ = valueToString[bool](b)
	var resultb bool
	err = valueFromString(str, &resultb)
	if err != nil || resultb != b {
		t.Error("bool: Error converting string to value")
	}
	b2 := false
	str, _ = valueToString[bool](b2)
	var resultb2 bool
	err = valueFromString(str, &resultb2)
	if err != nil || resultb2 != b2 {
		t.Error("bool: Error converting string to value")
	}

}
