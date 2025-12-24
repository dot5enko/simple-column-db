package compression

import (
	"reflect"
	"sort"
)

type AlignmentReport struct {
	StructSize    uintptr
	OptimalSize   uintptr
	WastedBytes   uintptr
	IsWellAligned bool
}

func GetWellAlignedStructReport(v any) AlignmentReport {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("not a struct")
	}

	type field struct {
		size  uintptr
		align uintptr
	}

	fields := make([]field, 0, t.NumField())
	maxAlign := uintptr(1)

	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i).Type
		a := uintptr(ft.Align())
		s := ft.Size()
		fields = append(fields, field{s, a})
		if a > maxAlign {
			maxAlign = a
		}
	}

	// Optimal order: descending alignment
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].align == fields[j].align {
			return fields[i].size > fields[j].size
		}
		return fields[i].align > fields[j].align
	})

	var offset uintptr
	for _, f := range fields {
		if rem := offset % f.align; rem != 0 {
			offset += f.align - rem
		}
		offset += f.size
	}

	if rem := offset % maxAlign; rem != 0 {
		offset += maxAlign - rem
	}

	actualSize := t.Size()
	optimalSize := offset
	wellAligned := actualSize == optimalSize
	wasted := actualSize - optimalSize
	return AlignmentReport{IsWellAligned: wellAligned, WastedBytes: wasted}
}
