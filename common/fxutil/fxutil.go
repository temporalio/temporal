package fxutil

import (
	"reflect"
	"strings"
	"sync"

	"github.com/jinzhu/copier"
	"go.uber.org/fx"
)

var (
	fxInType  = reflect.TypeOf(&fx.In{}).Elem()
	fxOutType = reflect.TypeOf(&fx.Out{}).Elem()
	outCache  sync.Map
)

// SupplyAllFields turns any struct (including an fx.In struct) into an fx.Option that supplies
// all of its fields. It panics if the argument is not a struct or if it has unexported fields.
func SupplyAllFields(s any) fx.Option {
	ptr := reflect.New(makeFxOutType(reflect.TypeOf(s)))
	if err := copier.Copy(ptr.Interface(), s); err != nil {
		panic(err)
	}
	return fx.Supply(ptr.Elem().Interface()) // TODO: try without Elem to use the pointer?
}

func makeFxOutType(in reflect.Type) reflect.Type {
	if v, ok := outCache.Load(in); ok {
		return v.(reflect.Type)
	}

	newField := reflect.StructField{
		Name:      fxOutType.Name(),
		Type:      fxOutType,
		Anonymous: true,
	}
	fields := make([]reflect.StructField, in.NumField(), in.NumField()+1)
	found := false
	for i := range fields {
		f := in.Field(i)
		if f.Anonymous && f.Type == fxInType {
			f = newField
			found = true
		} else if strings.Contains(string(f.Tag), `group:`) {
			// clear group tags when turning In to Out
			f.Tag = ""
		}
		fields[i] = f
	}
	if !found {
		fields = append(fields, newField)
	}

	t := reflect.StructOf(fields)
	outCache.Store(in, t)
	return t
}
