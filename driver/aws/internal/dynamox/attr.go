package dynamox

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// AttrAs fetches an attribute of type T from an item.
//
// It returns an error if the item is absent or a different type.
func AttrAs[T types.AttributeValue](
	item map[string]types.AttributeValue,
	name string,
) (v T, err error) {
	v, ok, err := TryAttrAs[T](item, name)
	if err != nil {
		return v, err
	}
	if !ok {
		return v, fmt.Errorf("item is corrupt: missing %q attribute", name)
	}
	return v, nil
}

// TryAttrAs fetches an attribute of type T from an item.
//
// It returns an error if the item is a different type.
func TryAttrAs[T types.AttributeValue](
	item map[string]types.AttributeValue,
	name string,
) (v T, ok bool, err error) {
	a, ok := item[name]
	if !ok {
		return v, false, nil
	}

	v, ok = a.(T)
	if !ok {
		return v, false, fmt.Errorf(
			"item is corrupt: %q attribute should be %s not %s",
			name,
			reflect.TypeOf(v).Name(),
			reflect.TypeOf(a).Name(),
		)
	}

	return v, true, nil
}
