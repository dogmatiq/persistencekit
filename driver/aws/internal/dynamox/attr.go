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
		return v, fmt.Errorf("integrity error: missing %q attribute", name)
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
			"integrity error: %q attribute should be %s not %s",
			name,
			reflect.TypeOf(v).Name(),
			reflect.TypeOf(a).Name(),
		)
	}

	return v, true, nil
}

// AsBytes fetches a binary attribute from an item.
func AsBytes(
	item map[string]types.AttributeValue,
	name string,
) ([]byte, error) {
	v, err := AttrAs[*types.AttributeValueMemberB](item, name)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

// TryAsBool fetches an optional boolean attribute from an item.
func TryAsBool(
	item map[string]types.AttributeValue,
	name string,
) (bool, bool, error) {
	v, ok, err := TryAttrAs[*types.AttributeValueMemberBOOL](item, name)
	if err != nil {
		return false, false, err
	}
	if !ok {
		return false, false, nil
	}
	return v.Value, true, nil
}

var (
	// True is a [types.AttributeValueMemberBOOL] for true.
	True = &types.AttributeValueMemberBOOL{Value: true}

	// False is a [types.AttributeValueMemberBOOL] for false.
	False = &types.AttributeValueMemberBOOL{Value: false}
)
