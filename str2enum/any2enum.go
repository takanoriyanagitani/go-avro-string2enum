package str2enum

import (
	"context"
	"errors"
	"iter"

	. "github.com/takanoriyanagitani/go-avro-string2enum/util"
)

var (
	ErrInvalidEnum error = errors.New("invalid enum")
)

type AnyToEnum func(any) IO[string]

type EnumSet map[string]struct{}

func (s EnumSet) ToAnyToEnum(alt IO[string]) AnyToEnum {
	return func(input any) IO[string] {
		return func(ctx context.Context) (string, error) {
			var raw string
			switch t := input.(type) {
			case string:
				raw = t
			default:
				return "", ErrInvalidEnum
			}

			_, found := s[raw]
			switch found {
			case true:
				return raw, nil
			default:
				return alt(ctx)
			}
		}
	}
}

func (a AnyToEnum) MapsToMaps(
	original iter.Seq2[map[string]any, error],
	enumColumnName string,
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			buf := map[string]any{}

			for row, e := range original {
				clear(buf)

				if nil != e {
					yield(buf, e)
					return
				}

				for key, val := range row {
					if key != enumColumnName {
						buf[key] = val
						continue
					}

					var ecol any = row[key]
					mapd, e := a(ecol)(ctx)
					if nil != e {
						yield(buf, e)
						return
					}

					buf[key] = mapd
				}

				if !yield(buf, nil){
					return
				}
			}
		}, nil
	}
}
