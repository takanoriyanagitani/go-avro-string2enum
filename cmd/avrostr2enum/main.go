package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	. "github.com/takanoriyanagitani/go-avro-string2enum/util"

	ae "github.com/takanoriyanagitani/go-avro-string2enum/str2enum"

	dh "github.com/takanoriyanagitani/go-avro-string2enum/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-string2enum/avro/enc/hamba"
	sh "github.com/takanoriyanagitani/go-avro-string2enum/str2enum/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var enumColumnName IO[string] = EnvValByKey("ENV_ENUM_COLNAME")

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var enumSet IO[map[string]struct{}] = Bind(
	All(
		schemaContent,
		enumColumnName,
	),
	Lift(func(s []string) (map[string]struct{}, error) {
		return sh.SchemaToEnumSet(
			s[0],
			s[1],
		), nil
	}),
)

var enumValueAlt IO[string] = EnvValByKey("ENV_ENUM_VAL_ALT")

var any2enum IO[ae.AnyToEnum] = Bind(
	enumSet,
	Lift(func(es map[string]struct{}) (ae.AnyToEnum, error) {
		return ae.EnumSet(es).ToAnyToEnum(enumValueAlt), nil
	}),
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	stdin2maps,
	func(
		original iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			any2enum,
			func(a2e ae.AnyToEnum) IO[iter.Seq2[map[string]any, error]] {
				return Bind(
					enumColumnName,
					func(ecol string) IO[iter.Seq2[map[string]any, error]] {
						return a2e.MapsToMaps(
							original,
							ecol,
						)
					},
				)
			},
		)
	},
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
