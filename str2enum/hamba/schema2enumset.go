package str2enum

import (
	"iter"
	"maps"
	"log"

	ha "github.com/hamba/avro/v2"
)

func EnumSchemaToEnumSet(es *ha.EnumSchema) map[string]struct{} {
	var symbols []string = es.Symbols()
	var i iter.Seq2[string, struct{}] = func(
		yield func(string, struct{}) bool,
	) {
		for _, sym := range symbols {
			yield(sym, struct{}{})
		}
	}
	return maps.Collect(i)
}

func FieldToEnumSet(field *ha.Field) map[string]struct{} {
	var s ha.Schema = field.Type()
	switch t := s.(type) {
	case *ha.EnumSchema:
		return EnumSchemaToEnumSet(t)
	default:
		log.Printf("invalid schema(expected enum): %v\n", field)
		return map[string]struct{}{}
	}
}

func FieldsToEnumSet(
	fields []*ha.Field,
	name string,
) map[string]struct{} {
	for _, field := range fields {
		var fname string = field.Name()
		if name == fname {
			return FieldToEnumSet(field)
		}
	}
	log.Printf("invalid schema: %v\n", fields)
	return map[string]struct{}{}
}

func RecordSchemaToEnumSet(
	r *ha.RecordSchema,
	name string,
) map[string]struct{} {
	return FieldsToEnumSet(r.Fields(), name)
}

func SchemaToEnumSetHamba(
	s ha.Schema,
	name string,
) map[string]struct{} {
	switch t := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToEnumSet(t, name)
	default:
		log.Printf("invalid schema: %v\n", t)
		return map[string]struct{}{}
	}
}

func SchemaToEnumSet(
	schema string,
	name string,
) map[string]struct{} {
	parsed, e := ha.Parse(schema)
	switch e {
	case nil:
		return SchemaToEnumSetHamba(parsed, name)
	default:
		log.Printf("invalid schema: %v\n", e)
		return map[string]struct{}{}
	}
}
