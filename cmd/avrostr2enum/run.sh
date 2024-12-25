#!/bin/sh

genavro(){
	cat ./sample.d/sample.jsonl |
		ENV_SCHEMA_FILENAME=./sample.d/input.avsc \
		json2avrows |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
export ENV_ENUM_COLNAME=status
export ENV_ENUM_VAL_ALT=UNSPECIFIED

cat sample.d/sample.avro |
	./avrostr2enum |
	rq \
		--input-avro \
		--output-json |
	jq -c
