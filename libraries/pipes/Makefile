install_quicktype:
	npm install -g quicktype

jsonschema_rust:
	printf -- '%s\0' jsonschema/*.schema.json | xargs -0 \
	quicktype -s schema -l rust \
		--visibility public --derive-debug --derive-clone --derive-partial-eq -o \
		implementations/rust/src/types.rs

jsonschema_java:
	for schema in jsonschema/*.schema.json; do \
	quicktype --package types --array-type list -s schema -l java -o implementations/java/dagster-pipes-java/src/main/java/types/$$(basename $$schema .schema.json).java $$schema ; \
	done

jsonschema: jsonschema_rust jsonschema_java

