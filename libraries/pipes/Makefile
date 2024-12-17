install_quicktype:
	npm install -g quicktype

jsonschema_rust:
	printf -- '%s\0' jsonschema/*.schema.json | xargs -0 \
	quicktype -s schema -l rust \
		--visibility public --derive-debug --derive-clone --derive-partial-eq -o \
		implementations/rust/src/types.rs

jsonschema: jsonschema_rust
