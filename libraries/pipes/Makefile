install_quicktype:
	npm install -g quicktype

jsonschema_rust:
	printf -- '%s\0' jsonschema/*.schema.json | xargs -0 \
	quicktype -s schema -l rust \
		--visibility public --derive-debug --derive-clone --derive-partial-eq -o \
		implementations/rust/src/types.rs
	cd implementations/rust && cargo fmt

# don't auto-generate PipesException (quicktype doesn't play nice with recursive definitions)
jsonschema_typescript:
	printf -- '%s\n' jsonschema/*.schema.json | \
	grep -v 'PipesException' | \
	xargs quicktype -s schema -l typescript  \
		--just-types -o \
		implementations/typescript/src/types.ts

jsonschema: jsonschema_rust jsonschema_typescript
