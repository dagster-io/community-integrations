#!bash

# Java
for schema in jsonschema/pipes/*.schema.json; do
    echo "Generating Java class for $schema"
    quicktype --package types -s schema -l java -o src/main/java/types/$(basename $schema .schema.json).java $schema
done
