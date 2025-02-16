const PIPES_METADATA_TYPE_INFER = "__infer__"

export function normalizeMetadata(
    metadata: Record<string, any>,
): Record<string, any> {
    const normalizedMetadata: Record<string, any> = {};
    
    for (const [key, value] of Object.entries(metadata)) {
        if ((typeof value == "object") && ("type" in value) && ("raw_value" in value)) {
            normalizedMetadata[key] = value
        }
        else {
            normalizedMetadata[key] = { raw_value: value, type: PIPES_METADATA_TYPE_INFER };
        }
    }
    
    return normalizedMetadata;
}
