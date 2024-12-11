package pipes.utils;

import org.junit.jupiter.api.Test;
import pipes.data.PipesMetadata;
import types.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("PMD.JUnitAssertionsShouldIncludeMessage")
class MetadataBuilderTest {

    @Test
    void testBuildFromFloat() {
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(123F);
        assertTrue(meta.containsKey("float"));
        assertEquals(meta.get("float").getType(), Type.FLOAT);
    }

    @Test
    void testBuildFromInt() {
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(123);
        assertTrue(meta.containsKey("int"));
        assertEquals(meta.get("int").getType(), Type.INT);
    }

    @Test
    void testBuildFromString() {
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom("123");
        assertTrue(meta.containsKey("text"));
        assertEquals(meta.get("text").getType(), Type.TEXT);
    }
    @Test
    void testBuildFromBoolean() {
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(true);
        assertTrue(meta.containsKey("boolean"));
        assertEquals(meta.get("boolean").getType(), Type.BOOL);
    }
    @Test
    void testBuildFromMap() {
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(new HashMap<>());
        assertTrue(meta.containsKey("json"));
        assertEquals(meta.get("json").getType(), Type.JSON);
    }
    @Test
    void testBuildFromList() {
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(new ArrayList<>());
        assertTrue(meta.containsKey("json"));
        assertEquals(meta.get("json").getType(), Type.JSON);
    }

    @Test
    void testBuildFromArray() {
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(new int[]{1, 2, 3});
        assertTrue(meta.containsKey("json"));
        assertEquals(meta.get("json").getType(), Type.JSON);
    }

}