package io.dagster.pipes.utils;

import org.junit.jupiter.api.Test;

import io.dagster.pipes.data.PipesMetadata;
import io.dagster.pipes.utils.MetadataBuilder;
import io.dagster.types.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("PMD.JUnitAssertionsShouldIncludeMessage")
class MetadataBuilderTest {

    @Test
    void testBuildFromFloat() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", 123F);
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        assertTrue(meta.containsKey("test"));
        assertEquals(meta.get("test").getType(), Type.FLOAT);
    }

    @Test
    void testBuildFromInt() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", 123);
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        assertTrue(meta.containsKey("test"));
        assertEquals(meta.get("test").getType(), Type.INT);
    }

    @Test
    void testBuildFromString() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", "123");
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        assertTrue(meta.containsKey("test"));
        assertEquals(meta.get("test").getType(), Type.TEXT);
    }
    @Test
    void testBuildFromBoolean() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", true);
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        assertTrue(meta.containsKey("test"));
        assertEquals(meta.get("test").getType(), Type.BOOL);
    }
    @Test
    void testBuildFromMap() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", new HashMap<>());
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        assertTrue(meta.containsKey("test"));
        assertEquals(meta.get("test").getType(), Type.JSON);
    }
    @Test
    void testBuildFromList() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", new ArrayList<>());
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        assertTrue(meta.containsKey("test"));
        assertEquals(meta.get("test").getType(), Type.JSON);
    }

    @Test
    void testBuildFromArray() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", new int[]{});
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        assertTrue(meta.containsKey("test"));
        assertEquals(meta.get("test").getType(), Type.JSON);
    }

}
