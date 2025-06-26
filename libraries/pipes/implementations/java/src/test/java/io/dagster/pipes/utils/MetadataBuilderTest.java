package io.dagster.pipes.utils;

import io.dagster.pipes.data.PipesMetadata;
import io.dagster.types.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitAssertionsShouldIncludeMessage")
class MetadataBuilderTest {

    @Test
    void testBuildFromFloat() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", 123F);
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        Assertions.assertTrue(meta.containsKey("test"));
        Assertions.assertEquals(Type.FLOAT, meta.get("test").getType());
    }

    @Test
    void testBuildFromInt() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", 123);
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        Assertions.assertTrue(meta.containsKey("test"));
        Assertions.assertEquals(Type.INT, meta.get("test").getType());
    }

    @Test
    void testBuildFromString() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", "123");
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        Assertions.assertTrue(meta.containsKey("test"));
        Assertions.assertEquals(Type.TEXT, meta.get("test").getType());
    }

    @Test
    void testBuildFromBoolean() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", true);
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        Assertions.assertTrue(meta.containsKey("test"));
        Assertions.assertEquals(Type.BOOL, meta.get("test").getType());
    }

    @Test
    void testBuildFromMap() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", new HashMap<>());
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        Assertions.assertTrue(meta.containsKey("test"));
        Assertions.assertEquals(Type.JSON, meta.get("test").getType());
    }

    @Test
    void testBuildFromList() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", new ArrayList<>());
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        Assertions.assertTrue(meta.containsKey("test"));
        Assertions.assertEquals(Type.JSON, meta.get("test").getType());
    }

    @Test
    void testBuildFromArray() {
        final Map<String, Object> testMap = new HashMap<>();
        testMap.put("test", new int[]{});
        final Map<String, PipesMetadata> meta = MetadataBuilder.buildFrom(testMap);
        Assertions.assertTrue(meta.containsKey("test"));
        Assertions.assertEquals(Type.JSON, meta.get("test").getType());
    }

}
