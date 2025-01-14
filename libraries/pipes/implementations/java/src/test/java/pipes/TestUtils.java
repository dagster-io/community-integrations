package pipes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestUtils {

    public static String getLastLine(String path) throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            Queue<String> queue = new LinkedList<>();
            lines.forEach(line -> {
                if (!queue.isEmpty()) {
                    queue.remove();
                }
                queue.add(line);
            });

            return String.join("\n", queue);
        }
    }

    public static Set<Map.Entry<String, String>> sanitizeMapEntries(Map<String, String> map) {
        return map.entrySet().stream()
            .map(entry -> new AbstractMap.SimpleEntry<>(
                    entry.getKey(),
                    removeTrailingQuotes(entry.getValue()))
            )
            .collect(Collectors.toSet());
    }

    public static String removeTrailingQuotes(String value) {
        if (value == null) {
            return null;
        }
        return value.replaceAll("^\"|\"$", "");
    }
}
