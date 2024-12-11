package pipes.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pipes.utils.PipesUtils;
import types.Method;

import java.util.HashMap;
import java.util.Map;

class PipesExceptionTest {

    @Test
    void testClose() {
        Exception exception = new Exception(
            "Exception message", new Exception("Inner exception")
        );
        PipesException pipesException = new PipesException(exception);
        Map<String, Object> payload = new HashMap<>();
        payload.put("exception", pipesException);
        String message = PipesUtils.makeMessage(Method.CLOSED, payload).toString();
        Assertions.assertTrue(message.startsWith(
            "{\"__dagster_pipes_version\":\"0.1\",\"method\":\"closed\",\"params\":{\"exception\":{\"name\":\"java.lang.Exception\",\"message\":\"Exception message\",\"cause\":{\"name\":\"java.lang.Exception\",\"message\":\"Inner exception\",\"cause\":null,\"stack\":null,\"context\":[]},\"stack\":["
        ));
    }

}