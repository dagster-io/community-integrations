package pipes.logger;

import java.util.logging.Level;

class PipesLogLevel extends Level {

    private static final long serialVersionUID = 3704362111109982327L;

    protected PipesLogLevel(String name, int value) {
        super(name, value);
    }
}
