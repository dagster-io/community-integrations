package pipes.loaders;

public class PipesEnvVarParamsLoader extends PipesMappingParamsLoader {

    public PipesEnvVarParamsLoader() {
        super(System.getenv());
    }
}
