import { PipesContext } from "./pipes_context";

/* A basic logger class that wraps logging to the dagster orchestration process via a `PipesContext`.

   The PipesLogger receives a reference to the PipesContext in the constructor, and allows logging
   messages to the dagster orchestration process, at various levels.
*/
export class PipesLogger {
    context: PipesContext

    public constructor(context: PipesContext) {
        this.context = context
    }

    public debug(message: string) {
        this.context.log(message, "DEBUG")
    }

    public info(message: string) {
        this.context.log(message, "INFO")
    }

    public warning(message: string) {
        this.context.log(message, "WARNING")
    }

    public error(message: string) {
        this.context.log(message, "ERROR")   
    }

    public critical(message: string) {
        this.context.log(message, "CRITICAL")
    }
}