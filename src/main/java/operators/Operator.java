package operators;

/**
 * Interface representing an operation that can be executed by a thread sequentially
 */
public interface Operator {
    /**
     * Executes the operation.
     */
    void execute();
}
