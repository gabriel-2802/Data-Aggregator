package operators;

import database.ConcurrentDb;

import java.util.List;

/**
 * Class that performs merge operations on concurrent db
 */
public class DataMerger implements Operator {
    private final List<ConcurrentDb.MergeFunction> mergeFunctionList;

    /**
     * Constructs a DataMerger with a list of merge functions to execute
     *
     * @param mergeFunctionList the list of merge functions this thread should execute
     */
    public  DataMerger(List<ConcurrentDb.MergeFunction> mergeFunctionList) {
        this.mergeFunctionList = mergeFunctionList;
    }

    /**
     * Executes all assigned merge functions sequentially
     */
    @Override
    public void execute() {
        for (var f : mergeFunctionList) {
            f.compute();
        }
    }
}
