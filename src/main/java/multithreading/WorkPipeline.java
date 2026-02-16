package multithreading;

import operators.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * A pipelinefor executing a sequence of stages in order
 */
public class WorkPipeline {
    // The ordered list of stages to execute in the pipeline
    private final List<PipelineStage> stages;

    /**
     * Constructs a new empty WorkPipeline
     */
    public WorkPipeline() {
        this.stages = new ArrayList<>();
    }
    /**
     * Adds a stage to the end of the pipeline
     *
     * @param stage the pipeline stage to add
     */
    public void addStage(PipelineStage stage) {
        stages.add(stage);
    }

    /**
     * Executes all stages in the pipeline sequentially
     */
    public void execute() {
        for (PipelineStage stage : stages) {
            stage.execute();
        }
    }

    /**
     * Represents a single stage in the work pipeline
     */
    public interface PipelineStage {
        /**
         * Executes this pipeline stage
         */
        void execute();
    }

    /**
     * A pipeline stage that synchronizes multiple threads using a CyclicBarrier
     */
    public static class SynchronizationStage implements PipelineStage {
        private final CyclicBarrier barrier;

        /**
         * Constructs a SynchronizationStage with the given barrier.
         *
         * @param barrier the CyclicBarrier to use for synchronization
         */
        public SynchronizationStage(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        /**
         * Executes the synchronization stage by waiting at the barrier
         *
         * @throws RuntimeException if the thread is interrupted or the barrier is broken
         */
        @Override
        public void execute() {
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException("Synchronization failed", e);
            }
        }
    }

    /**
     * A pipeline stage that executes a custom action defined by a Runnable
     */
    public static class ActionStage implements PipelineStage {
        private final Runnable action;

        /**
         * Constructs an ActionStage with the given runnable action
         *
         * @param action the action to execute in this stage
         */
        public ActionStage(Runnable action) {
            this.action = action;
        }

        /**
         * Executes the custom action by calling its run method.
         */
        @Override
        public void execute() {
            action.run();
        }
    }

    /**
     * A pipeline stage that executes a specified Operator
     */
    public static class OperatorStage implements PipelineStage {
        private final Operator operator;

        /**
         * Constructs an OperatorStage with the given operator
         *
         * @param operator the operator to execute in this stage
         */
        public OperatorStage(Operator operator) {
            this.operator = operator;
        }

        /**
         * Executes the operator's execute method
         */
        @Override
        public void execute() {
            operator.execute();
        }
    }

}

