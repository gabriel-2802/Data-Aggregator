package multithreading;

import auxs.Constants;
import operators.*;
import database.ConcurrentDb;
import database.SequentialDb;

import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * Represents a worker thread that processes news articles in parallel
 */
public class WorkerThread extends Thread implements Runnable {
    // metadata
    private final int threadId;
    private final int numThreads;

    // all files in MAIN DB
    private final List<String> allFiles;

    // partitioner to divide work among threads from shared lists
    private final WorkPartitioner partitioner;

    // synchronization barrier
    private final CyclicBarrier syncBarrier;

    // local sequential db for this thread
    private final SequentialDb localDb;

    /**
     * Constructs a WorkerThread with the specified parameters.
     * @param allFilesToRead list of all files to read from
     * @param threadId the ID of this thread
     * @param numThreads the total number of threads
     * @param barrier the cyclic barrier for synchronization
     */
    public WorkerThread(List<String> allFilesToRead, int threadId, int numThreads, CyclicBarrier barrier, SequentialDb localDb) {
        this.threadId = threadId;
        this.allFiles = allFilesToRead;
        this.partitioner = new WorkPartitioner(threadId, numThreads);
        this.syncBarrier = barrier;
        this.numThreads = numThreads;
        this.localDb = localDb;
    }

    /**
     * Checks if this is the master thread
     *
     * @return true if this is the master thread, false otherwise
     */
    private boolean isMasterThread() {
        return threadId == Constants.MASTER_THREAD;
    }

    /**
     * Executes the worker thread's pipeline of operations
     */
    @Override
    public void run() {
        WorkPipeline pipeline = buildPipeline();
        pipeline.execute();
    }

    /**
     * Builds the processing pipeline for this worker thread
     *
     * @return the configured work pipeline
     */
    private WorkPipeline buildPipeline() {
        WorkPipeline pipeline = new WorkPipeline();

        // read articles from files and sync
        pipeline.addStage(createReadStage());
        pipeline.addStage(createSyncStage());

        // master thread creates global deduplication, sync
        if (isMasterThread()) {
            pipeline.addStage(createDeduplicationStage());
        }
        pipeline.addStage(createSyncStage());

        // process articles
        pipeline.addStage(createProcessStage());
        pipeline.addStage(createSyncStage());

        // master thread creates global article list, sync
        if (isMasterThread()) {
            pipeline.addStage(createGlobalListStage());
        }
        pipeline.addStage(createSyncStage());

        // merges data and creates stats data, sync
        pipeline.addStage(createDataMergeStage());
        pipeline.addStage(createSyncStage());

        // finally write results, partial files and sync
        pipeline.addStage(createWriteStage());
        pipeline.addStage(createSyncStage());

        // merge all partial files and write report (master thread only)
        if (isMasterThread()) {
            pipeline.addStage(createMergeFilesStage());
            pipeline.addStage(createReportWriteStage());
        }

        return pipeline;
    }

    private WorkPipeline.PipelineStage createGlobalListStage() {
        return new WorkPipeline.ActionStage(() ->
            ConcurrentDb.getInstance().generateGlobalArticleList()
        );
    }

    private WorkPipeline.PipelineStage createDeduplicationStage() {
        return new WorkPipeline.ActionStage(() ->
            ConcurrentDb.getInstance().generateGlobalDedupMaps()
        );
    }

    /**
     * Creates the read stage for this thread's partition of files
     *
     * @return the read stage
     */
    private WorkPipeline.PipelineStage createReadStage() {
        List<String> filesToRead = partitioner.partitionList(allFiles);
        return new WorkPipeline.OperatorStage(new Reader(filesToRead, localDb));
    }

    /**
     * Creates a synchronization stage using the cyclic barrier
     *
     * @return the synchronization stage
     */
    private WorkPipeline.PipelineStage createSyncStage() {
        return new WorkPipeline.SynchronizationStage(syncBarrier);
    }

    /**
     * Creates the deduplication stage (master thread only)
     *
     * @return the deduplication stage
     */
//    private WorkPipeline.PipelineStage createDeduplicationStage() {
//        return new WorkPipeline.ActionStage(() ->
//            ConcurrentDb.getInstance().removeDuplicates()
//        );
//    }

    /**
     * Creates the article processing stage.
     *
     * @return the processing stage
     */
    private WorkPipeline.PipelineStage createProcessStage() {
        return new WorkPipeline.ActionStage(() -> {
            new Processor(localDb).execute();
        });
    }

    /**
     * Creates the data merging stage for this thread's partition of merge operations
     *
     * @return the data merge stage
     */
    private WorkPipeline.PipelineStage createDataMergeStage() {
        List<ConcurrentDb.MergeFunction> mfs = partitioner.partitionList(ConcurrentDb.getInstance().getMergeOperations());
        return new WorkPipeline.ActionStage(() -> {
            new DataMerger(mfs).execute();
        });
    }

    /**
     * Creates the write stage for outputting this thread's partition of results
     *
     * @return the write stage
     */
    private WorkPipeline.PipelineStage createWriteStage() {
        return new WorkPipeline.OperatorStage(new Writer(partitioner, threadId));
    }

    /**
     * Creates the file merging stage (master thread only)
     *
     * @return the file merge stage
     */
    private WorkPipeline.PipelineStage createMergeFilesStage() {
        return new WorkPipeline.OperatorStage(new FileMerger(numThreads));
    }

    /**
     * Creates the report writing stage (master thread only)
     *
     * @return the report write stage
     */
    private WorkPipeline.PipelineStage createReportWriteStage() {
        return new WorkPipeline.ActionStage(() -> {
            new ReportWriter().execute();
        });
    }


}
