import database.ConcurrentDb;
import multithreading.WorkerThread;
import database.DbInitializer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * Main entry point for the News Aggregator application.
 */
public class Tema1
{
    /**
     * Main method to start the news aggregation system.
     *
     * @param args command-line arguments:
     *             args[0] - number of worker threads to use
     *             args[1] - path to the file containing list of news files
     *             args[2] - path to the file containing auxiliary data files
     */
    public static void main(String[] args) {

        final int numThreads = Integer.parseInt(args[0]);
        final String newsFile =  args[1], additionalFile =  args[2];

        DbInitializer init = new DbInitializer();

        long startTime = System.currentTimeMillis(); // time start

        try {
            List<String> files = init.initDb(newsFile, additionalFile);
            ConcurrentDb.getInstance().initPartialDbs(numThreads);

            WorkerThread[] workers = new WorkerThread[numThreads];
            CyclicBarrier barrier = new CyclicBarrier(numThreads);

            for (int i = 0; i < numThreads; i++) {
                workers[i] = new WorkerThread(files, i, numThreads, barrier,ConcurrentDb.getInstance().getPartialDb(i) );
                workers[i].start();
            }

            for (WorkerThread worker : workers) {
                try  {
                    worker.join();
                } catch (InterruptedException e) {
                    System.err.println(e.getMessage());
                }
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Execution time with " + numThreads + " threads = " + (endTime - startTime) + " ms");

        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
