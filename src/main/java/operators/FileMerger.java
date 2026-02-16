package operators;

import auxs.Constants;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Class meant to be used by Master Thread to unify all auxiliary files created by the other threads
 */
public class FileMerger implements Operator {
    private final int numFiles;

    /**
     * Constructs the FileMerger
     *
     * @param numFiles number of files to concatenate (numFiles == numThreads)
     */
    public FileMerger(int numFiles) {
        this.numFiles = numFiles;
    }

    /**
     * Merges both types of files
     */
    @Override
    public void execute() {
        mergeAllFile(Constants.ALL_FILE);
        mergeAllFile(Constants.WORDS_FILE);
    }

    /**
     * Merges all partial files with the given filename into a single output file
     *
     * @param filename the base filename to merge
     */
    private void mergeAllFile(String filename) {

        // efficient file merger
        try (FileChannel outChannel = FileChannel.open(
                Path.of(filename),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            for (int i = 0; i < numFiles; i++) {
                String inputFileName = i + "_" + filename;
                mergeFileInto(inputFileName, outChannel);
            }

        } catch (IOException e) {
            System.err.println("Error creating merged file: " + e.getMessage());
        }

        deleteAuxFiles(filename);
    }

    /**
     * Merges a single input file into the output channel using efficient file transfer
     *
     * @param inputFileName the path to the input file to merge
     * @param outChannel the output channel to write merged content to
     */
    private void mergeFileInto(String inputFileName, FileChannel outChannel) {
        try (FileChannel inChannel = FileChannel.open(Path.of(inputFileName), StandardOpenOption.READ)) {
            long size = inChannel.size();
            long position = 0;

            while (position < size) {
                position += inChannel.transferTo(position, size - position, outChannel);
            }

        } catch (IOException e) {
            System.err.println("Error merging " + inputFileName + ": " + e.getMessage());
        }
    }

    /**
     * Deletes all temporary partial files after merging
     *
     * @param filename the base filename of the partial files to delete
     */
    private void deleteAuxFiles(String filename) {
        for (int i = 0; i < numFiles; i++) {
            String inputFileName = i + "_" + filename;

            try {
                boolean deleted = Files.deleteIfExists(Path.of(inputFileName));
                if (!deleted) {
                    System.out.println("[INFO] Aux file not found: " + inputFileName);
                }
            } catch (IOException e) {
                System.err.println("Error deleting aux file " + inputFileName + ": " + e.getMessage());
            }
        }
    }

}
