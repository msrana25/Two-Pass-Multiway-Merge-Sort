import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

public class Phase2 {
    private static final int MEMORY_SIZE = 51;
    private static final int BLOCK_SIZE=40;
    public static int total_write_blocks_phase2 =0;
    public static int total_read_blocks_phase2 =0;
    public static long output_records =0;
    private static File folder = new File("sublists");

    public static int run() throws Exception {
        List<String> sortedSublists = new ArrayList<>();
        File sublistsDir = new File("sublists");
        File[] sublists = sublistsDir.listFiles();
        String outputFilename="";

        // Collect all the sorted sublists in the sublists directory
        for (File sublist : sublists) {
            if (sublist.getName().startsWith("sublist_")) {
                sortedSublists.add(sublist.getName());
            }
        }

        int numMerges = 0;
        int numIOs = 0;
        int run = 1;
        while (sortedSublists.size() > 1) {
            List<String> newSortedSublists = new ArrayList<>();
            // Merge MEMORY_SIZE - 1 sublists at a time
            for (int i = 0; i < sortedSublists.size(); i += MEMORY_SIZE - 1) {
                int j = Math.min(i + MEMORY_SIZE - 1, sortedSublists.size());
                String[] sublistArray = sortedSublists.subList(i, j).toArray(new String[0]);
                     
                outputFilename = "sublist_" + UUID.randomUUID().toString() + "_" + (numMerges + 1) + ".txt";
                int merges = mergeSublists(sublistArray, outputFilename, run);
                numIOs += merges;
                newSortedSublists.add(outputFilename);
                numMerges++;
                
            }
            sortedSublists = newSortedSublists;
            folder = new File("mergedSublists_" + run);
            run++;
            

        }
        output_records= Files.lines(Paths.get("mergedSublists_"+(run-1)+"/"+outputFilename)).count();
        return numIOs;
    }

    public static int mergeSublists(String[] sublists, String outputFilename, int run) throws IOException {
        new File("mergedSublists_" + run).mkdirs();
        int numIOs = 0;
        int write_count =0;

        PriorityQueue<BagEntry> pq = new PriorityQueue<>();
        BufferedReader[] readers = new BufferedReader[sublists.length];
        BufferedWriter writer = new BufferedWriter(new FileWriter("mergedSublists_" + run + "/" + outputFilename));
        try {
            // Open all sublist files for reading
            for (int i = 0; i < sublists.length; i++) {
                readers[i] = new BufferedReader(new FileReader(folder.getName() + "/" + sublists[i]), 4*1024);
                  }

            // Load the first block of each sublist into the buffer
            @SuppressWarnings("unchecked")
			List<String>[] buffers = new ArrayList[sublists.length];
            for (int i = 0; i < sublists.length; i++) {
              //main memory blocks
            	buffers[i] = new ArrayList<>();
                for (int j = 0; j < BLOCK_SIZE; j++) {
                    String line = readers[i].readLine();
                    if (line != null) {
                        buffers[i].add(line);
                    }
                    
                }numIOs++; total_read_blocks_phase2++;
                
                
                
                if (!buffers[i].isEmpty()) {
                    pq.add(new BagEntry(buffers[i].get(0), i));
                }

            }


            // Merge the sublists by repeatedly removing the minimum element from the priority queue
            while (!pq.isEmpty()) {
                BagEntry minEntry = pq.poll();
                String minValue = minEntry.value;
                int sublistIndex = minEntry.sublistIndex;

                // Write the minimum element to the output file
                writer.write(minValue);
                writer.newLine();
                write_count++;
                if (write_count == BLOCK_SIZE)
                {	
                numIOs++;
                write_count =0;
                total_write_blocks_phase2++;
                }

                // Remove the minimum element from the buffer for the current sublist
                buffers[sublistIndex].remove(0);

                // If the buffer is empty, load the next block from the sublist
                if (buffers[sublistIndex].isEmpty()) {
                    for (int j = 0; j < BLOCK_SIZE; j++) {
                        String line = readers[sublistIndex].readLine();
                        if (line != null) {
                            buffers[sublistIndex].add(line);
                        }

                    }numIOs++; total_read_blocks_phase2++;
                }

                // Add the next element from the current sublist to the priority queue
                if (!buffers[sublistIndex].isEmpty()) {
                    pq.add(new BagEntry(buffers[sublistIndex].get(0), sublistIndex));
                }
            }

            if (write_count!=0) 
            	{
            	numIOs++;
            	total_write_blocks_phase2++;

            	}
            
        } finally {
            // Close all open files
            for (BufferedReader reader : readers) {
                if (reader != null) {
                    reader.close();
                }
            }
            writer.close();
        }

        return numIOs;
    }

	public static class BagEntry implements Comparable<BagEntry> {
	    public String value;
	    public int sublistIndex;

	    public BagEntry(String value, int sublistIndex) {
	        this.value = value;
	        this.sublistIndex = sublistIndex;
	    }

	    @Override
	    public int compareTo(BagEntry other) {
	        return value.compareTo(other.value);
	    }
	}
	
    }

