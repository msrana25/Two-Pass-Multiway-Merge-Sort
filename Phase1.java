import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Phase1 {
	
	  // Define the number of blocks available in memory
    private static final int MEMORY_SIZE = 51;
	public static int fileNumberIndex = 0;
	public static int total_write_blocks=0;
	public static long output_records=0;

    // Define the size of each sublist on disk
    private static final int SUBLIST_SIZE = 40;
    
    
    public static int sort(String inputFile, boolean refreshDirectory, String source) throws IOException {
        int numIOs = 0;
        int out_records=0;

        File directory = new File("sublists");
        if (refreshDirectory) {
            new File("sublists").mkdirs();

            for (File file : Objects.requireNonNull(directory.listFiles())) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
        }

        // Open the input file and read it in chunks of SUBLIST_SIZE tuples
        List<String[]> sublists = new ArrayList<>();
        String[][] memory = new String[MEMORY_SIZE][SUBLIST_SIZE];
        int memoryIndex = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                memory[memoryIndex][i++] = line;
                if (i == SUBLIST_SIZE) {
                    sublists.add(memory[memoryIndex]);
                    memory[memoryIndex] = new String[SUBLIST_SIZE];
                    i = 0;
                    memoryIndex = (memoryIndex + 1) % MEMORY_SIZE;
                    numIOs++;

                }
            }
            if (i > 0) {
                sublists.add(Arrays.copyOf(memory[memoryIndex], i));
                numIOs++;
            }
        }

        // Sort the sublists in memory and write them to temporary files on disk
        List<String> sortedSublists = new ArrayList<>();
        total_write_blocks += sublists.size();
        for (int i = 0; i < sublists.size(); i++) {
            String[] subarray = sublists.get(i);
            Arrays.sort(subarray);
            String filename = "sublists/" + "sublist_"+ source + "_" + fileNumberIndex + ".txt";
            sortedSublists.add(filename);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                for (int j = 0; j < subarray.length; j++) {
                    writer.write(subarray[j] + "\n");
                    output_records++;
                }
                numIOs++;
            }
            fileNumberIndex++;
        }

        return numIOs;
    }
}
