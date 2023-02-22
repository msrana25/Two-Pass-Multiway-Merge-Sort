public class Main {
    public static void main(String[] args) {
        // Constants
        final double AVERAGE_SEEK_TIME = 6.46;
        final double AVERAGE_ROTATIONAL_DELAY = 4.17;
        final double BLOCK_TRANSFER_TIME = 0.13;
        final double AVERAGE_TIME = AVERAGE_SEEK_TIME+AVERAGE_ROTATIONAL_DELAY + BLOCK_TRANSFER_TIME;

        // Phase 1
        try {
            System.out.println("--------------------------------------------- Phase 1 -------------------------------------------------");
            long startTime_p1 = System.currentTimeMillis();
            int disk_read_writes_r1 = Phase1.sort("r1_large.txt", true, "r1");
            int disk_read_writes_r2 = Phase1.sort("r2_large.txt", false, "r2");
            int total_read_writes_p1 = disk_read_writes_r1 + disk_read_writes_r2;
            System.out.println("Total # of disk I/o's during phase1: " + total_read_writes_p1);
            System.out.println("Number of output blocks written to disk for Phase1: " + Phase1.total_write_blocks);
            System.out.println("Total # of output records: " + Phase1.output_records);

            long endTime_p1 = System.currentTimeMillis();
            long duration_p1 = endTime_p1 - startTime_p1;
            System.out.println("Elapsed time (using System class): " + duration_p1 + " milliseconds.");
            System.out.println("Elapsed time (assuming Megatron 747 disk metrices and random R/W): " + (int) 2*(Phase1.total_write_blocks * (AVERAGE_TIME)) + " milliseconds.");

            // Phase 2
            System.out.println("--------------------------------------------- Phase 2 -------------------------------------------------");
            long startTime_p2 = System.currentTimeMillis();
            int total_read_writes_p2 = Phase2.run();
            System.out.println("Total # of disk I/o's during phase2: " + (total_read_writes_p2 - 1));
            System.out.println("Number of output blocks written to disk for Phase2: " + Phase2.total_write_blocks_phase2);
            System.out.println("Total # of output records: " + Phase2.output_records);
            long endTime_p2 = System.currentTimeMillis();
            long duration_p2 = endTime_p2 - startTime_p2;
            System.out.println("Elapsed time(using System class): " + duration_p2 + " milliseconds.");
            System.out.println("Elapsed time (assuming Megatron 747 disk metrices and random R/W): " + (int) ((Phase2.total_write_blocks_phase2+Phase2.total_read_blocks_phase2) * (AVERAGE_TIME)) + " milliseconds.");

            
            //Summary
            System.out.println("---------------------------------------Phase1 & Phase2 summary -----------------------------------------");
            System.out.println("Total # of output blocks written on disk during both the phases: " + (Phase1.total_write_blocks+Phase2.total_write_blocks_phase2));
            System.out.println("Total time together taken to produce the output: "+ (duration_p1+duration_p2) +" milliseconds(Using System class).");
            System.out.println("Total time together taken to produce the output: "+ AVERAGE_TIME*((2*Phase1.total_write_blocks)+((Phase2.total_write_blocks_phase2)+Phase2.total_read_blocks_phase2))  +" milliseconds(Using assuming Megatron 747 disk metrices and random R/W).");

            
            
            
            

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
