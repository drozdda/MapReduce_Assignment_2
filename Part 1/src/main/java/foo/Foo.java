package foo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Foo {
    private FreqTable freqTable;

    public static void main(String[] arg)  {
        // Start timer.
        Instant start = Instant.now();

        try {
            new Foo().doIt();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // End timer; display duration.
        Instant end = Instant.now();
        System.out.format("Duration (ms) = %d\n", Duration.between(start, end).getNano()); // Prints PT1M3.553S
    }
    private BufferedReader init() throws Exception {
        // TODO: Refactor absolute path.
        File file = new File("C:\\Users\\thoma\\Documents\\College\\Courses\\6. 19F\\PROG 34104 - Distributed\\Ass_02\\src\\main\\java\\foo\\data_elonmusk.csv");
        return new BufferedReader(new FileReader(file));
    }
    private void doIt() throws Exception {
        BufferedReader data = init();
        this.freqTable = new FreqTable();

        data.readLine(); // Discard header line.
        callMap(data);
    }
    private void callMap(BufferedReader data) throws Exception {
        String line = null;

        while ((line = data.readLine()) != null) {
            try {
                String tweet = split(line);
                map(tweet);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.freqTable.printTable();
    }
    private String split(String line) {
        String str = null;

        // Split at commas outside quotes.
        String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        // Remove leading & trailing quotation marks.
        return cleanup(tokens[1]);
    }
    private void map(String sentence) {
        String[] words = sentence.split(" ");

        for (String word: words) {
            String temp = word.trim();
            if (temp.isEmpty()) {break;}
            this.freqTable.addEntry(word.trim());
        }
    }
    private String cleanup(String word) {
        // Remove leading & trailing quotations.
        String temp = word.replaceAll("^\"(.*)\"$", "$1");
        // Remove all non-alphabetical character.
        return temp.replaceAll("[^a-zA-Z]", " ").toLowerCase();
    }
}

class FreqTable {
    private Map<String, Integer> freqTable;
    FreqTable() {
        freqTable = new ConcurrentHashMap<>();
    }
    void addEntry(final String word) {
        int count = 0;
        if (freqTable.containsKey(word)) {
            count = freqTable.get(word);
        }

        freqTable.put(word, count + 1);
    }
    void printTable() throws Exception {
        System.out.println("Printing all keys and values.");
        File outputFile = new File(System.getProperty("user.dir") + File.separator + "output_final.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        for (Map.Entry<String, Integer> entry : freqTable.entrySet()) {
            String key = entry.getKey().toString();
            Integer value = entry.getValue();
            System.out.format("Key(%s) => (%d)\n", key, value);
            writer.append(key + "," + value + "\n");
        }

        writer.close();
    }
}

