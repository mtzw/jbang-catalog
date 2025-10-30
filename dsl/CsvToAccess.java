///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS net.sf.ucanaccess:ucanaccess:5.0.1
//DEPS org.apache.commons:commons-csv:1.10.0
//DEPS info.picocli:picocli:4.7.5

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Converts CSV files in a directory to an Access DB.
 */
@Command(name = "CsvToAccess", mixinStandardHelpOptions = true, version = "1.0",
        description = "Converts CSV files in a directory to an Access DB.")
class CsvToAccess implements Callable<Integer> {

    @Parameters(index = "0", description = "Directory containing CSV files.")
    private Path csvDir;

    @Parameters(index = "1", description = "Output Access DB file path.")
    private String dbPath;

    @Option(names = {"-e", "--encoding"}, description = "CSV file encoding (default: ${DEFAULT-VALUE}).", defaultValue = "Shift_JIS")
    private String encoding;

    public static void main(String... args) {
        int exitCode = new CommandLine(new CsvToAccess()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws IOException, SQLException {
        System.out.println("Starting CSV to Access DB conversion...");
        System.out.println("CSV Directory: " + csvDir.toAbsolutePath());
        System.out.println("Output DB: " + dbPath);
        System.out.println("CSV Encoding: " + encoding);

        // Create a new Access DB if it doesn't exist
        String dbUrl = "jdbc:ucanaccess://" + dbPath + ";newDatabaseVersion=V2010";
        System.out.println("Connecting to DB with URL: " + dbUrl);

        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            System.out.println("Successfully connected to Access DB: " + dbPath);

            List<Path> csvFiles;
            try (Stream<Path> stream = Files.walk(csvDir)) {
                csvFiles = stream
                        .filter(file -> !Files.isDirectory(file))
                        .filter(file -> file.toString().toLowerCase().endsWith(".csv"))
                        .collect(Collectors.toList());
            }

            if (csvFiles.isEmpty()) {
                System.out.println("No CSV files found in " + csvDir.toAbsolutePath());
                return 0;
            }
            System.out.println("Found " + csvFiles.size() + " CSV file(s).");

            for (Path csvFile : csvFiles) {
                // Use filename (without extension) as table name
                String tableName = csvFile.getFileName().toString().replaceFirst("[.][^.]+$", "");
                System.out.println("\nProcessing " + csvFile.getFileName() + " -> table '" + tableName + "'");

                try (
                    Reader reader = new InputStreamReader(new FileInputStream(csvFile.toFile()), encoding);
                    CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
                ) {
                    List<String> headers = csvParser.getHeaderNames();
                    createTable(conn, tableName, headers);
                    insertData(conn, tableName, headers, csvParser.getRecords());
                } catch (Exception e) {
                    System.err.println("Error processing file " + csvFile.getFileName() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Conversion process finished.");
        return 0;
    }

    private static void createTable(Connection conn, String tableName, List<String> headers) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Drop table if it exists
            stmt.execute("DROP TABLE " + tableName);
        } catch (SQLException e) {
            // Ignore error if table doesn't exist
        }

        String columnsDef = headers.stream()
                .map(header -> "`" + header + "` TEXT") // Use TEXT for simplicity, quote headers
                .collect(Collectors.joining(", "));
        String createTableSql = "CREATE TABLE `" + tableName + "` (" + columnsDef + ")";

        System.out.println("Executing: " + createTableSql);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql);
        }
    }

    private static void insertData(Connection conn, String tableName, List<String> headers, List<CSVRecord> records) throws SQLException {
        String columns = headers.stream().map(h -> "`" + h + "`").collect(Collectors.joining(", "));
        String placeholders = headers.stream().map(h -> "?").collect(Collectors.joining(", "));
        String insertSql = "INSERT INTO `" + tableName + "` (" + columns + ") VALUES (" + placeholders + ")";

        System.out.println("Preparing to insert " + records.size() + " records...");
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            for (CSVRecord record : records) {
                for (int i = 0; i < headers.size(); i++) {
                    pstmt.setString(i + 1, record.get(i));
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            System.out.println("Successfully inserted " + records.size() + " records.");
        }
    }
}
