package net.evanstoner.sqlitemerge;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Author: Evan Stoner <evanstoner.net>
 * Date: 11/16/13
 */

public class SqliteMerge {

    static ArrayList<Table> tables = new ArrayList<Table>();
    static HashMap<String, HashMap<Integer, Integer>> keyMap = new HashMap<String, HashMap<Integer, Integer>>();
    static File targetFile;
    static File secondaryFile;
    static Connection targetConnection = null;
    static Connection secondaryConnection = null;

    public static void main(String[] args) throws IOException, SQLException {
        if (args.length != 3) {
            System.err.println("usage: SqliteMerge <primary_db> <secondary_db> <config_file>");
            return;
        }

        // get files and create a copy of primary to serve as the merged file
        File primaryFile = new File(args[0]);
        secondaryFile = new File(args[1]);
        targetFile = new File("merged.db");

        if (targetFile.exists()) {
            targetFile.delete();
        }
        Files.copy(primaryFile.toPath(), targetFile.toPath());

        // read the config file and split it on periods followed by whitespace
        String config = new String(Files.readAllBytes(Paths.get(args[2])));
        config = config.replaceAll("\\#[\\S ]*", "");
        String[] configEntries = config.split("\\.\\s");
        for (String entry : configEntries) {
            tables.add(new Table(entry));
        }

        for (Table t : tables) {
            //System.out.println(t);
            keyMap.put(t.name, new HashMap<Integer, Integer>());
        }

        try {
            openConnections();
        } catch (ClassNotFoundException e) {
            System.err.println("FATAL: Couldn't open databases: " + e.getMessage());
            return;
        }


        try {
            prepareForUpdate();
            doUpdate();
        } finally {
            closeConnections();
        }
    }

    public static void openConnections() throws IOException, ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        targetConnection = DriverManager.getConnection("jdbc:sqlite:" + targetFile.getAbsolutePath());
        secondaryConnection = DriverManager.getConnection("jdbc:sqlite:" + secondaryFile.getAbsolutePath());
    }

    public static void closeConnections() {
        try {
            if (targetConnection != null) {
                targetConnection.close();
            }
        } catch (SQLException e) {
            // do nothing
        }

        try {
            if (secondaryConnection != null) {
                secondaryConnection.close();
            }
        } catch (SQLException e) {
            // do nothing
        }
    }

    public static void refreshTargetConnection() throws SQLException{
        if (targetConnection != null) {
            targetConnection.close();
        }
        targetConnection = DriverManager.getConnection("jdbc:sqlite:" + targetFile.getAbsolutePath());
    }

    public static void prepareForUpdate() throws SQLException {
        System.out.println("== MATCHING RECORDS ==");
        Statement secondaryStatement = secondaryConnection.createStatement();
        Statement targetStatement = targetConnection.createStatement();

        for (Table t : tables) {
            System.out.println("\n" + t.name + ":");

            if (t.gidUpdates.size() + t.localUpdates.size() == 0) {
                System.out.println("Skipping this table (no updates required)");
                continue;
            }

            // the first field of the gid diffs for comparing age; null means "always update"
            String diffField = null;
            if (t.gidDiffs.size() > 0) {
                t.gidDiffs.get(0).getActualField();
            }

            /*
            Query for all fields
             */

            // the rowid is not selected with *
            String selectFields = t.skey == "rowid" ? t.name + "." + t.skey + ", *" : "*";
            selectFields = selectFields.replace("*", t.name + ".*"); // unambiguate
            SimpleQuery sqSecondaryRecords = new SimpleQuery("SELECT " + selectFields, "FROM " + t.name, "");
            for (Field gid : t.gids) {
                if (gid.foreignField != null) {
                    // we have to perform a join to look up the gid
                    Reference r = t.getReference(gid.localField);
                    sqSecondaryRecords.select += ", " + r.table + "." + gid.foreignField; // add the referenced field
//                    sqSecondaryRecords.select += ", " + r.toString(); // add the join field (for constraints)
                    sqSecondaryRecords.join += " INNER JOIN " + r.table + " USING (" + r.field + ")";
                }
            }

            System.out.println(sqSecondaryRecords.toString());
            ResultSet rsSecondaryRecords = secondaryStatement.executeQuery(sqSecondaryRecords.toString());

            /*
            For each record, look for a match in the target database
             */

            // improve performance by finding the next pseudokey outside the loop
            int newSkey = 1;
            ResultSet rsNewSkey = targetStatement.executeQuery("SELECT MAX(" + t.skey + ") FROM " + t.name);
            if (rsNewSkey.next()) {
                newSkey = rsNewSkey.getInt(1) + 1;
            }

            while (rsSecondaryRecords.next()) {
                SimpleQuery sqTargetMatch = new SimpleQuery("SELECT ", "FROM " + t.name, "WHERE");

                // we need the pseudokey for mapping
                sqTargetMatch.select += t.name + "." + t.skey;

                // we need any fields that the dependents rely on so that we can find them
                for (Reference dependent : t.dependents) {
                    // ensure we don't "double select" the same field
                    if (!sqTargetMatch.select.contains(dependent.field)) {
                        sqTargetMatch.select += ", " + t.name + "." + dependent.field;
                    }
                }
                // we need the first diff field (a date) for comparing age if a match is found
                if (diffField != null) {
                    sqTargetMatch.select += ", " + diffField;
                }

                // we'll have to join on the same tables to compare the gids
                sqTargetMatch.join = sqSecondaryRecords.join;

                // store the where values for binding later
                String[] gidValues = new String[t.gids.size()];
                for (int i = 0; i < t.gids.size(); i++) {
                    Field gid = t.gids.get(i);
                    if (!sqTargetMatch.where.equals("WHERE")) {
                        sqTargetMatch.where += " AND";
                    }
                    sqTargetMatch.where += " " + gid.getActualField() + "=?";
                    gidValues[i] = rsSecondaryRecords.getString(gid.getActualField());
                    System.out.println(gid.getActualField() + " = " + gidValues[i]);
                }

                // prepare the statement and bind the parameters
                PreparedStatement stmtTargetMatch = targetConnection.prepareStatement(sqTargetMatch.toString());
                for (int i = 0; i < gidValues.length; i++) {
                    stmtTargetMatch.setString(i + 1, gidValues[i]);
                }
                System.out.println(sqTargetMatch);
                ResultSet rsTargetMatch = stmtTargetMatch.executeQuery();

                /*
                If we find a match, map to its pseudokey and if it's outdated, delete its dependents. If no
                match is found, insert a new record and map that pseudokey.
                 */

                if (rsTargetMatch.next()) {
                    System.out.println("Found match: " + rsSecondaryRecords.getInt(t.skey) + " -> " + rsTargetMatch.getInt(t.skey));
                    keyMap.get(t.name).put(rsSecondaryRecords.getInt(t.skey), rsTargetMatch.getInt(t.skey));

                    if (diffField == null || rsSecondaryRecords.getDate(diffField).after(rsTargetMatch.getDate(diffField))) {
                        for (Reference dependent : t.dependents) {
                            SimpleQuery sqDeleteDependent = new SimpleQuery("DELETE", "FROM " + dependent.table, "WHERE " + dependent.field + "=?");
                            PreparedStatement stmtDeleteDependent = targetConnection.prepareStatement(sqDeleteDependent.toString());
                            stmtDeleteDependent.setString(1, rsTargetMatch.getString(dependent.field));
                            stmtDeleteDependent.executeUpdate();
                            System.out.println(".. Deleted dependents in " + dependent.table + " on " + dependent.field);
                        }
                    }
                } else {
                    System.out.println("No match: " + rsSecondaryRecords.getInt(t.skey));

                    ArrayList<String> values = new ArrayList<String>();
                    SimpleInsert siInsertRecord = new SimpleInsert("INSERT INTO " + t.name, "(" + t.skey, "VALUES (?");

                    // add the gids, since they define the record
                    for (Field gid : t.gids) {
                        // we can only insert local fields
                        if (gid.foreignField == null) {
                            siInsertRecord.fields += ", " + gid.localField;
                            siInsertRecord.values += ", ?";
                            values.add(rsSecondaryRecords.getString(gid.localField));
                        }
                    }

                    // add the foreign keys
                    ArrayList<String> fields = new ArrayList<String>();
                    fields.addAll(t.gidUpdates.keySet());
                    fields.addAll(t.localUpdates.keySet());
                    for (String field : fields) {
                        Reference r = t.getReference(field);
                        if (r != null) {
                            siInsertRecord.fields += ", " + field;
                            siInsertRecord.values += ", ?";
                            values.add(rsSecondaryRecords.getString(field));
                        }
                    }

                    for (String s : values) {
                        System.out.println("V: " + s);
                    }

                    siInsertRecord.fields += ")";
                    siInsertRecord.values += ")";

                    PreparedStatement stmtInsertRecord = targetConnection.prepareStatement(siInsertRecord.toString(), PreparedStatement.RETURN_GENERATED_KEYS);
                    // the new pseudokey is the first param
                    stmtInsertRecord.setInt(1, newSkey++);
                    // the gids makes up the rest of the params
                    for (int i = 0; i < values.size(); i++) {
                        stmtInsertRecord.setString(i+2, values.get(i));
                    }

                    System.out.println(siInsertRecord);
                    stmtInsertRecord.executeUpdate();
                    ResultSet rsInsertRecordKey = stmtInsertRecord.getGeneratedKeys();
                    rsInsertRecordKey.next();
                    keyMap.get(t.name).put(rsSecondaryRecords.getInt(t.skey), rsInsertRecordKey.getInt(1));
                    System.out.println(".. Created record " + rsInsertRecordKey.getInt(1));
                }
            }
        }
    }

    public static void doUpdate() {

    }

    private static class SimpleQuery {
        public String select = "";
        public String from = "";
        public String join = "";
        public String where = "";
        public String having = "";

        public SimpleQuery(String select, String from, String where) {
            this.select = select;
            this.from = from;
            this.where = where;
        }

        public String toString() {
            return select + " " + from + " " + join + " " + where + " " + having;
        }
    }

    private static class SimpleInsert {
        public String insert = "";
        public String fields = "";
        public String values = "";

        public SimpleInsert(String insert, String fields, String values) {
            this.insert = insert;
            this.fields = fields;
            this.values = values;
        }

        public String toString() {
            return insert + " " + fields + " " + values;
        }
    }
}
