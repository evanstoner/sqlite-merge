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
    static HashMap<String, HashMap<String, String>> keyMap = new HashMap<String, HashMap<String, String>>();
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
            Table t = new Table(entry);
            if (t.name == null) {
                return;
            }
            tables.add(new Table(entry));
        }

        for (Table t : tables) {
            //System.out.println(t);
            keyMap.put(t.name, new HashMap<String, String>());
        }

        try {
            openConnections();
        } catch (ClassNotFoundException e) {
            System.err.println("FATAL: Couldn't open databases: " + e.getMessage());
            return;
        }


        try {
            mergeDatabases();
            System.out.println("\nAll done, with no errors!");
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

    /**
     * Merge the target and secondary databases.
     * @throws SQLException
     */
    public static void mergeDatabases() throws SQLException {
        Statement targetStatement = targetConnection.createStatement();

        for (Table t : tables) {
            System.out.println("\n" + t.name + ":");

            if (t.gidUpdates.size() + t.localUpdates.size() == 0) {
                System.out.println("Skipping this table (no updates required)");
                continue;
            }

            /*
            For each record, look for a match in the target database. If one is found, update it. Otherwise, insert a new record.
             */

            ResultSet rsSecondaryRecords = querySecondaryTable(t);

            int newSkey = 1;
            ResultSet rsNewSkey = targetStatement.executeQuery("SELECT MAX(" + t.skey + ") FROM " + t.name);
            if (rsNewSkey.next()) {
                newSkey = rsNewSkey.getInt(1) + 1;
            }

            while (rsSecondaryRecords.next()) {
                String matchedKey = findMatch(rsSecondaryRecords, t);
                if (matchedKey == null) {
                    System.out.println("No match: " + rsSecondaryRecords.getInt(t.skey));
                    insertRecord(rsSecondaryRecords, newSkey++, t);
                } else {
                    System.out.println("Found match: " + rsSecondaryRecords.getInt(t.skey) + " -> " + matchedKey);
                    updateRecord(rsSecondaryRecords, matchedKey, t);
                }
            }
        }
    }

    /**
     * Get all the fields in the secondary database, plus the foreign fields.
     * @param t The table being processed.
     * @return The records from the secondary database.
     * @throws SQLException
     */
    public static ResultSet querySecondaryTable(Table t) throws SQLException {
        Statement secondaryStatement = secondaryConnection.createStatement();

        // the rowid is not selected with *
        String selectFields = t.skey == "rowid" ? t.name + "." + t.skey + ", *" : "*";
        selectFields = selectFields.replace("*", t.name + ".*"); // unambiguate
        SimpleQuery sqSecondaryRecords = new SimpleQuery("SELECT " + selectFields, "FROM " + t.name, "");

        // add all of the foreign gid fields
        for (Field gid : t.gids) {
            if (gid.foreignField != null) {
                // we have to perform a join to look up the gid
                Reference r = t.getReference(gid.localField);
                sqSecondaryRecords.select += ", " + r.table + "." + gid.foreignField; // add the referenced field
                sqSecondaryRecords.join += " INNER JOIN " + r.table + " USING (" + r.field + ")";
            }
        }

        // add all of the gid diffs
        for (Field gidDiff : t.gidDiffs) {
            if (gidDiff.foreignField != null) {
                Reference r = t.getReference(gidDiff.localField);
                sqSecondaryRecords.select += ", " + r.table + "." + gidDiff.foreignField; // add the referenced field
            }
        }

        return secondaryStatement.executeQuery(sqSecondaryRecords.toString());
    }

    /**
     * Find the key of a record that matches the current record.
     * @param secondaryRecords Records from the secondary database.
     * @param t The table being processed.
     * @return The ID of a matched record if one is found. Otherwise, null.
     * @throws SQLException
     */
    public static String findMatch(ResultSet secondaryRecords, Table t) throws SQLException {
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

        // we need the first GID diff field (a date) for comparing age if a match is found
        for (Field gidDiff : t.gidDiffs) {
            sqTargetMatch.select += ", " + gidDiff.getActualField();
        }

        // store the where values for binding later
        String[] gidValues = new String[t.gids.size()];
        for (int i = 0; i < t.gids.size(); i++) {
            Field gid = t.gids.get(i);
            if (!sqTargetMatch.where.equals("WHERE")) {
                sqTargetMatch.where += " AND";
            }
            sqTargetMatch.where += " " + gid.getActualField() + "=?";
            gidValues[i] = secondaryRecords.getString(gid.getActualField());
            if (gid.foreignField != null) {
                // we have to perform a join to look up the gid
                Reference r = t.getReference(gid.localField);
                sqTargetMatch.select += ", " + r.table + "." + gid.foreignField; // add the referenced field
                sqTargetMatch.join += " INNER JOIN " + r.table + " USING (" + r.field + ")";
            }
        }

        // prepare the statement and bind the parameters
        PreparedStatement stmtTargetMatch = targetConnection.prepareStatement(sqTargetMatch.toString());
        for (int i = 0; i < gidValues.length; i++) {
            stmtTargetMatch.setString(i + 1, gidValues[i]);
        }
        ResultSet rsTargetMatch = stmtTargetMatch.executeQuery();

        if (rsTargetMatch.next()) {
            return rsTargetMatch.getString(t.skey);
        }
        return null;
    }

    /**
     * Update a record in the target database with the secondary values.
     * @param secondaryRecords Records from the secondary database.
     * @param matchedKey The ID of a matched record.
     * @param t The table being processed.
     * @throws SQLException
     */
    public static void updateRecord(ResultSet secondaryRecords, String matchedKey, Table t) throws SQLException {
        keyMap.get(t.name).put(secondaryRecords.getString(t.skey), matchedKey);

        // get all the fields for the match and joined tables, used for updating the record if it's old
        SimpleQuery sqMatchDetails = new SimpleQuery("SELECT *", "FROM " + t.name, "WHERE " + t.skey + "=?");

        for (int i = 0; i < t.gids.size(); i++) {
            Field gid = t.gids.get(i);
            if (gid.foreignField != null) {
                // we have to perform a join to look up the gid
                Reference r = t.getReference(gid.localField);
                sqMatchDetails.join += " INNER JOIN " + r.table + " USING (" + r.field + ")";
            }
        }

        PreparedStatement stmtMatchDetails = targetConnection.prepareStatement(sqMatchDetails.toString());
        stmtMatchDetails.setString(1, matchedKey);
        ResultSet rsMatchDetails = stmtMatchDetails.executeQuery();
        rsMatchDetails.next();

        // try to find the diff date fields, using null if there is none, or if the table doesn't contain it
        String gidDiffDate = null;
        if (t.gidDiffs.size() > 0) {
            gidDiffDate = t.gidDiffs.get(0).getActualField();
        }
        Date secondaryGidDate = null;
        Date matchGidDate = null;
        if (gidDiffDate != null) {
            try {
                secondaryGidDate = secondaryRecords.getDate(gidDiffDate);
                matchGidDate = rsMatchDetails.getDate(gidDiffDate);
            } catch (SQLException e) {
                // the table does not have this column
                secondaryGidDate = null;
                matchGidDate = null;
            }
        }

        // update when the date of the secondary record is newer, or when there is nothing to compare
        if (matchGidDate == null || secondaryGidDate.after(matchGidDate)) {
            // delete the dependents
            for (Reference dependent : t.dependents) {
                SimpleQuery sqDeleteDependent = new SimpleQuery("DELETE", "FROM " + dependent.table, "WHERE " + dependent.field + "=?");
                PreparedStatement stmtDeleteDependent = targetConnection.prepareStatement(sqDeleteDependent.toString());
                stmtDeleteDependent.setString(1, rsMatchDetails.getString(dependent.field));
                stmtDeleteDependent.executeUpdate();
                System.out.println(".. Deleted dependents in " + dependent.table + " on " + dependent.field);
            }

            String gidDiffSignature = null;
            if (t.gidDiffs.size() > 1) {
                gidDiffSignature = t.gidDiffs.get(1).getActualField();
            }
            if (updateFields(secondaryRecords, rsMatchDetails, gidDiffSignature, t.gidUpdates.keySet(), matchedKey, t)) {
                System.out.println(".. Updated GID fields");
            }
        }

        // perform the same updates on the local fields
        String localDiffDate = null;
        if (t.gidDiffs.size() > 0) {
            localDiffDate = t.gidDiffs.get(0).getActualField();
        }
        if (updateFields(secondaryRecords, rsMatchDetails, localDiffDate, t.localUpdates.keySet(), matchedKey, t)) {
            System.out.println(".. Updated local fields");
        }
    }

    /**
     * Update the fields of the matched record using the secondary record if it is newer.
     * @param secondaryRecords Records from the secondary database.
     * @param matchDetails The matching record.
     * @param diffField The field to use for differencing. Will be compared lexicographically.
     * @param fields The fields to update if the record is old.
     * @param matchedKey The ID of the matched record.
     * @param t The table being processed.
     * @return True if the fields were updated. Otherwise, false.
     * @throws SQLException
     */
    public static boolean updateFields(ResultSet secondaryRecords, ResultSet matchDetails, String diffField, Set<String> fields, String matchedKey, Table t) throws SQLException {
        // try to find the diff fields, using null if there is none, or if the table doesn't contain it
        String secondaryDiff = null;
        String matchDiff = null;
        if (diffField != null) {
            try {
                secondaryDiff = secondaryRecords.getString(diffField);
                matchDiff = matchDetails.getString(diffField);
            } catch (SQLException e) {
                // the table does not have this column
                secondaryDiff = null;
                matchDiff = null;
            }
        }

        // update when the date of the secondary record is newer, or when there is nothing to compare
        if (matchDiff == null || secondaryDiff.compareTo(matchDiff) > 0) {
            SimpleUpdate suUpdateGidFields = new SimpleUpdate("UPDATE " + t.name, "SET", "WHERE " + t.skey + "=?");
            ArrayList<String> values = new ArrayList<String>();
            for (String field : fields) {
                Reference r = t.getReference(field);
                if (r == null) {
                    if (suUpdateGidFields.set != "SET") {
                        suUpdateGidFields.set += ",";
                    }
                    suUpdateGidFields.set += " " + field + "=?";
                    values.add(matchDetails.getString(field));
                }
            }
            // only execute the update if we found some fields to update
            if (suUpdateGidFields.set != "SET") {
                PreparedStatement stmtUpdateGidFields = targetConnection.prepareStatement(suUpdateGidFields.toString());
                for (int i = 0; i < values.size(); i++) {
                    stmtUpdateGidFields.setString(i+1, values.get(i));
                }
                stmtUpdateGidFields.setString(values.size(), matchedKey);
                stmtUpdateGidFields.executeUpdate();
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param secondaryRecords Records from the secondary database.
     * @param newSkey The new pseudokey. Probably the next sequential integer.
     * @param t The table being processed.
     * @throws SQLException
     */
    public static void insertRecord(ResultSet secondaryRecords, int newSkey, Table t) throws SQLException {
        ArrayList<String> values = new ArrayList<String>();
        SimpleInsert siInsertRecord = new SimpleInsert("INSERT INTO " + t.name, "(" + t.skey, "VALUES (?");

        // add the gids, since they define the record
        for (Field gid : t.gids) {
            // we can only insert local fields
            if (gid.foreignField == null) {
                siInsertRecord.fields += ", " + gid.localField;
                siInsertRecord.values += ", ?";
                values.add(secondaryRecords.getString(gid.localField));
            }
        }

        // add the foreign keys, but map them using the new values if they're available
        ArrayList<String> fields = new ArrayList<String>();
        fields.addAll(t.gidUpdates.keySet());
        fields.addAll(t.localUpdates.keySet());
        for (String field : fields) {
            Reference r = t.getReference(field);
            siInsertRecord.fields += ", " + field;
            siInsertRecord.values += ", ?";
            if (r == null) {
                values.add(secondaryRecords.getString(field));
            } else {
                String originalKey = secondaryRecords.getString(field);
                String mappedKey = null;
                if (keyMap.get(r.table) != null) {
                    mappedKey = keyMap.get(r.table).get(originalKey);
                }
                values.add(mappedKey == null ? originalKey : mappedKey);
            }
        }

        siInsertRecord.fields += ")";
        siInsertRecord.values += ")";

        PreparedStatement stmtInsertRecord = targetConnection.prepareStatement(siInsertRecord.toString(), PreparedStatement.RETURN_GENERATED_KEYS);
        // the new pseudokey is the first param
        stmtInsertRecord.setInt(1, newSkey);
        // the gids makes up the rest of the params
        for (int i = 0; i < values.size(); i++) {
            stmtInsertRecord.setString(i+2, values.get(i));
        }

        stmtInsertRecord.executeUpdate();
        ResultSet rsInsertRecordKey = stmtInsertRecord.getGeneratedKeys();
        rsInsertRecordKey.next();
        keyMap.get(t.name).put(secondaryRecords.getString(t.skey), rsInsertRecordKey.getString(1));
        System.out.println(".. Created record " + rsInsertRecordKey.getInt(1));
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

    private static class SimpleUpdate {
        public String update = "";
        public String set = "";
        public String values = "";

        public SimpleUpdate(String update, String set, String values) {
            this.update = update;
            this.set = set;
            this.values = values;
        }

        public String toString() {
            return update + " " + set + " " + values;
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
