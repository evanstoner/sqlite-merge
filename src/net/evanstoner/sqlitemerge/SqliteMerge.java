package net.evanstoner.sqlitemerge;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Author: Evan Stoner <evanstoner.net>
 * Date: 11/16/13
 */

public class SqliteMerge {

    static ArrayList<Table> tables = new ArrayList<Table>();

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("usage: SqliteMerge <target_db> <secondary_db> <config_file>");
            return;
        }

        // get files and create a copy of target to serve as the merged file
        File target = new File(args[0]);
        File secondary = new File(args[1]);
        File merged = new File("merged.db");

        if (merged.exists()) {
            merged.delete();
        }
        Files.copy(target.toPath(), merged.toPath());

        // read the config file and split it on periods followed by whitespace
        String config = new String(Files.readAllBytes(Paths.get(args[2])));
        config = config.replaceAll("\\#[\\S ]*", "");
        String[] configEntries = config.split("\\.\\s");
        for (String entry : configEntries) {
            tables.add(new Table(entry));
        }

        for (Table t : tables) {
            System.out.println(t);
        }
    }
}
