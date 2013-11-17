package net.evanstoner.sqlitemerge;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Author: Evan Stoner <evanstoner.net>
 * Date: 11/16/13
 */

public class Table {
    // name of the table
    public String name = null;

    // dependent tables
    public ArrayList<Reference> dependents = new ArrayList<Reference>();

    // pseudokey ("sudokey")
    public String skey = null;

    // global identifiers
    public ArrayList<Field> gids = new ArrayList<Field>();

    // first list of differencing fields
    public ArrayList<Field> diffs1 = new ArrayList<Field>();

    // first list of update fields
    public HashMap<String, Reference> updates1 = new HashMap<String, Reference>();

    // second list of differencing fields
    public ArrayList<Field> diffs2 = new ArrayList<Field>();

    // second list of update fields
    public HashMap<String, Reference> updates2 = new HashMap<String, Reference>();

    // instance fields
    public ArrayList<String> instanceFields = new ArrayList<String>();

    public Table(String configEntry) {
        String[] headerAndContents = configEntry.split("\\:");

        if (headerAndContents.length != 2) {
            System.err.println("WARNING: Bad table: " + configEntry);
            return;
        }

        String header = headerAndContents[0];
        name = header.trim();

        String[] lines = headerAndContents[1].split(";");

        if (lines.length < 6) {
            System.err.println("WARNING: Bad number of lines: " + lines.length);
            return;
        }
    }

    private ArrayList<Field> listOfAliases(String line) {
        ArrayList<Field> aliases = new ArrayList<Field>();

        return aliases;
    }

    private ArrayList<Reference> listOfReferences(String line) {
        ArrayList<Reference> references = new ArrayList<Reference>();

        return references;
    }

    private HashMap<String, Reference> mapOfRefernces(String line) {
        HashMap<String, Reference> references = new HashMap<String, Reference>();

        return references;
    }

    public String toString() {
        String s = name + "\n";
        for (int i = 0; i < name.length(); i++) {
            s += "=";
        }
        s += "\n";

        s += "Pseudokey: " + skey + "\n";

        s += "Global identifiers:\n";
        for (Field f : gids) {
            s += " - " + f + "\n";
        }

        s += "Dependents:\n";
        for (Reference r : dependents) {
            s += " - " + r + "\n";
        }

        s += "Updates 1 (diff'd on";
        for (Field f : diffs1) {
            s += " - " + f;
        }
        s += "):\n";
        for (String k : updates1.keySet()) {
            s += " - " + updates1.get(k) + "\n";
        }

        s += "Updates 2 (diff'd on";
        for (Field f : diffs2) {
            s += " - " + f;
        }
        s += "):\n";
        for (String k : updates2.keySet()) {
            s += " - " + updates1.get(k) + "\n";
        }

        return s;
    }
}
