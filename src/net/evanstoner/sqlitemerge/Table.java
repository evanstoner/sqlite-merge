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

        // name
        String[] headerSplit = headerAndContents[0].split("\\(");
        name = headerSplit[0].trim();

        // dependents
        if (headerSplit.length > 1) {
            headerSplit[1] = headerSplit[1].replace(")", "");
            String[] dependentsArray = headerSplit[1].split(",");
            for (int i = 0; i < dependentsArray.length; i++) {
                dependents.add(new Reference(dependentsArray[i].trim()));
            }
        }

        String[] lines = headerAndContents[1].split(";");

        if (lines.length < 6) {
            System.err.println("WARNING: Bad number of lines: " + lines.length);
            return;
        }

        // pseudokey; ROW is assumed if none is specified
        skey = lines[0].trim();
        if (skey.length() == 0) {
            skey = "ROW";
        }

        // global identifiers
        if (lines[1].trim().length() > 0) {
            String[] gidsArray = lines[1].split(",");
            for (int i = 0; i < gidsArray.length; i++) {
                gids.add(new Field(gidsArray[i].trim()));
            }
        }

        // diffs 1
        diffs1 = listOfFields(lines[2]);

        // updates 1
        updates1 = mapOfRefernces(lines[3]);

        // diffs 2
        diffs2 = listOfFields(lines[4]);

        // updates 2
        updates2 = mapOfRefernces(lines[5]);
    }

    private ArrayList<Field> listOfFields(String line) {
        ArrayList<Field> fields = new ArrayList<Field>();
        String[] split = line.split(",");

        if (split[0].trim().length() == 0) {
            return fields;
        }

        for (int i = 0; i < split.length; i++) {
            fields.add(new Field(split[i].trim()));
        }

        return fields;
    }

    private ArrayList<Reference> listOfReferences(String line) {
        ArrayList<Reference> references = new ArrayList<Reference>();

        return references;
    }

    private HashMap<String, Reference> mapOfRefernces(String line) {
        HashMap<String, Reference> references = new HashMap<String, Reference>();

        String[] split = line.split(",");

        if (split[0].trim().length() == 0) {
            return references;
        }

        for (int i = 0; i < split.length; i++) {
            if (split[i].contains("(")) {
                // there is a reference
                split[i] = split[i].replace(")", "");
                String[] fieldAndReference = split[i].split("\\(");
                references.put(fieldAndReference[0].trim(), new Reference(fieldAndReference[1].trim()));
            } else {
                references.put(split[i].trim(), null);
            }
        }

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
            s += " - " + k;
            if (updates1.get(k) != null) {
                s += " (" + updates1.get(k) + ")";
            }
            s += "\n";
        }

        s += "Updates 2 (diff'd on";
        for (Field f : diffs2) {
            s += " - " + f;
        }
        s += "):\n";
        for (String k : updates2.keySet()) {
            s += " - " + k;
            if (updates2.get(k) != null) {
                s += " (" + updates2.get(k) + ")";
            }
            s += "\n";
        }

        return s;
    }
}
