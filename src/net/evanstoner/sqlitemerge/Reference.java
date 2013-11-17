package net.evanstoner.sqlitemerge;

/**
 * Author: Evan Stoner <evanstoner.net>
 * Date: 11/16/13
 */

public class Reference {
    public String table = null;
    public String field = null;

    public Reference(String table, String field) {
        this.table = table;
        this.field = field;
    }

    public Reference(String reference) {
        String[] split = reference.split("\\.");

        if (split.length != 2) {
            System.err.println("WARNING: Bad reference: " + reference);
            return;
        }

        table = split[0];
        field = split[1];
    }

    public String toString() {
        return table + "." + field;
    }
}
