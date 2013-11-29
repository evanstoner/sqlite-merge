package net.evanstoner.sqlitemerge;

/**
 * Author: Evan Stoner <evanstoner.net>
 * Date: 11/16/13
 */

public class Field {
    public String localField = null;
    public String foreignField = null;

    /**
     * Creates a new Field by parsing a text representation
     * @param field The field in the form "localfield" or "localfield->foreignField"
     */
    public Field(String field) {
        String[] split = field.split("->");
        localField = split[0];
        if (split.length > 1) {
            foreignField = split[1];
        }
    }

    public String toString() {
        if (foreignField != null) {
            return localField + "->" + foreignField;
        } else {
            return localField;
        }
    }

    /**
     * Gets the name of the actual field, e.g. for querying
     * @return The local field if there is no foreign field; otherwise, the foreign field
     */
    public String getActualField() {
        if (foreignField == null) {
            return localField;
        } else {
            return foreignField;
        }
    }
}
