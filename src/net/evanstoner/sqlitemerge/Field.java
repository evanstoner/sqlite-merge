package net.evanstoner.sqlitemerge;

/**
 * Author: Evan Stoner <evanstoner.net>
 * Date: 11/16/13
 */

public class Field {
    public String localField = null;
    public String foreignField = null;

    public String toString() {
        if (foreignField != null) {
            return localField + "->" + foreignField;
        } else {
            return localField;
        }
    }
}
