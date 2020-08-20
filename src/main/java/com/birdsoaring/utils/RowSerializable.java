package com.birdsoaring.utils;


import java.io.Serializable;


/**
 * @ClassName RowSerializable
 * @Description TODO
 * @Author chezhao
 * @Date 2020/4/15 11:30
 * @Version 1.0
 **/
public class RowSerializable implements Serializable {
    private static final long serialVersionUID = 1L;
    private Object[] fields;

    /**
     * Creates an instance of RowSerializable
     *
     * @param arity Size of the row
     */
    public RowSerializable(int arity) {
        this.fields = new Object[arity];
    }

    /**
     * returns number of fields contained in a Row
     *
     * @return int arity
     */
    public int productArity() {
        return this.fields.length;
    }

    /**
     * Inserts the "field" Object in the position "i".
     *
     * @param i     index value
     * @param field Object to write
     */
    public void setField(int i, Object field) {
        this.fields[i] = field;
    }

    /**
     * returns the Object contained in the position "i" from the RowSerializable.
     *
     * @param i index value
     * @return Object
     */
    public Object productElement(int i) {
        return this.fields[i];
    }


    /**
     * returns a String element with the fields of the RowSerializable
     *
     * @return String
     */
    @Override
    public String toString() {
        String str = fields[0].toString();
        for (int i = 1; i < fields.length; i++) {
            str = str + ", " + fields[i].toString();
        }
        return str;
    }

    @Override
    public boolean equals(Object object) {
        return false;
    }
}
