package com.sparsity.SparkseePGQL;

/**
 */
public class VariableInfo {
    private final String name;
    private final Boolean anonymous; // Anonymous variables can't be in more than one connection
    private final Boolean node;
    private int currentColumnIndex;


    public VariableInfo(String name, Boolean anonymous, Boolean node) {
        this.name = name;
        this.anonymous = anonymous;
        this.node = node;
        this.currentColumnIndex = -1;
    }

    public int getCurrentColumnIndex() {
        return currentColumnIndex;
    }

    public void setCurrentColumnIndex(int currentColumnIndex) {
        this.currentColumnIndex = currentColumnIndex;
    }

    public String getName() {
        return name;
    }

    public Boolean isAnonymous() {
        return anonymous;
    }

    public Boolean isNode() {
        return node;
    }

    public Boolean isEdge() {
        return !node;
    }

    @Override
    public String toString() {
        return "VariableInfo{" +
                "name='" + name + '\'' +
                ", anonymous=" + anonymous +
                ", node=" + node +
                ", currentColumnIndex=" + currentColumnIndex +
                '}';
    }
}
