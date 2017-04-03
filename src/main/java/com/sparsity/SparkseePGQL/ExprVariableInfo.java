package com.sparsity.SparkseePGQL;

import java.util.HashSet;
import java.util.Set;

/**
 */
public class ExprVariableInfo {
    private boolean labelChecked; // si es consulta la label de la variable o no (implica buscar-ne el tipus)
    private final Set<String> attributes; // quins atributs consulta de la variable (implica obtenir l'atribut)

    public ExprVariableInfo() {
        labelChecked = false;
        attributes = new HashSet<String>();
    }

    public void addAttribute(String name) {
        attributes.add(name);
    }

    public void setLabelChecked() {
        labelChecked = true;
    }

    public boolean isLabelChecked() {
        return labelChecked;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "ExprVariableInfo{" +
                "labelChecked=" + labelChecked +
                ", attributes=" + attributes +
                '}';
    }
}
