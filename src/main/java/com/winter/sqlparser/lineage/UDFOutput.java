package com.winter.sqlparser.lineage;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

public class UDFOutput {
    private List<TableRelation> relations;
    private List<List<String>> lineages;

    public UDFOutput(List<TableRelation> relations, List<List<String>> lineages) {
        this.relations = relations;
        this.lineages = lineages;
    }

    @Override
    public String toString() {
        return "UDFOutput{" +
                "relations=" + relations +
                ", lineages=" + lineages +
                '}';
    }

    public List<TableRelation> getRelations() {
        if (CollectionUtils.isEmpty(relations)) {
            return null;
        }
        return relations;
    }

    public List<List<String>> getLineages() {
        if (CollectionUtils.isEmpty(lineages)) {
            return null;
        }
        return lineages;
    }
}
