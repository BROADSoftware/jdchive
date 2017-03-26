package com.kappaware.jdchive.yaml;

import com.kappaware.jdchive.DescriptionException;

public class YamlField {
	public String name;
	public String type;
	public String comment;

	void polish(String tableName, YamlState defaultState) throws DescriptionException {
		if (this.name == null) {
			throw new DescriptionException(String.format("Invalid description: Table '%s'. Missing 'name' attribute in some field(s)!", tableName));
		}
		if (this.type == null) {
			throw new DescriptionException(String.format("Invalid description: Table '%s'. Missing 'type' attribute for field '%s'!", tableName, this.name));
		}
	}

	public boolean almostEquals(Object other) {
		return this.name.equalsIgnoreCase(((YamlField) other).name) && this.type.equalsIgnoreCase(((YamlField) other).type);
	}

	@Override
	public boolean equals(Object other) {
		return this.name.equalsIgnoreCase(((YamlField) other).name) && this.type.equalsIgnoreCase(((YamlField) other).type) && this.comment.equals(((YamlField) other).comment);
	}
}
