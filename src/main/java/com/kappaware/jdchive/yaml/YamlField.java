/*
 * Copyright (C) 2017 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.jdchive.yaml;

import com.kappaware.jdchive.DescriptionException;

public class YamlField {
	public String name;
	public String type;
	public String comment;

	void polish(String tableName, YamlState defaultState, boolean check) throws DescriptionException {
		if (check && this.name == null) {
			throw new DescriptionException(String.format("Invalid description: Table '%s'. Missing 'name' attribute in some field(s)!", tableName));
		}
		if (check && this.type == null) {
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
