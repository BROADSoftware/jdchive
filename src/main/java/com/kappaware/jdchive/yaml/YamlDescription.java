/*
 * Copyright (C) 2016 BROADSoftware
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

import java.util.List;

import com.kappaware.jdchive.DescriptionException;

public class YamlDescription {

	public List<YamlDatabase> databases;
	public List<YamlTable> tables;

	public void polish(YamlState defaultState) throws DescriptionException {
		if (databases != null) {
			for (YamlDatabase db : this.databases) {
				db.polish(defaultState);
			}
		}
		if (tables != null) {
			for (YamlTable tbl : this.tables) {
				tbl.polish(defaultState);
			}
		}
	}

}
