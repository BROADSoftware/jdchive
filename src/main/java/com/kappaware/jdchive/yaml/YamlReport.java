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

import java.util.List;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kappaware.jdchive.DescriptionException;

public class YamlReport {
	public Done done;
	public Todo todo;
	
	public YamlReport() {
		this.done = new Done();
		this.todo = new Todo();
	}
	
	static public class Done {
		public List<String> commands;

		public Done() {
			this.commands = new Vector<String>();
		}
	}
	
	static public class Todo {
		public List<TableMigration> tableMigrations;
		public List<DatabaseMigration> databaseMigrations;
		
		public Todo() {
			this.tableMigrations = new Vector<TableMigration>();
			this.databaseMigrations = new Vector<DatabaseMigration>();
		}
	}
	
	
	static public class TableMigration {
		public String name;
		public String database;
		public YamlTable existing;
		public YamlTable target;
		public YamlTable diff;
		public Long existingFingerprint;
		public Long targetFingerprint;
		public Long diffFingerprint;
		
		public TableMigration(YamlTable existing, YamlTable target, YamlTable diff) throws JsonProcessingException, DescriptionException {
			this.name = existing.name;
			this.database = existing.database;
			this.existing = existing;
			this.target = target;
			this.diff = diff;
			this.existingFingerprint = existing.computeFingerprint();
			this.targetFingerprint = target.computeFingerprint();
			this.diffFingerprint = diff.computeFingerprint();
			this.existing.name = null;
			this.target.name = null;
			this.diff.name = null;
		}
	}
	
	
	static public class DatabaseMigration {
		public String name;
		public YamlDatabase existing;
		public YamlDatabase target;
		public YamlDatabase diff;
		public Long existingFingerprint;
		public Long targetFingerprint;
		public Long diffFingerprint;
		
		public DatabaseMigration(YamlDatabase existing, YamlDatabase target, YamlDatabase diff) throws JsonProcessingException {
			this.name = existing.name;
			this.existing = existing;
			this.target = target;
			this.diff = diff;
			this.existingFingerprint = existing.computeFingerprint();
			this.targetFingerprint = target.computeFingerprint();
			this.diffFingerprint = diff.computeFingerprint();
			this.existing.name = null;
			this.target.name = null;
			this.diff.name = null;
		}
	}
	
	public String toYaml() throws JsonProcessingException {
		return YamlUtils.yaml2String(this);
	}

	
}
