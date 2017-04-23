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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kappaware.jdchive.DescriptionException;

public class YamlTable {
	public String name;
	public String database;
	public Boolean external;
	public List<YamlField> fields;
	public String comment;
	public String location;
	public Map<String, String> properties;
	public String stored_as;
	public String input_format;
	public String output_format;
	public Delimited delimited;
	public String serde;
	public Map<String, String> serde_properties;
	public String storage_handler;
	public List<YamlField> partitions;
	public Bucketing clustered_by;
	public Skewing skewed_by;

	public Boolean alterable;
	public Boolean droppable;

	public YamlState state;
	//public String owner;		

	// Note: Two possible notation:  '\t' or '\u0009'
	static public class Delimited {
		public String fields_terminated_by;
		public String fields_escaped_by;
		public String collection_items_terminated_by;
		public String map_keys_terminated_by;
		public String lines_terminated_by;
		public String null_defined_as;
	}

	public enum Direction {
		ASC, asc, DESC, desc
	}

	static public class SortItem {
		public String column;
		public Direction direction;

		public void polish() {
			if (this.direction == null) {
				this.direction = Direction.ASC;
			}
		}

		@Override
		public boolean equals(Object other) {
			return column.equals(((SortItem) other).column) && direction.toString().toLowerCase().equals(((SortItem) other).direction.toString().toLowerCase());
		}
	}

	static public class Bucketing {
		public List<String> columns;
		public Integer nbr_buckets;
		public List<SortItem> sorted_by;

		public void polish(String tableName, boolean check) throws DescriptionException {
			if (this.columns == null) {
				this.columns = new Vector<String>();
			}
			if (this.sorted_by == null) {
				this.sorted_by = new Vector<SortItem>();
			}
			if (check && this.nbr_buckets == null) {
				throw new DescriptionException(String.format("Invalid description: Table '%s' is missing clustered_by.nbr_buckets attribute!", tableName));
			}
			for (SortItem si : this.sorted_by) {
				si.polish();
			}
		}

		@Override
		public boolean equals(Object other) {
			return this.columns.equals(((Bucketing) other).columns) && this.nbr_buckets.equals(((Bucketing) other).nbr_buckets) && this.sorted_by.equals(((Bucketing) other).sorted_by);
		}
	}

	static public class Skewing {
		public List<String> columns;
		public List<List<String>> values;
		public Boolean stored_as_directories;

		public void polish() {
			if (this.columns == null) {
				this.columns = new Vector<String>();
			}
			if (this.values == null) {
				this.values = new Vector<List<String>>();
			}
			if (this.stored_as_directories == null) {
				this.stored_as_directories = false;
			}
		}

		@Override
		public boolean equals(Object other) {
			return this.columns.equals(((Skewing) other).columns) && this.values.equals(((Skewing) other).values) && this.stored_as_directories.equals(((Skewing) other).stored_as_directories);
		}

	}

	public void polish(YamlState defaultState, boolean check) throws DescriptionException {
		if (check && this.name == null) {
			throw new DescriptionException("Invalid description: Every table must have a 'name' attribute");
		}
		if (check && this.database == null) {
			throw new DescriptionException(String.format("Invalid description: Table '%s' is missing database attribute!", this.name));
		}
		if (this.external == null) {
			this.external = false;
		}
		if (this.droppable == null) {
			this.droppable = this.external;
		}
		if (this.fields == null) {
			this.fields = new Vector<YamlField>();
		}
		for (YamlField f : this.fields) {
			f.polish(this.name, defaultState, check);
		}
		if (this.properties == null) {
			this.properties = new HashMap<String, String>();
		}
		if (check && ((this.input_format == null) != (this.output_format == null))) {
			throw new DescriptionException(String.format("Invalid description: Table '%s.%s': Both 'input_format' and 'output_format' must be defined together!", this.database, this.name));
		}
		if (check && (this.input_format != null && this.stored_as != null)) {
			throw new DescriptionException(String.format("Invalid description: Table '%s.%s': Both 'stored_as' and 'input/output_format' can't be defined together!", this.database, this.name));
		}
		if (check && (this.delimited != null && this.serde != null)) {
			throw new DescriptionException(String.format("Invalid description: Table '%s.%s': Both 'delimited' and 'serde' can't be defined together!", this.database, this.name));
		}
		if (this.serde_properties == null) {
			this.serde_properties = new HashMap<String, String>();
		}
		if (this.state == null) {
			this.state = defaultState;
		}
		if (this.alterable == null) {
			this.alterable = false;
		}
		if (this.partitions == null) {
			this.partitions = new Vector<YamlField>();
		}
		for (YamlField f : this.partitions) {
			f.polish(this.name, defaultState, check);
		}
		if (this.clustered_by != null) {
			this.clustered_by.polish(String.format("%s.%s", this.database, this.name), check);
		}
		if (this.skewed_by != null) {
			this.skewed_by.polish();
		}
	}

	public String toYaml() throws JsonProcessingException {
		return YamlUtils.yaml2String(this);
	}

	/* WARNING: This function modify the data 
	 */
	public Long computeFingerprint() throws JsonProcessingException, DescriptionException {
		this.polish(YamlState.present, false);
		// Fingerprint must be database independant
		this.database = null;
		// Try to cannonize the table definition
		this.name = this.name.toLowerCase();
		if (this.fields != null) {
			for (YamlField field : this.fields) {
				field.name = field.name.toLowerCase();
				field.type = field.type.toUpperCase();
			}
		}
		if (this.partitions != null) {
			for (YamlField field : this.partitions) {
				field.name = field.name.toLowerCase();
				field.type = field.type.toUpperCase();
			}
		}
		if(this.clustered_by != null) {
			for(int i = 0; i < this.clustered_by.columns.size(); i++) {
				this.clustered_by.columns.set(i, this.clustered_by.columns.get(i).toLowerCase());
			}
			for(SortItem si : this.clustered_by.sorted_by) {
				si.column = si.column.toLowerCase();
				if(si.direction == Direction.asc) {
					si.direction = Direction.ASC;
				}
				if(si.direction == Direction.desc) {
					si.direction = Direction.DESC;
				}
			}
		}
		if(this.skewed_by != null) {
			for(int i = 0; i < this.skewed_by.columns.size(); i++) {
				this.skewed_by.columns.set(i, this.skewed_by.columns.get(i).toLowerCase());
			}
		}
		String yaml = this.toYaml();
		return Math.abs(hashcode(yaml));
	}

	static private long hashcode(String s) {
		long h = 0;
		for (int i = 0; i < s.length(); i++) {
			h = 31 * h + s.charAt(i);
		}
		return h;
	}

	private Map<String, String> typeByColumn;

	public String getColumnType(String colName) {
		if (this.typeByColumn == null) {
			this.typeByColumn = new HashMap<String, String>();
			for (YamlField f : this.fields) {
				this.typeByColumn.put(f.name.trim().toUpperCase(), f.type.trim().toUpperCase());
			}
			for (YamlField f : this.partitions) {
				this.typeByColumn.put(f.name.trim().toUpperCase(), f.type.trim().toUpperCase());
			}
		}
		return this.typeByColumn.get(colName.trim().toUpperCase());
	}

	static private Set<String> quotedType = new HashSet<String>(Arrays.asList("STRING", "VARCHAR", "CHAR"));

	public boolean isValueNeedQuotes(String colName) {
		return quotedType.contains(this.getColumnType(colName));
	}
}
