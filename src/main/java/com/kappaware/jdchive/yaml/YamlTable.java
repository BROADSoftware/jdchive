package com.kappaware.jdchive.yaml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

		public Boolean alterable;
		public Boolean droppable;

		public YamlState state;
		//public String owner;		

		// Note: Two possible notation:  '\t' or '\u0009'
		static public class Delimited {
			public String fields_terminated_by;
			public String fields_escaped_by;
			public String collection_item_terminated_by;
			public String map_keys_terminated_by;
			public String lines_terminated_by;
			public String null_defined_as;
		}
		
		public void polish(YamlState defaultState) throws DescriptionException {
			if (this.name == null) {
				throw new DescriptionException("Invalid description: Every table must have a 'name' attribute");
			}
			if (this.database == null) {
				throw new DescriptionException(String.format("Invalid description: Table '%s' is missing database attribute!", this.name));
			}
			if (this.external == null) {
				this.external = false;
			}
			if(this.droppable == null) {
				this.droppable = this.external;
			}
			if (this.fields == null) {
				this.fields = new Vector<YamlField>();
			}
			for (YamlField f : this.fields) {
				f.polish(this.name, defaultState);
			}
			if (this.properties == null) {
				this.properties = new HashMap<String, String>();
			}
			if ((this.input_format == null) != (this.output_format == null)) {
				throw new DescriptionException(String.format("Invalid description: Table '%s.%s': Both 'input_format' and 'output_format' must be defined together!", this.database, this.name));
			}
			if(this.input_format != null && this.stored_as != null) {
				throw new DescriptionException(String.format("Invalid description: Table '%s.%s': Both 'stored_as' and 'input/output_format' can't be defined together!", this.database, this.name));
			}
			if(this.delimited != null && this.serde != null) {
				throw new DescriptionException(String.format("Invalid description: Table '%s.%s': Both 'delimited' and 'serde' can't be defined together!", this.database, this.name));
			}
			if (this.serde_properties == null) {
				this.serde_properties = new HashMap<String, String>();
			}
			if (this.state == null) {
				this.state = defaultState;
			}
			if(this.alterable == null) {
				this.alterable = false;
			}
			if(this.partitions == null) {
				this.partitions = new Vector<YamlField>();
			}
		}
		
		public String toYaml() throws JsonProcessingException {
			return YamlUtils.yaml2String(this);
		}
		
		public Long computeFingerprint() throws JsonProcessingException {
			// Fingerprint must be database name independant
			String db = this.database;
			this.database = null;
			String yaml = this.toYaml();
			this.database = db;
			return Math.abs(hashcode(yaml));
		}
		
		static private long hashcode(String s) {
			long h = 0;
			for(int i = 0; i < s.length(); i++) {
				h = 31 * h + s.charAt(i);
			}
			return h;
		}
	}

