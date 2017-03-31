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
		public String owner;		// TODO: Remove ?
		public String comment;
		public String location;
		public Map<String, String> properties;
		public Boolean droppable;

		
		
		public String file_format;
		public String storage_handler;
		public String input_format;
		public String output_format;
		public String serde;
		public Map<String, String> serde_properties;
		public YamlState state;

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
				throw new DescriptionException(String.format("Invalid description: Table '%s.%s': Both 'inputFormat' and 'outputFormat' must be defined together!", this.database, this.name));
			}
			if (this.serde_properties == null) {
				this.serde_properties = new HashMap<String, String>();
			}
			if (this.state == null) {
				this.state = defaultState;
			}
		}
		
		public String toYaml() throws JsonProcessingException {
			return YamlUtils.yaml2String(this);
		}
		
		
	}

