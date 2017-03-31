package com.kappaware.jdchive.yaml;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kappaware.jdchive.DescriptionException;

public class YamlDatabase {

	public enum OwnerType {
		user, group, role, USER, GROUP, ROLE
	}

	
	public String name;
	public Map<String, String> properties;
	public String location;
	public String owner_name;
	public OwnerType owner_type;
	public String comment;
	public YamlState state;

	public void polish(YamlState defaultState) throws DescriptionException {
		if (this.name == null) {
			throw new DescriptionException("Invalid description: Every database must have a 'name' attribute");
		}
		if ("default".equals(this.name)) {
			throw new DescriptionException("Can't alter 'default' database");
		}
		if (this.location == null) {
			throw new DescriptionException(String.format("Invalid description: Database '%s' is missing 'location' attribute", this.name));
		}
		if (!this.location.startsWith("/")) {
			throw new DescriptionException(String.format("Invalid description: Database '%s' location must be absolute", this.name));
		}
		if (this.properties == null) {
			this.properties = new HashMap<String, String>();
		}
		if (this.state == null) {
			this.state = defaultState;
		}
	}
	
	public String toYaml() throws JsonProcessingException {
		return YamlUtils.yaml2String(this);
	}
}
