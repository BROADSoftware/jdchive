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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kappaware.jdchive.DescriptionException;

public class YamlDatabase {

	public enum OwnerType {
		USER, GROUP, ROLE
	}

	
	public String name;
	public Map<String, String> properties;
	public String location;
	public String owner;
	public OwnerType owner_type;
	public String comment;
	public YamlState state;

	public void polish(YamlState defaultState, boolean check) throws DescriptionException {
		if (check && this.name == null) {
			throw new DescriptionException("Invalid description: Every database must have a 'name' attribute");
		}
		if (check && "default".equals(this.name)) {
			throw new DescriptionException("Can't alter 'default' database");
		}
		if (check && this.location != null && !this.location.startsWith("/")) {
			throw new DescriptionException(String.format("Invalid description: Database '%s' location must be absolute", this.name));
		}
		if (this.properties == null) {
			this.properties = new HashMap<String, String>();
		}
		if (this.state == null) {
			this.state = defaultState;
		}
		if(check && owner != null && this.owner_type == null) {
			throw new DescriptionException(String.format("Invalid description for database '%s'. If an owner is defined, then owner_type (USER|GROUP|ROLE) must be also!", this.name));
		}
	}
	
	public String toYaml() throws JsonProcessingException {
		return YamlUtils.yaml2String(this);
	}

	public Long computeFingerprint() throws JsonProcessingException {
		String yaml = this.toYaml();
		this.name = this.name.toLowerCase();
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
