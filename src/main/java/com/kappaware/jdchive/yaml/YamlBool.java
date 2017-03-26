package com.kappaware.jdchive.yaml;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.scalar.ScalarSerializer;

public class YamlBool {
	public boolean value;

	public YamlBool(boolean b) {
		this.value = b;
	}

	public boolean booleanValue() {
		return value;
	}
	
	@Override
	public boolean equals(Object other) {
		return this.booleanValue() == ((YamlBool)other).booleanValue();
		
	}

	static class YamlBoolSerializer implements ScalarSerializer<YamlBool> {

		@Override
		public YamlBool read(String value) throws YamlException {
			if ("yes".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)) {
				return new YamlBool(true);
			} else if ("no".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
				return new YamlBool(false);
			} else {
				throw new YamlException("String value must be 'yes', 'true', 'no' or 'false', not" + value);
			}
		}

		@Override
		public String write(YamlBool value) throws YamlException {
			return value.booleanValue() ? "true" : "false";
		}
	}
}
