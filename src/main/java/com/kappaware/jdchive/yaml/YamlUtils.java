package com.kappaware.jdchive.yaml;

import java.io.StringWriter;

import com.esotericsoftware.yamlbeans.YamlConfig;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.esotericsoftware.yamlbeans.YamlConfig.WriteClassName;

public class YamlUtils {

	public static YamlConfig yamlConfig = new YamlConfig();
	static {
		yamlConfig.setPropertyElementType(YamlDescription.class, "databases", YamlDatabase.class);
		yamlConfig.setPropertyElementType(YamlDescription.class, "tables", YamlTable.class);
		yamlConfig.setPropertyElementType(YamlTable.class, "fields", YamlField.class);
		yamlConfig.setPropertyElementType(YamlReport.class, "todo", YamlReport.Todo.class);
		yamlConfig.setPropertyElementType(YamlReport.class, "done", YamlReport.Done.class);
		yamlConfig.setScalarSerializer(YamlBool.class, new YamlBool.YamlBoolSerializer());
		yamlConfig.writeConfig.setWriteRootTags(false);
		yamlConfig.writeConfig.setWriteRootElementTags(false);
		yamlConfig.writeConfig.setWriteClassname(WriteClassName.NEVER);
		yamlConfig.writeConfig.setIndentSize(2);
	}


	public static String toYamlString(Object o) {
		StringWriter sw = new StringWriter();
		YamlWriter yamlWriter = new YamlWriter(sw, yamlConfig);
		try {
			yamlWriter.write(o);
			yamlWriter.close();
			sw.close();
		} catch (Exception e) {
			throw new RuntimeException("Exception in YAML generation", e);
		}
		return sw.toString();
	}

}
