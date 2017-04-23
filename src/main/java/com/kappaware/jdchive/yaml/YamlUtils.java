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

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;


/**
 * Several yaml lib was tried, before landing on jackson stuff.
 * 
 * YamlBeans was first choice, as simple. But
 * - Does not handle boolean parsing properly ( external: xxxx was understood as false, or null)
 * - Need help to detect inner object type.
 * - Direct inner objects can't be beans.
 * 
 *  Then try SnakeYaml.
 *  - Generation was fine.
 *  - Boolean parsing was also incorrect.
 *  - API is complex and not well documented.
 *  
 *  Then, try jackson, which, although it use SnakeYaml under the hood is for for what we need:
 *  - Correct boolean handling ( external: xxxx generate an error).
 *  - Clean generation.
 *  NB: Note: If we need more control on Yaml generation, we can use SnakeYaml for this, as it is embedded as a dependency.
 * 
 * @author sa
 *
 */

public class YamlUtils {

	private static ObjectMapper mapper;
	static {
		mapper =  new ObjectMapper(new YAMLFactory());
		mapper = mapper.setSerializationInclusion(Include.NON_NULL);
	}
	
	public static <T> T parse(File f, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        //ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(f, clazz);
	}

	public static String yaml2String(Object o) throws JsonProcessingException {
		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o);
	}


	public static void writeYaml(Writer w, Object value) throws IOException {
		mapper.writerWithDefaultPrettyPrinter().writeValue(w, value);
	}

}
