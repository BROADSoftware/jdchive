package com.kappaware.jdchive;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.kappaware.jdchive.yaml.YamlUtils;


public class TestYamlUtils {

	public enum OwnerType {
		user, group, role, USER, GROUP, ROLE
	}

	static public class Database {
		public String name;
		public Map<String, String> properties;
		public OwnerType owner_type;
		public Boolean droppable;
		public List<Table> tables;
		public Integer count;
	}
	
	static public class Table {
		public String name;
	}
	
	@Test
	public void test1() throws JsonParseException, JsonMappingException, IOException {
		  String f = this.getClass().getClassLoader().getResource("testYamlUtils1.yml").getFile();
		  File file = new File(f); 
		  
		  Database db = YamlUtils.parse(file, Database.class);
		  
		  String s = YamlUtils.yaml2String(db);
		  
		  System.out.println(s);
		
	}

	
}
