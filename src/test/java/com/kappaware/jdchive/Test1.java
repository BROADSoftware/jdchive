package com.kappaware.jdchive;

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


public class Test1 {

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
		String db1yaml = "{ name: db1, properties: {prp1: val1, prp2: val2}, owner_type: \"GROUP\", droppable: yes, tables: [{ name: tbl1 } ]}";
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Database db1 = mapper.readValue(db1yaml, Database.class);
        //System.out.println(ReflectionToStringBuilder.toString(db1,ToStringStyle.MULTI_LINE_STYLE));
        //System.out.println(ReflectionToStringBuilder.toString(db1.tables.get(0),ToStringStyle.MULTI_LINE_STYLE));
        Assert.assertEquals("db1", db1.name);
        Assert.assertEquals(OwnerType.class, db1.owner_type.getClass());
        Assert.assertNull(db1.count);
	}

	// Test we got an error on invalid boolean
	@Test
	public void test2() throws JsonParseException, JsonMappingException, IOException {
		String db1yaml = "{ name: db1, properties: {prp1: val1, prp2: val2}, owner_type: GROUP, droppable: xxx, tables: [{ name: tbl1 } ]}";
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        boolean caught = false;
        try {
        	mapper.readValue(db1yaml, Database.class);
        } catch(InvalidFormatException e) {
        	caught = true;
        }
        Assert.assertTrue(caught);
	}


	// Test we got an error on invalid Integer
	@Test
	public void test3() throws JsonParseException, JsonMappingException, IOException {
		String db1yaml = "{ name: db1, properties: {prp1: val1, prp2: val2}, owner_type: GROUP, tables: [{ name: tbl1 } ], count: xxx}";
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        boolean caught = false;
        try {
        	mapper.readValue(db1yaml, Database.class);
        } catch(InvalidFormatException e) {
        	caught = true;
        }
        Assert.assertTrue(caught);
	}

	

	@Test
	public void test4() throws JsonParseException, JsonMappingException, IOException {
		String db1yaml = "{ name: db1, properties: {prp1: val1, prp2: val2}, owner_type: GROUP, droppable: yes, tables: [{ name: tbl1 } ]}";
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Database db1 = mapper.readValue(db1yaml, Database.class);

        String yaml = mapper.setSerializationInclusion(Include.NON_NULL).writerWithDefaultPrettyPrinter().writeValueAsString(db1);
        System.out.println(yaml);
	}


	@Test
	public void test5() throws JsonParseException, JsonMappingException, IOException {
		String db1yaml = "{ name: db1, properties: {prp1: val1, prp2: val2}, owner_type: GROUP, droppable: yes, tables: [ ]}";
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Database db1 = mapper.readValue(db1yaml, Database.class);

        String yaml = mapper.setSerializationInclusion(Include.NON_NULL).writerWithDefaultPrettyPrinter().writeValueAsString(db1);
        System.out.println(yaml);
	}

	@Test
	public void test6() throws JsonParseException, JsonMappingException, IOException {
		String db1yaml = "{ name: db1 }";
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Database db1 = mapper.setSerializationInclusion(Include.NON_NULL).readValue(db1yaml, Database.class);
        String yaml = mapper.setSerializationInclusion(Include.NON_NULL).writerWithDefaultPrettyPrinter().writeValueAsString(db1);
        System.out.println(yaml);
	}
	
	
}
