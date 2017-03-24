package com.kappaware.jdchive;

import java.io.FileNotFoundException;
import java.io.FileReader;

import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlReader.YamlReaderException;


public class BooleanTest {
	
	
	
	@Test
	public void test1() throws FileNotFoundException, YamlException, DescriptionException {
		String file = this.getClass().getClassLoader().getResource("test1.yml").getFile();
		
		YamlReader yamlReader = new YamlReader(new FileReader(file), Description.getYamlConfig());
		Description description = yamlReader.read(Description.class);
		description.polish(Description.State.absent);
		
		Assert.assertEquals(description.tables.get(0).name, "test1");
		Assert.assertFalse(description.tables.get(0).external.booleanValue());
		
		Assert.assertEquals(description.tables.get(1).name, "test2");
		Assert.assertTrue(description.tables.get(1).external.booleanValue());
		
		Assert.assertEquals(description.tables.get(2).name, "test3");
		Assert.assertTrue(description.tables.get(2).external.booleanValue());
				
	}

	
	@Test
	public void test2() throws FileNotFoundException, YamlException, DescriptionException {
		String file = this.getClass().getClassLoader().getResource("test2.yml").getFile();
		
		YamlReader yamlReader = new YamlReader(new FileReader(file), Description.getYamlConfig());
		try {
			Description description = yamlReader.read(Description.class);		description.polish(Description.State.absent);
		} catch(YamlReaderException e) {
			// Fine
			return;
		}
		Assert.fail("A YamlReaderException should have been generated");
	}


}
