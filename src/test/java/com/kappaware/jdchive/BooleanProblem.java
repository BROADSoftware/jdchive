package com.kappaware.jdchive;

import java.io.FileNotFoundException;

import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.constructor.ConstructorException;


public class BooleanProblem {

	static public class BooleanContainer {
		private boolean yesno1;
		private Boolean yesno2;

		public boolean isYesno1() {
			return yesno1;
		}

		public void setYesno1(boolean yesno1) {
			this.yesno1 = yesno1;
		}

		public Boolean getYesno2() {
			return yesno2;
		}

		public void setYesno2(Boolean yesno2) {
			this.yesno2 = yesno2;
		}

	}

	@Test
	public void test1() throws FileNotFoundException, DescriptionException {

		Yaml yamld = new Yaml(new Constructor(BooleanContainer.class));
		String test1 = "yesno1: yes";
		String test2 = "yesno1: false";
		String test3 = "yesno1: no";
		String test4 = "yesno1: xxx";
		String test5 = "{yesno1: yes, yesno2: false}";
		String test6 = "{yesno1: yes, yesno2: xxx }";

		BooleanContainer container1 = (BooleanContainer) yamld.load(test1);
		Assert.assertTrue(container1.isYesno1());
		Assert.assertNull(container1.getYesno2());

		BooleanContainer container2 = (BooleanContainer) yamld.load(test2);
		Assert.assertFalse(container2.isYesno1());
		Assert.assertNull(container2.getYesno2());

		BooleanContainer container3 = (BooleanContainer) yamld.load(test3);
		Assert.assertFalse(container3.isYesno1());
		Assert.assertNull(container3.getYesno2());

		boolean caught4 = false;
		try {
			yamld.load(test4);
		} catch (ConstructorException e) {
			//System.out.println(e.getClass());
			caught4 = true;
		}
		Assert.assertTrue(caught4);

		BooleanContainer container5 = (BooleanContainer) yamld.load(test5);
		Assert.assertTrue(container5.isYesno1());
		Assert.assertFalse(container5.getYesno2());

		boolean caught6 = false;
		try {
			yamld.load(test6);
		} catch (ConstructorException e) {
			//System.out.println(e.getClass());
			caught6 = true;
		}
		Assert.assertFalse(caught6);		// I would like opposite behavior
		
	}

}
