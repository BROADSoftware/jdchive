package com.kappaware.jdchive.yaml;

import java.util.List;
import java.util.Vector;

public class YamlReport {
	public Done done;
	public Todo todo;
	
	public YamlReport() {
		this.done = new Done();
		this.todo = new Todo();
	}
	
	static public class Done {
		public List<String> commands;

		public Done() {
			this.commands = new Vector<String>();
		}
	}
	
	static public class Todo {
		public List<TableMigration> tableMigrations;
		
		public Todo() {
			this.tableMigrations = new Vector<TableMigration>();
		}
	}
	
	
	static public class TableMigration {
		public YamlTable existing;
		public YamlTable target;
		public YamlTable diff;
		
		public TableMigration(YamlTable existing, YamlTable target, YamlTable diff) {
			this.existing = existing;
			this.target = target;
			this.diff = diff;
		}
	}
	
	public String toYaml() {
		return YamlUtils.toYamlString(this);
	}
	
}
