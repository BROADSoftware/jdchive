package com.kappaware.jdchive.yaml;

import java.util.List;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonProcessingException;

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
		public List<DatabaseMigration> databaseMigrations;
		
		public Todo() {
			this.tableMigrations = new Vector<TableMigration>();
			this.databaseMigrations = new Vector<DatabaseMigration>();
		}
	}
	
	
	static public class TableMigration {
		public YamlTable existing;
		public YamlTable target;
		public YamlTable diff;
		public Long existingFingerprint;
		public Long targetFingerprint;
		public Long diffFingerprint;
		
		public TableMigration(YamlTable existing, YamlTable target, YamlTable diff) throws JsonProcessingException {
			this.existing = existing;
			this.target = target;
			this.diff = diff;
			this.existingFingerprint = existing.computeFingerprint();
			this.targetFingerprint = target.computeFingerprint();
			this.diffFingerprint = diff.computeFingerprint();
		}
	}
	
	
	static public class DatabaseMigration {
		public YamlDatabase existing;
		public YamlDatabase target;
		public YamlDatabase diff;
		public Long existingFingerprint;
		public Long targetFingerprint;
		public Long diffFingerprint;
		
		public DatabaseMigration(YamlDatabase existing, YamlDatabase target, YamlDatabase diff) throws JsonProcessingException {
			this.existing = existing;
			this.target = target;
			this.diff = diff;
			this.existingFingerprint = existing.computeFingerprint();
			this.targetFingerprint = target.computeFingerprint();
			this.diffFingerprint = diff.computeFingerprint();
		}
	}
	
	public String toYaml() throws JsonProcessingException {
		return YamlUtils.yaml2String(this);
	}

	
}
