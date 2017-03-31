package com.kappaware.jdchive;

import java.util.List;
import java.util.Vector;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kappaware.jdchive.yaml.YamlField;
import com.kappaware.jdchive.yaml.YamlReport;
import com.kappaware.jdchive.yaml.YamlState;
import com.kappaware.jdchive.yaml.YamlTable;

public class TableEngine extends BaseEngine {
	static Logger log = LoggerFactory.getLogger(TableEngine.class);


	public TableEngine(Driver driver, YamlReport report) throws HiveException {
		super(driver, report);
	}


	int run(List<YamlTable> tables) throws TException, DescriptionException, HiveException, CommandNeedRetryException, JsonProcessingException {
		int nbrModif = 0;
		for (YamlTable dTable : tables) {
			Table table = this.hive.getTable(dTable.database, dTable.name, false);
			if (table == null) {
				if (dTable.state == YamlState.present) {
					this.createTable(dTable);
					nbrModif++;
				} // Else nothing to do
			} else {
				if (dTable.state == YamlState.absent) {
					this.dropTable(dTable);
					nbrModif++;
				} else {
					if (this.updateTable(table, dTable)) {
						nbrModif++;
					}
				}

			}
		}
		return nbrModif;
	}

	/**
	 * Perform update operation on a table if possible. If not, then add a migration in the 'todo' part of the report.
	 * - 
	 * @param table	The table description, as provided by Hive
	 * @param target The target table, as in the description file
	 * @return True if an operation was performed. Adding a migration to the report will NOT set this.
	 * @throws CommandNeedRetryException
	 * @throws JsonProcessingException 
	 */
	private boolean updateTable(Table table, YamlTable target) throws CommandNeedRetryException, JsonProcessingException {
		log.info(String.format("Found existing table: %s", table.toString()));
		List<String> changes = new Vector<String>();

		int migration = 0;
		YamlTable original = new YamlTable();
		original.name = table.getTableName();
		original.database = table.getDbName();
		original.external = new Boolean(table.getTableType() == TableType.EXTERNAL_TABLE);
		original.owner = table.getOwner();
		original.comment = table.getParameters().get("comment");
		original.location = table.getDataLocation().toString();
		original.properties = table.getParameters();
		original.fields = new Vector<YamlField>();
		for (FieldSchema field : table.getCols()) {
			YamlField f = new YamlField();
			f.name = field.getName();
			f.type = field.getType();
			f.comment = field.getComment();
			original.fields.add(f);
		}

		target.location = this.normalizePath(target.location);
		YamlTable diffTable = new YamlTable();
		diffTable.name = table.getTableName();
		diffTable.database = table.getDbName();

		if (!original.external.equals(target.external)) {
			diffTable.external = target.external;
			migration++;
		}
		int commonFieldCount = Math.min(original.fields.size(), target.fields.size());
		// A loop to adjust comment on field, provided this is the only change (Same name, type and position)
		int changedFields = 0;
		for (int i = 0; i < commonFieldCount; i++) {
			if (original.fields.get(i).almostEquals(target.fields.get(i))) {
				if (!Utils.isEqual(original.fields.get(i).comment, target.fields.get(i).comment)) {
					original.fields.get(i).comment = target.fields.get(i).comment;
					changes.add(String.format("ALTER TABLE %s.%s CHANGE COLUMN %s %s %s COMMENT '%s'", table.getDbName(), table.getTableName(), original.fields.get(i).name, original.fields.get(i).name, original.fields.get(i).type, original.fields.get(i).comment));
				}
			} else {
				changedFields++;
			}
		}
		// If there is some differences in fields set, then need migration.
		if (changedFields > 0 || original.fields.size() != target.fields.size()) {
			diffTable.fields = target.fields;
			migration++;
		}
		if (!Utils.isEqual(original.comment, target.comment)) {
			original.comment = target.comment;
			if (target.comment == null) {
				changes.add(String.format("ALTER TABLE %s.%s UNSET TBLPROPERTIES ('comment')", table.getDbName(), table.getTableName()));
			} else {
				changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('comment' = '%s')", table.getDbName(), table.getTableName(), target.comment));
			}
		}
		if (target.location != null) {
			if (!target.location.equals(original.location)) {
				diffTable.location = target.location;
				migration++;
			}
		}
		for (String keyp : target.properties.keySet()) {
			if (!original.properties.containsKey(keyp) || !original.properties.get(keyp).equals(target.properties.get(keyp))) {
				original.properties.put(keyp, target.properties.get(keyp));
				changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('%s' = '%s')", table.getDbName(), table.getTableName(), keyp, target.properties.get(keyp)));
			}
		}

		if (changes.size() > 0 && (migration == 0 || !target.droppable.booleanValue())) {
			this.performCmds(changes);
		}
		if (migration > 0) {
			if (target.droppable.booleanValue()) {
				this.dropTable(target);
				this.createTable(target);
				return true;
			} else {
				YamlReport.TableMigration tm = new YamlReport.TableMigration(original, target, diffTable);
				this.report.todo.tableMigrations.add(tm);
				return changes.size() > 0;
			}
		} else {
			return changes.size() > 0;
		}

	}

	private void dropTable(YamlTable dTable) throws CommandNeedRetryException {
		this.performCmd(String.format("DROP TABLE %s.%s", dTable.database, dTable.name));
	}

	private void createTable(YamlTable dTable) throws CommandNeedRetryException {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("CREATE %s TABLE %s.%s (", (dTable.external.booleanValue() ? "EXTERNAL" : ""), dTable.database, dTable.name));
		String sep = "";
		for (YamlField field : dTable.fields) {
			sb.append(String.format("%s %s %s", sep, field.name, field.type));
			if (field.comment != null) {
				sb.append(" COMMENT '" + field.comment + "'");
			}
			sep = ",";
		}
		sb.append(" )");
		if (dTable.comment != null) {
			sb.append(" COMMENT '" + dTable.comment + "'");
		}
		if (dTable.location != null) {
			sb.append(" LOCATION '" + this.normalizePath(dTable.location) + "'");
		}
		if (dTable.properties.size() > 0) {
			sb.append(" TBLPROPERTIES (");
			sep = "";
			for (String keyp : dTable.properties.keySet()) {
				sb.append(String.format("%s '%s'='%s'", sep, keyp, dTable.properties.get(keyp)));
				sep = ",";
			}
			sb.append(")");
		}
		this.performCmd(sb.toString());
	}

}
