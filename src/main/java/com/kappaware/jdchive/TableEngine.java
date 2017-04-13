package com.kappaware.jdchive;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kappaware.jdchive.yaml.YamlField;
import com.kappaware.jdchive.yaml.YamlReport;
import com.kappaware.jdchive.yaml.YamlState;
import com.kappaware.jdchive.yaml.YamlTable;

@SuppressWarnings("deprecation")
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

	private void createTable(YamlTable dTable) throws CommandNeedRetryException, DescriptionException {
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
		if (dTable.delimited != null) {
			sb.append(" ROW FORMAT DELIMITED");
			if (dTable.delimited.fields_terminated_by != null) {
				sb.append(String.format(" FIELDS TERMINATED BY '%c'", dTable.delimited.fields_terminated_by));
			}
		} else if (dTable.serde != null) {
			sb.append(String.format(" ROW FORMAT SERDE '%s'"));
			if (dTable.serde_properties.size() > 0) {
				sb.append(String.format(" WITH SERDEPROPERTIES (%s)", this.buildPropertiesAsString(dTable.serde_properties)));
			}
		}
		if (dTable.file_format != null) {
			sb.append(String.format(" STORED AS %s", dTable.file_format));
		} else if (dTable.input_format != null) {
			sb.append(String.format(" INPUTFORMAT '%s' OUTPUTFORMAT '%s'", dTable.input_format, dTable.output_format));
		}
		if (dTable.storage_handler != null) {
			sb.append(String.format(" STORED BY '%s'", dTable.storage_handler));
			if (dTable.serde_properties.size() > 0) {
				sb.append(String.format(" WITH SERDEPROPERTIES (%s)", this.buildPropertiesAsString(dTable.serde_properties)));
			}
		}
		if (dTable.location != null) {
			sb.append(" LOCATION '" + this.normalizePath(dTable.location) + "'");
		}
		if (dTable.properties.size() > 0) {
			sb.append(String.format(" TBLPROPERTIES (%s)", this.buildPropertiesAsString(dTable.properties)));
		}
		this.performCmd(sb.toString());
	}

	private String buildPropertiesAsString(Map<String, String> properties) {
		StringBuffer sb = new StringBuffer();
		String sep = "";
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			sb.append(String.format("%s '%s'='%s'", sep, entry.getKey(), entry.getValue()));
			sep = ",";
		}
		return sb.toString();
	}

	/**
	 * Perform update operation on a table if possible. If not, then add a migration in the 'todo' part of the report.
	 * - 
	 * @param table	The table description, as provided by Hive
	 * @param target The target table, as in the description file
	 * @return True if an operation was performed. Adding a migration to the report will NOT set this.
	 * @throws CommandNeedRetryException
	 * @throws JsonProcessingException 
	 * @throws DescriptionException 
	 */
	private boolean updateTable(Table table, YamlTable target) throws CommandNeedRetryException, JsonProcessingException, DescriptionException {
		log.info(String.format("Found existing table: %s", table.toString()));
		List<String> changes = new Vector<String>();

		int migration = 0;

		// Rebuild and original in Yaml format, to ease comparaison
		YamlTable original = new YamlTable();
		original.name = table.getTableName();
		original.database = table.getDbName();
		original.external = new Boolean(table.getTableType() == TableType.EXTERNAL_TABLE);
		original.owner = table.getOwner();
		original.fields = new Vector<YamlField>();
		for (FieldSchema field : table.getCols()) {
			YamlField f = new YamlField();
			f.name = field.getName();
			f.type = field.getType();
			f.comment = field.getComment();
			original.fields.add(f);
		}
		original.comment = table.getParameters().get("comment");
		original.input_format = table.getSd().getInputFormat();
		original.output_format = table.getSd().getOutputFormat();
		if (table.getSd().getSerdeInfo() != null) {
			original.serde = table.getSd().getSerdeInfo().getName();
			original.serde_properties = table.getSd().getSerdeInfo().getParameters();
		}

		original.location = table.getDataLocation().toString();
		original.properties = table.getParameters();

		// Normalize the target
		target.location = this.normalizePath(target.location);

		if (target.input_format == null && target.output_format == null && target.storage_handler == null && target.file_format == null) {
			target.file_format = "TEXTFILE";
		}
		if (target.file_format != null) {
			StorageFormatFactory sff = new StorageFormatFactory();
			StorageFormatDescriptor sfDescriptor = sff.get(target.file_format);
			if (target.input_format == null) {
				target.input_format = sfDescriptor.getInputFormat();
			}
			if (target.output_format == null) {
				target.output_format = sfDescriptor.getOutputFormat();
			}
			if (target.serde == null) {
				target.serde = sfDescriptor.getSerde();
			}
		}

		// Create an object to store the differences.
		YamlTable diffTable = new YamlTable();
		diffTable.name = table.getTableName();
		diffTable.database = table.getDbName();

		// --------------------------------------------------------------- external switch
		if (!original.external.equals(target.external)) {
			diffTable.external = target.external;
			migration++;
		}
		// ---------------------------------------------------------------- fields list
		int commonFieldCount = Math.min(original.fields.size(), target.fields.size());
		// A loop to adjust comment on field, provided this is the only change (Same name, type and position)
		int changedFields = 0;
		for (int i = 0; i < commonFieldCount; i++) {
			if (original.fields.get(i).almostEquals(target.fields.get(i))) {
				if (!Utils.isEqual(original.fields.get(i).comment, target.fields.get(i).comment)) {
					if (!original.external || Utils.isDifferent(original.fields.get(i).comment, "from deserializer")) { // Seems on some external table, fields comments are constant 'from deserializer'
						original.fields.get(i).comment = target.fields.get(i).comment;
						changes.add(String.format("ALTER TABLE %s.%s CHANGE COLUMN %s %s %s COMMENT '%s'", table.getDbName(), table.getTableName(), original.fields.get(i).name, original.fields.get(i).name, original.fields.get(i).type, original.fields.get(i).comment));
					}
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
		// ---------------------------------------------------------------- Handle table comment
		if (Utils.isDifferent(original.comment, target.comment)) {
			original.comment = target.comment;
			if (target.comment == null) {
				changes.add(String.format("ALTER TABLE %s.%s UNSET TBLPROPERTIES ('comment')", table.getDbName(), table.getTableName()));
			} else {
				changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('comment' = '%s')", table.getDbName(), table.getTableName(), target.comment));
			}
		}
		// ------------------------------------------------------------------ Handle file format
		if (Utils.isDifferent(original.input_format, target.input_format)) {
			diffTable.input_format = Utils.nullMarker(target.input_format);
			migration++;
		}
		if (Utils.isDifferentWithSubstitute(original.output_format, target.output_format, hiveOutputFormatSubstitute)) {
			diffTable.output_format = Utils.nullMarker(target.output_format);
			migration++;
		}
		// ------------------------------------------------------------------- Handle serde
		if (Utils.isDifferent(original.serde, target.serde)) {
			original.serde = target.serde;
			changes.add(String.format("ALTER TABLE %s.%s SET SERDE '%s'", table.getDbName(), table.getTableName(), target.serde));
		}
		// ------------------------------------------------------------------- Handle serde properties (Preserve existing one if not redefined)
		for (Map.Entry<String, String> entry : target.serde_properties.entrySet()) {
			if (Utils.isDifferent(entry.getValue(), original.serde_properties.get(entry.getKey()))) {
				changes.add(String.format("ALTER TABLE %s.%s SET SERDEPROPERTIES ('%s' = '%s')", table.getDbName(), table.getTableName(), entry.getKey(), entry.getValue()));
			}
		}

		// ----------------------------------------------------------------- Handle location
		if (target.location != null) {
			if (!target.location.equals(original.location)) {
				diffTable.location = Utils.nullMarker(target.location);
				migration++;
			}
		}
		// ---------------------------------------------------------------- Handle table properties (We preserve existing properties)
		for (String keyp : target.properties.keySet()) {
			if (!original.properties.containsKey(keyp) || !original.properties.get(keyp).equals(target.properties.get(keyp))) {
				original.properties.put(keyp, target.properties.get(keyp));
				changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('%s' = '%s')", table.getDbName(), table.getTableName(), keyp, target.properties.get(keyp)));
			}
		}

		// --------------------------------------------------------------- Apply modification
		if (changes.size() > 0 && (migration == 0 || !target.droppable)) {
			this.performCmds(changes);
		}
		if (migration > 0) {
			if (target.droppable) {
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

	
	/**
	 * This information is grabbed from org.apache.hadoop.hive.ql.io.HiveFileFormatUtils.outputFormatSubstituteMap
	 */
	static Map<String, Set<String>> hiveOutputFormatSubstitute;
	static {
		hiveOutputFormatSubstitute = new HashMap<String, Set<String>>();
		hiveOutputFormatSubstitute.put(IgnoreKeyTextOutputFormat.class.getName(), new HashSet<String>(Arrays.asList(HiveIgnoreKeyTextOutputFormat.class.getName())));
		hiveOutputFormatSubstitute.put(SequenceFileOutputFormat.class.getName(), new HashSet<String>(Arrays.asList(HiveSequenceFileOutputFormat.class.getName())));
	}
	
	private void dropTable(YamlTable dTable) throws CommandNeedRetryException, DescriptionException {
		this.performCmd(String.format("DROP TABLE %s.%s", dTable.database, dTable.name));
	}

}
