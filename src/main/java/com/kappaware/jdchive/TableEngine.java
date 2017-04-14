package com.kappaware.jdchive;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.hive.serde.serdeConstants;
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

import groovy.json.StringEscapeUtils;

@SuppressWarnings("deprecation")
public class TableEngine extends BaseEngine {
	static Logger log = LoggerFactory.getLogger(TableEngine.class);

	// Taken from org.apache.hadoop.hive.serde.serdeConstants
	/*
	private static final String FIELD_DELIM = "field.delim";
	private static final String COLLECTION_DELIM = "colelction.delim";
	private static final String LINE_DELIM = "line.delim";
	private static final String MAPKEY_DELIM = "mapkey.delim";
	private static final String ESCAPE_CHAR = "escape.delim";
	private static final String SERIALIZATION_NULL_FORMAT = "serialization.null.format";
	private static final String SERIALIZATION_FORMAT = "serialization.format";
	*/
	public TableEngine(Driver driver, YamlReport report, boolean dryRun) throws HiveException {
		super(driver, report, dryRun);
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
				sb.append(String.format(" FIELDS TERMINATED BY '%s'", dTable.delimited.fields_terminated_by));
			}
			if (dTable.delimited.fields_escaped_by != null) {
				sb.append(String.format(" ESCAPED BY '%s'", dTable.delimited.fields_escaped_by));
			}
			if (dTable.delimited.collection_item_terminated_by != null) {
				sb.append(String.format(" COLLECTION ITEMS TERMINATED BY '%s'", dTable.delimited.collection_item_terminated_by));
			}
			if (dTable.delimited.map_keys_terminated_by != null) {
				sb.append(String.format(" MAP KEYS TERMINATED BY '%s'", dTable.delimited.map_keys_terminated_by));
			}
			if (dTable.delimited.lines_terminated_by != null) {
				sb.append(String.format(" LINES TERMINATED BY '%s'", dTable.delimited.lines_terminated_by));
			}
			if (dTable.delimited.null_defined_as != null) {
				sb.append(String.format(" NULL DEFINED AS '%s'", dTable.delimited.null_defined_as));
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
		YamlTable existing = new YamlTable();
		existing.name = table.getTableName();
		existing.database = table.getDbName();
		existing.external = new Boolean(table.getTableType() == TableType.EXTERNAL_TABLE);
		existing.owner = table.getOwner();
		existing.fields = new Vector<YamlField>();
		for (FieldSchema field : table.getCols()) {
			YamlField f = new YamlField();
			f.name = field.getName();
			f.type = field.getType();
			f.comment = field.getComment();
			existing.fields.add(f);
		}
		existing.comment = table.getParameters().get("comment");

		existing.input_format = table.getSd().getInputFormat();
		existing.output_format = table.getSd().getOutputFormat();
		if (table.getSd().getSerdeInfo() != null) {
			log.debug(String.format("Serde name:'%s'   serializationLib:'%s'", table.getSd().getSerdeInfo().getName(), table.getSd().getSerdeInfo().getSerializationLib()));
			existing.serde = table.getSd().getSerdeInfo().getSerializationLib();
			existing.serde_properties = table.getSd().getSerdeInfo().getParameters();
		}

		existing.location = table.getDataLocation().toString();
		existing.properties = table.getParameters();

		// Normalize the target
		target.location = this.normalizePath(target.location);

		if (target.input_format == null && target.output_format == null && target.storage_handler == null && target.file_format == null) {
			target.file_format = "TEXTFILE";
		}
		if (target.file_format != null) {
			StorageFormatFactory sff = new StorageFormatFactory(); // Refer to https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide#DeveloperGuide-RegistrationofNativeSerDes
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
		if (target.delimited != null) {
			if (target.delimited.fields_terminated_by != null) {
				target.serde_properties.put(serdeConstants.FIELD_DELIM, StringEscapeUtils.unescapeJava(target.delimited.fields_terminated_by));
			}
			if (target.delimited.fields_escaped_by != null) {
				target.serde_properties.put(serdeConstants.ESCAPE_CHAR, StringEscapeUtils.unescapeJava(target.delimited.fields_escaped_by));
			}
			if (target.delimited.collection_item_terminated_by != null) {
				target.serde_properties.put(serdeConstants.COLLECTION_DELIM, StringEscapeUtils.unescapeJava(target.delimited.collection_item_terminated_by));
			}
			if (target.delimited.map_keys_terminated_by != null) {
				target.serde_properties.put(serdeConstants.MAPKEY_DELIM, StringEscapeUtils.unescapeJava(target.delimited.map_keys_terminated_by));
			}
			if (target.delimited.lines_terminated_by != null) {
				target.serde_properties.put(serdeConstants.LINE_DELIM, StringEscapeUtils.unescapeJava(target.delimited.lines_terminated_by));
			}
			if (target.delimited.null_defined_as != null) {
				target.serde_properties.put(serdeConstants.SERIALIZATION_NULL_FORMAT, StringEscapeUtils.unescapeJava(target.delimited.null_defined_as));
			}
		}
		if (target.external) {
			// An external table will not accept any ALTER command
			target.alterable = false;
		}

		// ------------------------------------------------------ Create an object to store the differences.
		YamlTable diffTable = new YamlTable();
		diffTable.name = table.getTableName();
		diffTable.database = table.getDbName();

		// --------------------------------------------------------------- external switch
		if (!existing.external.equals(target.external)) {
			diffTable.external = target.external;
			migration++;
		}
		// ---------------------------------------------------------------- fields list
		int commonFieldCount = Math.min(existing.fields.size(), target.fields.size());
		diffTable.fields = new Vector<YamlField>();
		for (int i = 0; i < commonFieldCount; i++) {
			YamlField existingField = existing.fields.get(i);
			YamlField targetField = target.fields.get(i);
			if (existingField.name.equalsIgnoreCase(targetField.name)) {
				if (existingField.type.equalsIgnoreCase(targetField.type)) {
					// Type is unchanged
					if (Utils.isTextDifferent(existingField.comment, targetField.comment)) {
						log.debug(String.format("Table %s.%s.%s: Comment diff: existing:%s  target:%s", existing.database, existing.name, existingField.name, existingField.comment, targetField.comment));
						if (Utils.isDifferent(existingField.comment, "from deserializer")) { // 'from deserializer' is always returned as comment with some specific serde
							if (!target.external) { // An external table will not accept any ALTER command
								changes.add(String.format("ALTER TABLE %s.%s CHANGE COLUMN %s %s %s COMMENT '%s'", table.getDbName(), table.getTableName(), existingField.name, targetField.name, targetField.type, targetField.comment));
							} else {
								diffTable.fields.add(targetField);
								migration++;
							}
						}
					}
				} else {
					// type is changed
					if (target.alterable) {
						changes.add(String.format("ALTER TABLE %s.%s CHANGE COLUMN %s %s %s COMMENT '%s'", table.getDbName(), table.getTableName(), existingField.name, targetField.name, targetField.type, targetField.comment));
					} else {
						diffTable.fields.add(targetField);
						migration++;
					}
				}
			} else {
				diffTable.fields.add(targetField);
				migration++;
			}
		}
		for (int i = commonFieldCount; i < target.fields.size(); i++) {
			diffTable.fields.add(target.fields.get(i));
			migration++;
		}
		for (int i = commonFieldCount; i < existing.fields.size(); i++) {
			diffTable.fields.add(existing.fields.get(i));
			migration++;
		}
		// ---------------------------------------------------------------- Handle table comment
		if (Utils.isDifferent(existing.comment, target.comment)) {
			if (!target.external) { // An external table will not accept any ALTER command
				if (target.comment == null) {
					changes.add(String.format("ALTER TABLE %s.%s UNSET TBLPROPERTIES ('comment')", table.getDbName(), table.getTableName()));
				} else {
					changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('comment' = '%s')", table.getDbName(), table.getTableName(), target.comment));
				}
			} else {
				diffTable.comment = target.comment;
				migration++;

			}
		}
		// ------------------------------------------------------------------ Handle row format

		// ------------------------------------------------------------------ Handle file format
		if (Utils.isDifferent(existing.input_format, target.input_format)) {
			diffTable.input_format = Utils.nullMarker(target.input_format);
			migration++;
		}
		if (Utils.isDifferentWithSubstitute(existing.output_format, target.output_format, hiveOutputFormatSubstitute)) {
			diffTable.output_format = Utils.nullMarker(target.output_format);
			migration++;
		}
		// ------------------------------------------------------------------- Handle serde
		if (target.serde != null && Utils.isDifferent(existing.serde, target.serde)) {
			if (target.alterable) {
				changes.add(String.format("ALTER TABLE %s.%s SET SERDE '%s'", table.getDbName(), table.getTableName(), target.serde));
			} else {
				diffTable.serde = target.serde;
				migration++;
			}
		}
		// ------------------------------------------------------------------- Handle serde properties (Preserve existing one if not redefined)
		for (Map.Entry<String, String> entry : target.serde_properties.entrySet()) {
			if (Utils.isDifferent(entry.getValue(), existing.serde_properties.get(entry.getKey()))) {
				log.debug(String.format("Serde property '%s': target: %s != existing: %s", entry.getKey(), Utils.toDebugString(entry.getValue()), Utils.toDebugString(existing.serde_properties.get(entry.getKey()))));
				if (target.alterable) {
					changes.add(String.format("ALTER TABLE %s.%s SET SERDEPROPERTIES ('%s' = '%s')", table.getDbName(), table.getTableName(), entry.getKey(),  StringEscapeUtils.escapeJava(entry.getValue())));
				} else {
					if (diffTable.serde_properties == null) {
						diffTable.serde_properties = new HashMap<String, String>();
					}
					diffTable.serde_properties.put(entry.getKey(), entry.getValue());
					migration++;
				}
			}
		}

		// ----------------------------------------------------------------- Handle location
		if (target.location != null) {
			if (!target.location.equals(existing.location)) {
				diffTable.location = Utils.nullMarker(target.location);
				migration++;
			}
		}
		// ---------------------------------------------------------------- Handle table properties (We preserve existing properties)
		for (Map.Entry<String, String> entry : target.properties.entrySet()) {
			if (Utils.isDifferent(entry.getValue(), existing.properties.get(entry.getKey()))) {
				if (target.alterable) {
					changes.add(String.format("ALTER TABLE %s.%s SET TBLPROPERTIES ('%s' = '%s')", table.getDbName(), table.getTableName(), entry.getKey(),  StringEscapeUtils.escapeJava(entry.getValue())));
				} else {
					if (diffTable.properties == null) {
						diffTable.properties = new HashMap<String, String>();
					}
					diffTable.properties.put(entry.getKey(), entry.getValue());
					migration++;
				}
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
				YamlReport.TableMigration tm = new YamlReport.TableMigration(existing, target, diffTable);
				this.report.todo.tableMigrations.add(tm);
				return changes.size() > 0;
			}
		} else {
			return changes.size() > 0;
		}
	}

	/**
	 * This information is grabbed from org.apache.hadoop.hive.ql.io.HiveFileFormatUtils.outputFormatSubstituteMap
	 * 
	 * 
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
