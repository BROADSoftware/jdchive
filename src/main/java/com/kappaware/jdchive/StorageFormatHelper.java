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
package com.kappaware.jdchive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;

import com.kappaware.jdchive.yaml.YamlTable;


/**
 * Code adapted from org.apache.hadoop.hive.ql.parse.StorageFormat;
 * 
 * @author sa
 *
 */
public class StorageFormatHelper {
	private static final StorageFormatFactory storageFormatFactory = new StorageFormatFactory();

	private HiveConf conf;
	
	private String inputFormat;
	private String outputFormat;
	private String storageHandler;
	private String serde;

	StorageFormatHelper(HiveConf conf, YamlTable tbl) throws DescriptionException {
		this.conf = conf;
		if (tbl.stored_as != null) {
			this.processStorageFormat(tbl.stored_as.toString());
		} 
		if(tbl.input_format != null) {
			this.inputFormat = ensureClassExists(tbl.input_format);
		}
		if(tbl.output_format != null) {
			this.outputFormat = ensureClassExists(tbl.output_format);
		}
		if(tbl.storage_handler != null) {
			this.storageHandler = ensureClassExists(tbl.storage_handler);
		}
		if(tbl.serde != null) {
			this.serde = ensureClassExists(tbl.serde);
		}
		this.fillDefaultStorageFormat(tbl.external.booleanValue());
	}

	
	
	private void processStorageFormat(String name) throws DescriptionException {
		StorageFormatDescriptor descriptor = storageFormatFactory.get(name);
		if (descriptor == null) {
			throw new DescriptionException("Unrecognized file format:" + " '" + name + "'");
		}
		inputFormat = ensureClassExists(descriptor.getInputFormat());
		outputFormat = ensureClassExists(descriptor.getOutputFormat());
		if (serde == null) {
			serde = ensureClassExists(descriptor.getSerde());
		}
		if (serde == null) {
			// RCFile supports a configurable SerDe
			if (name.equalsIgnoreCase(IOConstants.RCFILE)) {
				serde = ensureClassExists(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE));
			} else {
				serde = ensureClassExists(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTSERDE));
			}
		}
	}

	protected void fillDefaultStorageFormat(boolean isExternal) throws DescriptionException {
		if ((inputFormat == null) && (storageHandler == null)) {
			String defaultFormat;
			String defaultManagedFormat;
			defaultFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT);
			defaultManagedFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTMANAGEDFILEFORMAT);

			if (!isExternal && !"none".equals(defaultManagedFormat)) {
				defaultFormat = defaultManagedFormat;
			}

			if (StringUtils.isBlank(defaultFormat)) {
				inputFormat = IOConstants.TEXTFILE_INPUT;
				outputFormat = IOConstants.TEXTFILE_OUTPUT;
			} else {
				processStorageFormat(defaultFormat);
				if (defaultFormat.equalsIgnoreCase(IOConstants.RCFILE)) {
					serde = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
				}
			}
		}
	}

	public static String ensureClassExists(String className) throws DescriptionException {
		if (className == null) {
			return null;
		}
		try {
			Class.forName(className, true, JavaUtils.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new DescriptionException("Cannot find class '" + className + "'", e);
		}
		return className;
	}

	public String getInputFormat() {
		return inputFormat;
	}

	public String getOutputFormat() {
		return outputFormat;
	}

	public String getStorageHandler() {
		return storageHandler;
	}

	public String getSerde() {
		return serde;
	}

}
