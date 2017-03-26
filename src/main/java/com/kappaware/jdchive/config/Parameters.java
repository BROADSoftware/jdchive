/*
 * Copyright (C) 2016 BROADSoftware
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
package com.kappaware.jdchive.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import com.kappaware.jdchive.Utils;
import com.kappaware.jdchive.yaml.YamlState;


public class Parameters {
	static Logger log = LoggerFactory.getLogger(Parameters.class);

	private String inputFile;
	private YamlState defaultState;
	private String principal;
	private String keytab;
	private String dumpConfigFile;
	private String reportFile;
	private List<String> configFiles;
	
	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120,2));
	}
	static OptionSpec<String> INPUT_FILE_OPT = parser.accepts("inputFile", "Hbase table layout description").withRequiredArg().describedAs("input_file").ofType(String.class).required();
	static OptionSpec<YamlState> DEFAULT_STATE = parser.accepts("defaultState", "Default entity state").withRequiredArg().describedAs("present|absent").ofType(YamlState.class).defaultsTo(YamlState.present);

	static OptionSpec<String> CONFIG_FILES_OPT = parser.accepts("configFile", "Config file (xxx-site.xml). May be specified several times").withRequiredArg().describedAs("xxxx-site.xml").ofType(String.class);
	
	static OptionSpec<String> PRINCIPAL_OPT = parser.accepts("principal", "Kerberos principal").withRequiredArg().describedAs("principal").ofType(String.class);
	static OptionSpec<String> KEYTAB_OPT = parser.accepts("keytab", "Keytyab file path").withRequiredArg().describedAs("keytab_file").ofType(String.class);

	static OptionSpec<String> DUMP_CONFIG_FILE_OPT = parser.accepts("dumpConfigFile", "Debuging purpose: All HBaseConfiguration will be dumped in this file").withRequiredArg().describedAs("dump_file").ofType(String.class);
	static OptionSpec<String> REPORT_FILE_OPT = parser.accepts("reportFile", "Allow tracking of performed operation and migration still to perform").withRequiredArg().describedAs("report_file").ofType(String.class);


	
	@SuppressWarnings("serial")
	private static class MyOptionException extends Exception {

		public MyOptionException(String message) {
			super(message);
		}
		
	}

	
	public Parameters(String[] argv) throws ConfigurationException {
		try {
			OptionSet result = parser.parse(argv);
			if (result.nonOptionArguments().size() > 0 && result.nonOptionArguments().get(0).toString().trim().length() > 0) {
				throw new MyOptionException(String.format("Unknow option '%s'", result.nonOptionArguments().get(0)));
			}
			// Mandatories parameters
			this.inputFile = result.valueOf(INPUT_FILE_OPT);
			this.defaultState = result.valueOf(DEFAULT_STATE);
			this.principal = result.valueOf(PRINCIPAL_OPT);
			this.keytab = result.valueOf(KEYTAB_OPT);
			this.dumpConfigFile = result.valueOf(DUMP_CONFIG_FILE_OPT);
			this.reportFile = result.valueOf(REPORT_FILE_OPT);
			this.configFiles = result.valuesOf(CONFIG_FILES_OPT);
		} catch (OptionException | MyOptionException t) {
			throw new ConfigurationException(usage(t.getMessage()));
		}
		if(Utils.isNullOrEmpty(this.principal) ^ Utils.isNullOrEmpty(this.keytab)) {
			throw new ConfigurationException("Both or none of --principal and --keytab must be defined");
		}
	}

	private static String usage(String err) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(baos);
		if (err != null) {
			pw.print(String.format("\n\n * * * * * ERROR: %s\n\n", err));
		}
		try {
			parser.printHelpOn(pw);
		} catch (IOException e) {
		}
		pw.flush();
		pw.close();
		return baos.toString();
	}

	// --------------------------------------------------------------------------

	public String getInputFile() {
		return inputFile;
	}

	public YamlState getDefaultState() {
		return defaultState;
	}

	public String getPrincipal() {
		return this.principal;
	}

	public String getKeytab() {
		return this.keytab;
	}

	public String getDumpConfigFile() {
		return dumpConfigFile;
	}

	public List<String> getConfigFiles() {
		return configFiles;
	}

	public String getReportFile() {
		return reportFile;
	}

	

}
