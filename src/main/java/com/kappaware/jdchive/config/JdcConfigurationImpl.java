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

import java.util.List;

import com.kappaware.jdchive.yaml.YamlState;

public class JdcConfigurationImpl implements JdcConfiguration {
	Parameters parameters;
	
	public JdcConfigurationImpl(Parameters parameters)  {
		this.parameters = parameters;
		
	}

	@Override
	public String getInputFile() {
		return parameters.getInputFile();
	}


	@Override
	public YamlState getDefaultState() {
		return parameters.getDefaultState();
	}

	@Override
	public String getPrincipal() {
		return parameters.getPrincipal();
	}

	@Override
	public String getKeytab() {
		return parameters.getKeytab();
	}

	@Override
	public String getDumpConfigFile() {
		return parameters.getDumpConfigFile();
	}

	
	@Override
	public List<String> getConfigFiles() {
		return parameters.getConfigFiles();
	}

	@Override
	public String getReportFile() {
		return parameters.getReportFile();
	}


		
}
