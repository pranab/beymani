/*
 * beymani: Outlier and anamoly detection 
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.beymani.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.chombo.util.BasicUtils;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataStreamSchema implements Serializable {
	private List<DataStream> dataStreams;
	
	/**
	 * 
	 */
	public DataStreamSchema() {
	}

	/**
	 * @return
	 */
	public List<DataStream> getDataStreams() {
		return dataStreams;
	}

	/**
	 * @param dataStreams
	 */
	public void setDataStreams(List<DataStream> dataStreams) {
		this.dataStreams = dataStreams;
	}

	/**
	 * @param type
	 * @return
	 */
	public DataStream findByType(String type) {
		DataStream stream = null;
		for (DataStream daStrm : dataStreams) {
			if (daStrm.getType().equals(type)) {
				stream = daStrm;
				break;
			}
		}	
		return stream;
	}
	
	/**
	 * @param type
	 * @return
	 */
	public List<DataStream> findAllByType(String type) {
		List<DataStream> streams = new ArrayList<DataStream>();
		for (DataStream daStrm : dataStreams) {
			if (daStrm.getType().equals(type)) {
				streams.add(daStrm);
			}
		}	
		return streams;
	}

	/**
	 * @param type
	 * @return
	 */
	public DataStream findByTypeAndId(String type, String id) {
		DataStream stream = null;
		for (DataStream daStrm : dataStreams) {
			if (daStrm.getId().equals("*")) {
				if (daStrm.getType().equals(type)) {
					boolean done = false;
					List<DataStream> parents = findAllByType(daStrm.getParentType());
					for (DataStream pa : parents) {
						List<String> children = pa.getChildrenId();
						BasicUtils.assertNotNull(children, "missing child ID list in parent");
						if (children.contains(id)) {
							BasicUtils.assertCondition(daStrm.getParentId().equals(pa.getId()), "mismatched parent ID");
							stream = daStrm;
							done = true;
							break;
						}
					}
					if (done)
						break;
				}
			} else {
				if (daStrm.getType().equals(type) && daStrm.getId().equals(id)) {
					stream = daStrm;
					break;
				}
			}
		}	
		return stream;
	}

	/**
	 * @param type
	 * @param id
	 * @return
	 */
	public DataStream findParent(String type, String id) {
		DataStream parentStream = null;
		DataStream stream = findByType(type);
		BasicUtils.assertNotNull(stream, "coud not find data stream object");
		parentStream = findByType(stream.getParentType());
		if (!parentStream.isSingleton()) {
			//instance based
			stream = findByTypeAndId(type, id);
			parentStream = findByTypeAndId(stream.getParentType(), stream.getParentId());
		} 
		return parentStream;
	}

	/**
	 * @param type
	 * @return
	 */
	public String findParentType(String type) {
		DataStream stream = findByType(type);
		BasicUtils.assertNotNull(stream, "coud not find data stream object");
		return stream.getParentType();
	}
	
	/**
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static DataStreamSchema loadDataStreamSchema(String path) throws IOException {
        InputStream fs = new FileInputStream(path);
        ObjectMapper mapper = new ObjectMapper();
        DataStreamSchema schema = mapper.readValue(fs, DataStreamSchema.class);
        return schema;
	}
	
}
