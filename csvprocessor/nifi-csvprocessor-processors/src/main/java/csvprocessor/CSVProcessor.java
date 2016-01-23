/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * A NiFi processor, written by Obaid[obaidcuet@gmail.com] 
 * following code of other Existing Nifi processors. 
 */
package csvprocessor;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.opencsv.CSVReader;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"example checkfile content"})
@CapabilityDescription("Example of reading file size and body")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
//@WritesAttributes({@WritesAttribute(attribute="", description="")})

public class CSVProcessor extends AbstractProcessor {

    public static final PropertyDescriptor HEADER_EXISTS = new PropertyDescriptor
            .Builder().name("HeaderExists")
            .description("Is there any header?")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SKIP_HEADER = new PropertyDescriptor
            .Builder().name("SkipHeader")
            .description("Should skip header?")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor SKIP_EMPTY_LINE = new PropertyDescriptor
            .Builder().name("SkipEmptyLine")
            .description("Should skip empty line?")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor SEPERATOR = new PropertyDescriptor
            .Builder().name("Seperator")
            .description("Seperator of the source data.")
            .required(true)
            .allowableValues(",", "|","\\t",":","-")
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
   
    public static final PropertyDescriptor END_OF_LINE = new PropertyDescriptor
            .Builder().name("EndOfLine")
            .description("End of line symbol.")
            .required(true)
            .allowableValues("\\n \n","\\r \r","\\n\\r \n\r")
            .defaultValue("\\n \n")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    
    public static final PropertyDescriptor QUOTE = new PropertyDescriptor
            .Builder().name("Quote")
            .description("Option quote string of the source data.")
            .required(true)
            .allowableValues("\"", "'")
            .defaultValue("\"")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
   
    public static final PropertyDescriptor ESCAPE = new PropertyDescriptor
            .Builder().name("Escape")
            .description("Option escape character of the source data.")
            .required(true)
            .allowableValues("\\", "^")
            .defaultValue("\\")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build(); 
    
    public static final PropertyDescriptor MASK_COLUMNS = new PropertyDescriptor
            .Builder().name("MaskColumns")
            .description("Comma seperated column position[starting from 0] those should be masked. -1 mean no masking.")
            .required(true)
            .defaultValue("-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build(); 

    public static final PropertyDescriptor QUOTED_COLUMNS = new PropertyDescriptor
            .Builder().name("QuotedColumns")
            .description("Comma seperated column position[starting from 0] those should be quoted. -1 mean no quote, -2 means all quoted.")
            .required(true)
            .defaultValue("-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();  
    
    public static final PropertyDescriptor ESCAPED_COLUMNS = new PropertyDescriptor
            .Builder().name("EscapedColumns")
            .description("Comma seperated column position[starting from 0] those may have quote char as value and needs escape them. -1 mean no quote, -2 means all quoted.")
            .required(true)
            .defaultValue("-1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();  
    
    
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
	        .name("CharacterSet")
	        .description("The Character Set in which the file is encoded")
	        .required(true)
	        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
	        .defaultValue("ASCII")
	        .build();

    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("MaxBufferSize")
            .description("Specifies the maximum amount of data to buffer (per file or per line, depending on the Evaluation Mode) in order to "
                + "apply the replacement. If 'Entire Text' (in Evaluation Mode) is selected and the FlowFile is larger than this value, "
                + "the FlowFile will be routed to 'failure'. "
                + "In 'Line-by-Line' Mode, if a single line is larger than this value, the FlowFile will be routed to 'failure'. A default value "
                + "of 1 MB is provided, primarily for 'Entire Text' mode. In 'Line-by-Line' Mode, a value such as 8 KB or 16 KB is suggested. "
                + "This value is ignored if the <Replacement Strategy> property is set to one of: Append, Prepend, Always Replace")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build(); 
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success_relationship")
            .description("Success relationship")
            .build();
    
    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("Fail_relationship")
            .description("Fail relationship")
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HEADER_EXISTS);
        descriptors.add(SKIP_HEADER);
        descriptors.add(HEADER_EXISTS);
        descriptors.add(SKIP_EMPTY_LINE);        
        descriptors.add(SEPERATOR);
        descriptors.add(END_OF_LINE);
        descriptors.add(QUOTE);
        descriptors.add(ESCAPE);
        descriptors.add(MASK_COLUMNS);
        descriptors.add(QUOTED_COLUMNS);
        descriptors.add(ESCAPED_COLUMNS);
        descriptors.add(CHARACTER_SET);
        descriptors.add(MAX_BUFFER_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAIL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();
        FlowFile flowfile = session.get();
        if ( flowfile == null ) {
        	logger.info("Null FlowFile.");
            return;
        }
                
        // TODO implement
        final boolean skipHeader = Boolean.parseBoolean(context.getProperty(SKIP_HEADER).getValue());
        final boolean headerExists = Boolean.parseBoolean(context.getProperty(HEADER_EXISTS).getValue());
        final boolean skipEmptyLine = Boolean.parseBoolean(context.getProperty(SKIP_EMPTY_LINE).getValue());
        final String seperator = context.getProperty(SEPERATOR).getValue();
        final String quote = context.getProperty(QUOTE).getValue();
        final String escape = context.getProperty(ESCAPE).getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
		final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
		//final StringBuilder lineEndingBuilder = new StringBuilder(2);
		//lineEndingBuilder.setLength(0);
		//lineEndingBuilder.append(context.getProperty(END_OF_LINE).getValue().split(" ")[1]);//second part is the actual line seperator character
		final String lineEnder = context.getProperty(END_OF_LINE).getValue().split(" ")[1];//second part is the actual line seperator character

		final String maskColumns = context.getProperty(MASK_COLUMNS).getValue().trim();
		Integer [] maskColumnsIdx;
		if (maskColumns != null && !maskColumns.equals("-1") && maskColumns.length() > 0) {

			String[] maskCols = maskColumns.split(",");
	    	
			// collect index of masked columns 
			maskColumnsIdx = new Integer[maskCols.length];
	    	for(int i=0; i <maskColumnsIdx.length; i++) {
	        	try {
	        		maskColumnsIdx[i] =  Integer.parseInt((maskCols[i].trim()));
	        	}catch(Exception e) {
	        		maskColumnsIdx[i] = -1;
	        	}
	        }	     
		} else {
			maskColumnsIdx = null;
		}
	    
		
		final String quotedColumns = context.getProperty(QUOTED_COLUMNS).getValue().trim();
		Integer [] quotedColumnsIdx;
		if (quotedColumns != null && !quotedColumns.equals("-1") && quotedColumns.length() > 0) {

			String[] quotedCols = quotedColumns.split(",");
	    	
			// collect index of masked columns 
			quotedColumnsIdx = new Integer[quotedCols.length];
	    	for(int i=0; i <quotedColumnsIdx.length; i++) {
	        	try {
	        		quotedColumnsIdx[i] =  Integer.parseInt((quotedCols[i].trim()));
	        	}catch(Exception e) {
	        		quotedColumnsIdx[i] = -1;
	        	}
	        }	     
		} else {
			quotedColumnsIdx = null;
		}
		
		
		final String escapedColumns = context.getProperty(ESCAPED_COLUMNS).getValue().trim();
		Integer [] escapedColumnsIdx;
		if (escapedColumns != null && !escapedColumns.equals("-1") && escapedColumns.length() > 0) {

			String[] escapedCols = escapedColumns.split(",");
	    	
			// collect index of masked columns 
			escapedColumnsIdx = new Integer[escapedCols.length];
	    	for(int i=0; i <escapedColumnsIdx.length; i++) {
	        	try {
	        		escapedColumnsIdx[i] =  Integer.parseInt((escapedCols[i].trim()));
	        	}catch(Exception e) {
	        		escapedColumnsIdx[i] = -1;
	        	}
	        }	     
		} else {
			escapedColumnsIdx = null;
		}
		
		
		
		try {
        
	        final StopWatch stopWatch = new StopWatch(true);
	        flowfile = session.write(flowfile, new StreamCallback() {
	        	@Override
	            public void process(InputStream in, OutputStream out) throws IOException {
					
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset), maxBufferSize);
                        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, charset));) 
                    {
                    	//CSVReader csvreader =  new CSVReader(reader, seperator.toCharArray()[0], quote.toCharArray()[0], escape.toCharArray()[0]);                    	

                    	if(skipHeader == true && headerExists==true) { // to skip header, do an additional line fetch before going to next step								
							
                    		if(reader.readLine()!=null) {}//just check null, do nothing
							
						} else if( skipHeader == false && headerExists == true) { // if header is not skipped then no need to mask, just pass through

							writer.write(reader.readLine() + lineEnder);  
							//writer.write(lineEndingBuilder.toString()); 
						}
							
						String line="";
						while ((line = reader.readLine()) != null) { 
							if(line.length() > 0 ) { // non empty line process normally
								writer.write( parseLine(line, seperator, quote, escape, maskColumnsIdx, quotedColumnsIdx, escapedColumnsIdx) + lineEnder );
								//writer.write( lineEndingBuilder.toString() );
							} else if (skipEmptyLine == true && line.length() == 0 ) { // empty line and want to skip
								//do nothing just skip
							} else { // empty line and do not want to skip
								//writer.write(parseLine(line, seperator, quote, escape, maskColumns));  //process normally
								//writer.write(lineEndingBuilder.toString());
								writer.write(lineEnder);
							}
									
						};
						
						//csvreader.close();	
						writer.flush();							
                    }
	            }
	
	        });
				
	        session.getProvenanceReporter().modifyContent(flowfile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
	        session.transfer(flowfile, REL_SUCCESS);
	        
	    } catch(Exception ex) {
            ex.printStackTrace();
            logger.error(getStackTrace(ex));
	    }
        
    }
    
    // method to process exception stack trace
    public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }
    
    // mail method to process record by record
    public String parseLine(String inLine, String seperator, String quote, String escape, Integer[] maskColumnsIdx, Integer[] quotedColumnsIdx, Integer[] escapedColumnsIdx) throws IOException {
    	String retLine= "";
    	
    	CSVReader csvreader =  new CSVReader(new StringReader(inLine), seperator.toCharArray()[0], quote.toCharArray()[0], escape.toCharArray()[0]);
    	String []strEvent; // holding columns of current line
    	
    	if ((strEvent = csvreader.readNext()) != null) {
			
    		//escape targeted columns
    		if(escapedColumnsIdx != null) {  // escape only targeted columns
    			if(escapedColumnsIdx[0] > -1) {
		    		for (int i=0; i < escapedColumnsIdx.length; i++) {	
		    			if( escapedColumnsIdx[i] > -1 && escapedColumnsIdx[i] < strEvent.length)
		    				strEvent[ escapedColumnsIdx[i] ]= strEvent[ escapedColumnsIdx[i] ].replaceAll(quote, "\\"+escape+quote); //escape all the qoutes						
					}
    			} else if(escapedColumnsIdx[0] == -2) { // escape all
    				for (int i=0; i < strEvent.length; i++) 
    					strEvent[i]= strEvent[i].replaceAll(quote, "\\"+escape+quote); //escape all the qoutes
    			}	
    			
    		}
    		
				
			//quote targeted columns
			if (quotedColumnsIdx != null) {	     
				if(quotedColumnsIdx[0] > -1) { // quote only target column
					for(int i=0; i<quotedColumnsIdx.length;i++) 
						if(quotedColumnsIdx[i] < strEvent.length)
							strEvent[ quotedColumnsIdx[i] ] = quote + strEvent[  quotedColumnsIdx[i] ] + quote ; //always masking last 6 characters			        			
				}else if(quotedColumnsIdx[0] == -2){// quote all
					for(int i=0; i<strEvent.length;i++) 
						strEvent[i] = quote + strEvent[i] + quote ;			        							
				}
			}
			
			
			// TODO: Do other operations as per needed
			//
			//
			
			// putting everything together
			for(int i =0; i<strEvent.length; i++) {			
				retLine = retLine + strEvent[i]	;		
				if(i < strEvent.length-1) // add seperator until it is the last column
					retLine = retLine + seperator;
			}
    	}
		csvreader.close();
		return retLine.trim();
	}
}
