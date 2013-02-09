/*
 Copyright (c) 2011-2012 NaN Projectes de Programari Lliure, S.L. All Rights Reserved.
                         http://www.NaN-tic.com

 WARNING: This program as such is intended to be used by professional
 programmers who take the whole responsability of assessing all potential
 consequences resulting from its eventual inadequacies and bugs
 End users who are looking for a ready-to-use solution with commercial
 garantees and support are strongly adviced to contract a Free Software
 Service Company

 This program is Free Software; you can redistribute it and/or
 modify it under the terms of the GNU Affero General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.*/

package kafkadb;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.ini4j.ConfigParser;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;

/**
 * 
 * @author angel
 */

public class Migrate {
	static SharedObjects shared = null;
	static JobMeta jobMeta = null;
	static Job job = null;
	static JobEntryCopy start = null;
	static TransMeta transMeta2 = null;
	static Trans trans = null;
	static DatabaseMeta sourceDb = null;
	static DatabaseMeta targetDb = null;
	static int y = 0;
	static int yoffset = 75;
	static int xoffset = 75;
	static String upload = new String();
	static String outputDir = "/tmp/output/";
	static HashMap<TransMeta, List<String>> dependencies = new HashMap<TransMeta, List<String>>();
	static HashMap<String, TransMeta> transMap = new HashMap<String, TransMeta>();
	static HashMap<TransMeta, Trans> transMetaMap = new HashMap<TransMeta, Trans>();

	static ConfigParser config = new ConfigParser();

	// static private Logger log = Logger.getLogger(Migrate.class + "");
	static private Logger log = Logger.getLogger("KafkaDB");

	public static List<String> getPreviousFields(TransMeta transMeta,
			StepMeta step) throws KettleStepException, KettleException {

		// Get fields input from step.
		List<String> fieldList = new ArrayList<String>();

		log.info("Get Previous fields:" + step.getName());
		RowMetaInterface rmi = transMeta.getPrevStepFields(step);
		if (rmi==null && rmi.isEmpty()) 
			return fieldList;
		
		
		log.info("Get previous fields rmi:" + rmi);
		String[] fields = rmi.getFieldNames();
		for (int i = 0; i < fields.length; i++) {
			fieldList.add(fields[i]);
		}

		return fieldList;

	}

	public static String[] getTargetFields(List<String> previousFields,
			String targetTable) throws Exception {

		// Get fields from table output.
		log.info("\n\t\tGetting target Fields from table.:" + targetTable);

		Database targetDatabase = new Database(targetDb);
		targetDatabase.connect();

		String[] targetFields = targetDatabase.getQueryFields(
				"select * from " + targetTable, true).getFieldNames();

		log.info("Target fields:" + targetFields.toString());

		List<String> selectFields = new ArrayList<String>();
		for (int i = 0; i < targetFields.length; i++) {
			if (previousFields.contains(targetFields[i])) {
				selectFields.add(targetFields[i]);
			}
		}
		targetDatabase.disconnect();

		return selectFields.toArray(new String[0]);

	}

	public static TextFileField[] getFileFields(TransMeta transMeta,
			StepMeta step, String targetTable) throws Exception {

		// FIELD FROM PREVIOUS STEP.
		List<String> fieldList = new ArrayList<String>();

		RowMetaInterface rmi = transMeta.getStepFields(step);
		String[] fields = rmi.getFieldNames();
		for (int i = 0; i < fields.length; i++) {
			fieldList.add(fields[i]);
		}

		// FIELD FROM DATABASE.
		Database targetDatabase = new Database(targetDb);
		targetDatabase.connect();
		String[] targetFields = targetDatabase.getQueryFields(
				"select * from " + targetTable, true).getFieldNames();
		targetDatabase.disconnect();

		List<TextFileField> fileFields = new ArrayList<TextFileField>();
		for (int i = 0; i < targetFields.length; i++) {
			if (fieldList.contains(targetFields[i])) {
				int index = fieldList.indexOf(targetFields[i]);
				fileFields.add(new TextFileField(targetFields[i], rmi
						.getValueMeta(index).getType(), new String(), -1, -1,
						new String(), ".", new String(), new String()));
			}
		}
		return fileFields.toArray(new TextFileField[0]);
	}


	public static TransMeta makeTrans(String source, String target,
			List<String> filename) throws Exception {

		
		// Prepare Transformation, load kettle shared file and make
		// available to transformation.
		TransMeta transMeta = new TransMeta();
		transMeta.clear();
		transMeta.clearCaches();
		
		transMeta.setName(source + "_" + target);
		transMeta.setSharedObjects(shared);
		transMeta.readSharedObjects();
		transMeta.setUsingUniqueConnections(true);

		transMap.put(target, transMeta);

		int x = 100;

		// Source Input data.
		TableInputMeta tii = new TableInputMeta();
		tii.setDatabaseMeta(sourceDb);
		String selectSQL = "SELECT * FROM " + source;
		tii.setSQL(selectSQL);

		// Source step.
		StepMeta fromStep = new StepMeta("table_source_" + source,
				(StepMetaInterface) tii);
		fromStep.setLocation(x, y);
		fromStep.setDraw(true);

		
		transMeta.addStep(fromStep);
		
		
		// Get Fields from Input step.
		List<String> fromFields = getPreviousFields(transMeta, fromStep);
		log.info("Previous fields:" + fromFields.toString());
		log.info("Get target fields table:" + target);
		// Get Target field from table
		String[] selectFields = getTargetFields(fromFields, target);

		x += xoffset;

		log.info("filenames:" + filename.toString());
		// LOAD TRANS NODE
		StepMeta sourceJoin=fromStep;
		
		if (filename != null && filename.size() > 0) {

			for( Iterator<String> filenameIt = filename.iterator(); filenameIt.hasNext();){
				String file = (String) filenameIt.next();
				log.info("**************** Process File:"+ file);
		
				// Load ktr
				TransMeta readktr = new TransMeta(file);
				// Get list of Hops
                for (int j = 0; j < readktr.nrTransHops(); j++) {
                	TransHopMeta thm = readktr.getTransHop(j);
                	transMeta.addTransHop(thm);
                }

                StepMeta sourceStepMeta = readktr.findStep("source");
				StepMeta targetStepMeta = readktr.findStep("target"); 
				
				
                List<StepMeta> steps = readktr.getTransHopSteps(true);				
				for (StepMeta temp : steps) {
					temp.setName(transMeta.getAlternativeStepname(temp.getName()));
					temp.setLocation(temp.getLocation().x,temp.getLocation().y * (1+filename.indexOf(file))*2);
					log.info("adding step:"+temp.getName());
					transMeta.addStep(temp);					
				}
				
				if(sourceStepMeta == null || targetStepMeta == null){
					log.info("\n\nNo Source or Target Step finded on file "+file);
				}
				 
				TransHopMeta fs = new TransHopMeta(sourceJoin, sourceStepMeta);
				log.info("Make hop: "+sourceJoin.getName() +"-"+ sourceStepMeta.getName());
				transMeta.addTransHop(fs);				
				sourceJoin = targetStepMeta;
				
			}
		}
			
		transMeta.writeXML("/tmp/output/" + source + "-" + target + "-debug.ktr");			
		
		log.info("Final Text");

		// Add Final text file output.
		String path = outputDir + target;
		TextFileField[] tf = null;
		TextFileOutputMeta fileOutput = new TextFileOutputMeta();
		fileOutput.setDefault();
		fileOutput.setFileName(path);
		fileOutput.setHeaderEnabled(true);
		fileOutput.setExtension("txt");
		fileOutput.setSeparator("|");
		fileOutput.setEnclosure("\"");
		fileOutput.setEnclosureForced(true);
		fileOutput.setFileCompression(TextFileOutputMeta.fileCompressionTypeCodes[TextFileOutputMeta.FILE_COMPRESSION_TYPE_NONE]);

		log.info("Select Values");
		// SelectValues
		// ADD this step to ensure all field on transformations exists
		// on target table.
		SelectValuesMeta selectMeta = new SelectValuesMeta();
		// Select Step.
		StepMeta selectStep = new StepMeta("select-" + target,
				(StepMetaInterface) selectMeta);
		selectStep.setName("select-" + target);
		selectStep.setLocation(x, y);
		selectStep.setDraw(true);
		x += xoffset;
		// Target Step.
		StepMeta toStep = new StepMeta(target, (StepMetaInterface) fileOutput);
		toStep.setLocation(0, 0);
		toStep.setDraw(true);
		

		// Add Required Steps to trans.
		
		transMeta.addStep(selectStep);
		transMeta.addStep(toStep);

		
		// Add a hop between the two steps...
		if (filename == null || filename.size() == 0) {
			// If no ktr file
			// workflow: [input]->[select]->[output]
			TransHopMeta fs = new TransHopMeta(sourceJoin, selectStep);
			transMeta.addTransHop(fs);
			TransHopMeta st = new TransHopMeta(selectStep, toStep);
			transMeta.addTransHop(st);

			selectMeta.allocate(selectFields.length, 0, 0);
			selectMeta.setSelectName(selectFields);

			tf = getFileFields(transMeta, sourceJoin, target);
			fileOutput.setOutputFields(tf);

		} else {
			// When any file specified.
			// Input -> [ktr files] -> select -> text output

			TransHopMeta ofs = new TransHopMeta(sourceJoin,selectStep);
			transMeta.addTransHop(ofs);
			log.info("Make hop: "+sourceJoin.getName() +"-"+ selectStep.getName());

			
			TransHopMeta st = new TransHopMeta(selectStep, toStep);
			log.info("Make hop: "+selectStep.getName() +"-"+ toStep.getName());
			transMeta.addTransHop(st);

			
			
			// If Transformation, then got the dummy output and look for
			// output fields from that step.
			
			log.info("Get previous Fields:"+sourceJoin.getName()); 
			fromFields = getPreviousFields(transMeta, toStep);
			log.info("Get Target FIelds"); 
			selectFields =  getTargetFields(fromFields, target);
			  
			log.info("allocate memory");
			selectMeta.allocate(selectFields.length, 0, 0);
			selectMeta.setSelectName(selectFields); 
			tf = getFileFields(transMeta, toStep, target);
			fileOutput.setOutputFields(tf);			

		}
		
		
		
		// Generate output file
		String[] f = new String[tf.length];
		for (int i = 0; i < tf.length; i++) {
			f[i] = "\"" + tf[i].getName() + "\"";
		}
		String result = StringUtils.join(f, ", ");

		upload += "COPY " + target + " ( " + result + ") from '" + path
				+ ".txt' with delimiter '|' CSV HEADER QUOTE '\"'; \n\n";

		transMeta.writeXML("/tmp/output/" + source + "-" + target + ".ktr");
		y += yoffset;

		return transMeta;

	}

	public static void init(String kettle_shared) throws KettleException {

		KettleEnvironment.init();

		// Load Shared Objects
		shared = new SharedObjects(kettle_shared);

		transMeta2 = new TransMeta();
		transMeta2.setName("trans");
		transMeta2.setSharedObjects(shared);
		transMeta2.readSharedObjects();
		transMeta2.setLogLevel(LogLevel.ROWLEVEL);
		transMeta2.setUsingUniqueConnections(true);

		// Instance to Execute Trans.
		trans = new Trans(transMeta2);

		// Database uses
		sourceDb = transMeta2.findDatabase("source");
		log.info("source databse:" + sourceDb);
		targetDb = transMeta2.findDatabase("target");
		log.info("target databse:" + targetDb);

	}

	public static void writeFile() {
		/*
		 * Write file with sql instructions to bulk load on target database.
		 * TODO: Make output file configuration.
		 */
		try {
			FileWriter fstream = new FileWriter("/tmp/output/copy.sql");
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(upload);
			// Close the output stream
			out.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public static void readConfig(String filename) throws Exception {
		/*
		 * Read config file to prepare migration steps Load transformation
		 * Execute transformations
		 */

		// Get Order to execute transformations
		List<TransMeta> executed = new ArrayList<TransMeta>();
		String[] execOrder = config.get("transformation_order",
				"transformation_order").split(",");

		// Create and Execute transformation in ORder:
		for (String table : execOrder) {

			log.info("\n\n\n************************************** Transformation:"
					+ table);

			// Get all ktr available for this table
			List<String> files = Arrays.asList(config.get(table,
					"transformation").split(","));

			String source = table;
			String target = table;

			if (config.options(table).contains("source")) {
				source = config.get(table, "source");
				log.info("SOURCE:" + source);
			}
			if (config.options(table).contains("target")) {
				target = config.get(table, "target");
				log.info("Target:" + target);
			}

			// Make transformations with all files that reference target table
			TransMeta transMeta = Migrate.makeTrans(source, target, files);
			transMeta.setLogLevel(LogLevel.ROWLEVEL);

			// If transfromation already executed, continue
			if (executed.contains(transMeta))
				continue;

			// Make transformation, execute and added to list of executed
			Trans t = new Trans(transMeta);
			transMetaMap.put(transMeta, t);
			t.execute(null);
			t.waitUntilFinished();
			executed.add(transMeta);
		}

	}


	public static void start(String migration_filename, String kettle_shared)
			throws Exception {

		log.info("Start Kettle process");
		Migrate.init(kettle_shared);
		Migrate.config.read(migration_filename);
		Migrate.readConfig(migration_filename);
		Migrate.writeFile();

	}

}
