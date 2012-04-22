/*
 Copyright (c) 2011 NaN Projectes de Programari Lliure, S.L. All Rights Reserved.
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
import java.io.File;
import java.io.FileWriter;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.TransMeta;

import org.pentaho.di.core.database.*;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.steps.tableinput.*;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.textfileoutput.*;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.job.Job;
import java.lang.String;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.logging.*;

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
    static List<Trans> transList = new ArrayList<Trans>();
    static String outputDir = "/tmp/output/";
    static HashMap<TransMeta, List<String>> dependencies = new HashMap<TransMeta, List<String>>();
    static HashMap<String, TransMeta> transMap = new HashMap<String, TransMeta>();
    static HashMap<TransMeta, Trans> transMetaMap = new HashMap<TransMeta, Trans>();

    static private Logger log = Logger.getLogger(Migrate.class + "");

    public static List<String> getPreviousFields(TransMeta transMeta,
	    StepMeta step) throws KettleStepException {
	// Get fields input from step.
	List<String> fieldList = new ArrayList<String>();

	RowMetaInterface rmi = transMeta.getStepFields(step);
	String[] fields = rmi.getFieldNames();
	for (int i = 0; i < fields.length; i++) {
	    fieldList.add(fields[i]);
	}

	TextFileField[] t = new TextFileField[fields.length];
	for (int i = 0; i < fields.length; i++) {
	    t[i] = new TextFileField(fields[i], rmi.getValueMeta(i)
		    .getOriginalColumnType(), new String(), -1, -1,
		    new String(), new String(), new String(), new String());
	}

	return fieldList;

    }

    public static String[] getTargetFields(List<String> previousFields,
	    String targetTable) throws Exception {
	// Get fields from table output.

//	log.info("Getting target Fields from table: " + targetTable);

	Database targetDatabase = new Database(targetDb);
	targetDatabase.connect();

//	log.info("after connect");

	String[] targetFields = targetDatabase.getQueryFields(
		"select * from " + targetTable, true).getFieldNames();

//	log.info("Target fields:" + targetFields.toString());

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

    public static StepMeta findSource(TransMeta transMeta) {
	List<StepMeta> hopsteps = transMeta.getTransHopSteps(false);
	for (Iterator it = hopsteps.iterator(); it.hasNext();) {
	    StepMeta step = (StepMeta) it.next();
	    if (step.getTypeId().equals("Dummy")) {
		String[] names = transMeta.getPrevStepNames(step.getName());
		if (names.length == 0) {
		    return step;
		}
	    }
	}
	return null;
    }

    public static StepMeta findTarget(TransMeta transMeta) {
	List<StepMeta> hopsteps = transMeta.getTransHopSteps(false);
	Iterator it = hopsteps.iterator();
	while (it.hasNext()) {
	    StepMeta step = (StepMeta) it.next();
	    if (step == null)
		continue;
	    if (step.getTypeId().equals("Dummy")) {
		String[] names = transMeta.getNextStepNames(step);
		if (names.length == 0) {
		    return step;
		}
	    }
	}
	return null;
    }

    public static TransMeta makeTrans(String source, String target,
	    List<String> filename) throws Exception {

	// Prepare Transformation, load kettle shared file and make
	// available to transformation.
	TransMeta transMeta = new TransMeta();
	transMeta.setName(source + "_" + target);
	transMeta.setSharedObjects(shared);
	transMeta.readSharedObjects();
//	transMeta.setLogLevel(LogLevel.ROWLEVEL);
	transMeta.setUsingUniqueConnections(true);

	transMap.put(target, transMeta);

	int x = 100;

	// Source Input data.
	TableInputMeta tii = new TableInputMeta();
	tii.setDatabaseMeta(sourceDb);
	String selectSQL = "SELECT * FROM " + source;
	tii.setSQL(selectSQL);

//	log.info("Input Query SQL:" + selectSQL);

	// Source step.
	StepMeta fromStep = new StepMeta("table_source_" + source,
		(StepMetaInterface) tii);
	fromStep.setLocation(x, y);
	fromStep.setDraw(true);

	List<String> fromFields = getPreviousFields(transMeta, fromStep);

//	log.info("Previous fields:" + fromFields.toString());
//	log.info("Get target fields table:" + target);

	String[] selectFields = getTargetFields(fromFields, target);

	x += xoffset;

	System.out.println("*************************:   " + filename);
	// LOAD TRANS NODE
	if (filename != null && filename.size() > 0) {
	    List<StepMeta> join = new ArrayList<StepMeta>();
	    List<String> depends = new ArrayList<String>();
	    for (Iterator<String> it = filename.iterator(); it.hasNext();) {
		String file = (String) it.next();
		TransMeta read = new TransMeta(file);
		List<StepMeta> s = read.getTransHopSteps(true);
		StepMeta jsource = null;
		StepMeta jtarget = null;
		for (Iterator<StepMeta> it2 = s.iterator(); it2.hasNext();) {
		    StepMeta sp = it2.next();
		    sp.setName(transMeta.getAlternativeStepname(sp.getName()));

//		    log.info("Step:"+ sp.getName());
		    
		    if (sp.getName().contains("target"))
			jtarget = sp;
 		    if (sp.getName().contains("source")) {
// 			log.info( sp.getStepID().toString() );
 			if( ! sp.getStepID().toString().equals("Dummy")){
 			    fromStep = sp;
 			    continue;
 			}
 			if (filename.indexOf(file) > 0) {
 			    jsource = sp;
 			}
		    }	
		    transMeta.addStep(sp);
		}
		if (jsource != null && filename.indexOf(file) > 0)
		    join.add(jsource);
		if (jtarget != null)
		    join.add(jtarget);

		for (int j = 0; j < read.nrTransHops(); j++) {
		    TransHopMeta thm = read.getTransHop(j);
//		    log.info(  thm.getFromStep().getName() +"-" + thm.getToStep().getName() ); 
		    transMeta.addTransHop(thm);
		}

		for (Iterator<StepMeta> it3 = join.iterator(); it3.hasNext();) {

		    StepMeta joinSource = (StepMeta) it3.next();
		    
		    if (!it3.hasNext())
			break;
		    StepMeta joinTarget = (StepMeta) it3.next();
		    TransHopMeta fs = new TransHopMeta(joinSource, joinTarget);
		    transMeta.addTransHop(fs);
		}
	    }
	}

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
	fileOutput
		.setFileCompression(TextFileOutputMeta.fileCompressionTypeCodes[TextFileOutputMeta.FILE_COMPRESSION_TYPE_NONE]);

	// SelectValues
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
	toStep.setLocation(x, y);
	toStep.setDraw(true);

	// Add Steps to trans.
	transMeta.addStep(fromStep);
	transMeta.addStep(selectStep);
	transMeta.addStep(toStep);

	// Add a hop between the two steps...
	if (filename == null || filename.size() == 0) {
	    TransHopMeta fs = new TransHopMeta(fromStep, selectStep);
	    transMeta.addTransHop(fs);
	    TransHopMeta st = new TransHopMeta(selectStep, toStep);
	    transMeta.addTransHop(st);

	    selectMeta.allocate(selectFields.length, 0, 0);
	    selectMeta.setSelectName(selectFields);

	    tf = getFileFields(transMeta, fromStep, target);
	    fileOutput.setOutputFields(tf);

	} else {	    
	    System.out.println("0:" + transMeta.getStep(0).getName());
	    System.out.println("1:" + transMeta.getStep(1).getName());

	    StepMeta input_connect_step = findSource(transMeta);
	    if( input_connect_step != null ){
		TransHopMeta ifs = new TransHopMeta(fromStep, input_connect_step);
	    	transMeta.addTransHop(ifs);
	    }

	    StepMeta output_connect_step = findTarget(transMeta);
	    if (output_connect_step == null)
		return transMeta;

	    TransHopMeta ofs = new TransHopMeta(output_connect_step, selectStep);
	    transMeta.addTransHop(ofs);
	    TransHopMeta st = new TransHopMeta(selectStep, toStep);
	    transMeta.addTransHop(st);

	    // If Transformation, then got the dummy output and look for
	    // output fields from that step.
	    fromFields.clear();
	    fromFields = getPreviousFields(transMeta, output_connect_step);
	    selectFields = getTargetFields(fromFields, target);
	    selectMeta.allocate(selectFields.length, 0, 0);
	    selectMeta.setSelectName(selectFields);

	    tf = getFileFields(transMeta, output_connect_step, target);
	    fileOutput.setOutputFields(tf);
	}

	String[] f = new String[tf.length];
	for (int i = 0; i < tf.length; i++) {
	    f[i] = tf[i].getName();
	}
	String result = StringUtils.join(f, ", ");

	System.out.println(target);
	System.out.println(result);
	upload += "COPY " + target + " ( " + result + ") from '" + path
		+ ".txt' with delimiter '|' CSV HEADER QUOTE '\"'; \n\n";

	transMeta.writeXML("/tmp/" + source + "-" + target + ".ktr");
	y += yoffset;

	return transMeta;

    }

    public static void init(String kettle_shared) throws KettleException {
	KettleEnvironment.init();

	shared = new SharedObjects(kettle_shared);

	transMeta2 = new TransMeta();
	transMeta2.setName("trans");
	transMeta2.setSharedObjects(shared);
	transMeta2.readSharedObjects();
	//transMeta2.setLogLevel(LogLevel.ROWLEVEL);
	transMeta2.setLogLevel(LogLevel.BASIC);
	transMeta2.setUsingUniqueConnections(true);

	// Instance to Execute Trans.
	trans = new Trans(transMeta2);

	// Database uses
	// shared.getObjectsMap().get()
	sourceDb = transMeta2.findDatabase("source");
//	log.info("source databse:" + sourceDb);
	targetDb = transMeta2.findDatabase("target");
//	log.info("target databse:" + targetDb);
	// upload += "BEGIN TRANSACTION;\n\n";
	// upload += "SET CONSTRAINTS ALL DEFERRED; \n\n";

    }

    public static void writeFile() {
	try {
	    FileWriter fstream = new FileWriter("/tmp/output_copy.sql");
	    BufferedWriter out = new BufferedWriter(fstream);
	    out.write(upload);
	    // Close the output stream
	    out.close();
	} catch (Exception e) {
	    System.err.println("Error: " + e.getMessage());
	}
    }

    public static void readConfig(String filename) throws Exception {

	ObjectMapper mapper = new ObjectMapper();
	Map<String, HashMap> mp = mapper.readValue(new File(filename),
		Map.class);

	List<TransMeta> executed = new ArrayList<TransMeta>();

	List<String> transformationOrder = (List<String>) mp
		.get("transformation_order");
	Iterator orderIt = transformationOrder.iterator();
	while (orderIt.hasNext()) {
	    String transformation = (String) orderIt.next();
	    System.out.println(transformation);
	    HashMap h = (HashMap) mp.get(transformation);
	    List<String> tranFile = (List<String>) h.get("transformation");
	    List<String> executeFile = (List<String>) h.get("execute");
	    TransMeta transMeta;
	    if (executeFile == null || executeFile.isEmpty())
		transMeta = Migrate.makeTrans(transformation, transformation,
			tranFile);

	    else {
		for (Iterator<String> it = executeFile.iterator(); it.hasNext();) {
		    String file = (String) it.next();
		    TransMeta read = new TransMeta(file);

		    if (executed.contains(read))
			continue;
		    Trans t2 = new Trans(read);
		    transList.add(t2);
		    transMetaMap.put(read, t2);

		    t2.execute(null);
		    t2.waitUntilFinished();
		    executed.add(read);

		}
		continue;
	    }
	    if (executed.contains(transMeta))
		continue;
	    Trans t = new Trans(transMeta);
	    transList.add(t);
	    transMetaMap.put(transMeta, t);

	    t.execute(null);
	    t.waitUntilFinished();
	    executed.add(transMeta);

	}

    }

    public static List<TransMeta> executeTrans(TransMeta tm,
	    List<TransMeta> executed) throws Exception {

	Trans t = (Trans) transMetaMap.get(tm);
	List<String> depends = dependencies.get(tm);
	Iterator<String> it = (Iterator) depends.iterator();
	while (it.hasNext()) {
	    String dependency = (String) it.next();
	    TransMeta tmDepend = (TransMeta) dependencies.get(dependency);
	    if (executed.contains(tmDepend))
		continue;
	    List<TransMeta> tmExecuted = executeTrans(tmDepend, executed);

	    Trans td = (Trans) transMetaMap.get(tmDepend);
	    td.execute(null);
	    td.waitUntilFinished();
	    executed.addAll(tmExecuted);
	}

	t.execute(null);
	t.waitUntilFinished();
	return executed;
    }

    public static void start(String migration_filename, String kettle_shared)
	    throws Exception {

	ConsoleHandler handler = new ConsoleHandler();
	log.addHandler(handler);
	log.info("Start Kettle process");
	Migrate.init(kettle_shared);
	Migrate.readConfig(migration_filename);
	Migrate.writeFile();
	System.out.println("FINISH");

    }

}
