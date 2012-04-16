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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.shared.SharedObjects;
import org.pentaho.di.trans.steps.tableinput.*;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.tableoutput.*;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.Job;
import java.lang.String;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.Result;

/**
 * 
 * @author angel
 */
public class Main {


    /**
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) {

        try {
        	String migration_filename = "migrate.json";
        	if(args.length == 1 ){
        		migration_filename = args[0];
        	}
        	String kettle_shared = "shared.xml";
        	if( args.length == 2){
        		kettle_shared = args[1];
        	}
        			
            KettleEnvironment.init( );
            Migrate.start(migration_filename, kettle_shared);

        } catch (Exception ex) {
            ex.printStackTrace();
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
