/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.pentaho.di.trans.step.filestream;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleMissingPluginsException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.SubtransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorData;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorParameters;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Describe your step plugin.
 */
public class FileStream extends BaseStreamStep<List<String>> implements StepInterface {

  private static Class<?> PKG = FileStreamMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private FileStreamMeta fileStreamMeta;
  private SubtransExecutor subtransExecutor;

  public FileStream( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                     Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    fileStreamMeta = (FileStreamMeta) stepMetaInterface;

    if (fileStreamMeta == null || fileStreamMeta.getTransformationPath() == null) {
      return super.init( stepMetaInterface, stepDataInterface ); // TODO fix hackiness
    }
    try {
      subtransExecutor = new SubtransExecutor(
        getTrans(), new TransMeta( fileStreamMeta.getTransformationPath() ), true,
        new TransExecutorData(), new TransExecutorParameters() );
    } catch ( KettleXMLException | KettleMissingPluginsException e ) {
      e.printStackTrace();
    }
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "foo" ) );

    window = new FixedTimeStreamWindow<>( subtransExecutor, rowMeta, 1000, 5 );
    try {
      source = new TailFileStreamSource( fileStreamMeta.getSourcePath() );
    } catch ( FileNotFoundException e ) {
      // TODO log, error
      e.printStackTrace();
    }
    return super.init( stepMetaInterface, stepDataInterface );
  }

  @Override public Collection<StepStatus> subStatuses() {
    return subtransExecutor != null ? subtransExecutor.getStatuses().values() : Collections.emptyList();
  }

//  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
//    Object[] r = getRow(); // get row, set busy!
//    if ( r == null ) {
//      // no more input to be expected...
//      setOutputDone();
//      return false;
//    }
//
//    putRow( getInputRowMeta(), r ); // copy row to possible alternate rowset(s).
//
//    if ( checkFeedback( getLinesRead() ) ) {
//      if ( log.isBasic() ) {
//        logBasic( BaseMessages.getString( PKG, "FileStream.Log.LineNumber" ) + getLinesRead() );
//      }
//    }
//
//    return true;
//  }
}