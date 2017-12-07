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

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.steps.BaseStreamingDialog;

public class FileStreamDialog extends BaseStreamingDialog implements StepDialogInterface {

  private static Class<?> PKG = FileStreamMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private FileStreamMeta meta;

  protected Label wlSourcePath;
  protected TextVar wSourcePath;
  protected Button wbBrowseSource;

  public FileStreamDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (FileStreamMeta) in;
  }

  @Override protected void buildSetup( Composite wSetupComp ) {
    wlSourcePath = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlSourcePath );
    wlSourcePath.setText( BaseMessages.getString( PKG, "FileStreamDialog.Transformation" ) );
    FormData fdlTransPath = new FormData();
    fdlTransPath.left = new FormAttachment( 0, 0 );
    fdlTransPath.top = new FormAttachment( 0, 0 );
    fdlTransPath.right = new FormAttachment( 50, 0 );
    wlSourcePath.setLayoutData( fdlTransPath );

    wSourcePath = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSourcePath );
    wSourcePath.addModifyListener( lsMod );
    FormData fdTransPath = new FormData();
    fdTransPath.left = new FormAttachment( 0, 0 );
    fdTransPath.right = new FormAttachment( 75, 0 );
    fdTransPath.top = new FormAttachment( wlSourcePath, 5 );
    wSourcePath.setLayoutData( fdTransPath );

    wbBrowseSource = new Button( wSetupComp, SWT.PUSH );
    props.setLook( wbBrowseSource );
    wbBrowseSource.setText( BaseMessages.getString( PKG, "FileStreamDialog.Transformation.Browse" ) );
    FormData fdBrowseTrans = new FormData();
    fdBrowseTrans.left = new FormAttachment( wSourcePath, 5 );
    fdBrowseTrans.top = new FormAttachment( wlSourcePath, 5 );
    wbBrowseSource.setLayoutData( fdBrowseTrans );

    wbBrowseSource.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        selectFileTrans( wSourcePath, new String[]{"*"} );
      }
    } );
  }

  @Override protected void getData() {
    super.getData();
    if ( meta.getSourcePath() != null ) {
      wSourcePath.setText( meta.getSourcePath() );
    }
  }
}
