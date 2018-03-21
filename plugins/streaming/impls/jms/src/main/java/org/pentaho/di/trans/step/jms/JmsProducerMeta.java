/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.step.jms;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.GenericStepData;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.streaming.common.BaseStreamStepMeta;

import static org.pentaho.di.core.ObjectLocationSpecificationMethod.FILENAME;
//TODO: this class needs implemented
@InjectionSupported ( localizationPrefix = "JmsProducerMeta.Injection." )
//TODO: change image to JMSP.svg - don't know where to find this
@Step ( id = "JmsProducer2", image = "JMSC.svg", name = "JMS Producer2.0",
  description = "JmsProducerDialog.TypeLongDesc", categoryDescription = "Streaming" )
public class JmsProducerMeta extends BaseStepMeta implements StepMetaInterface, Cloneable {

  @InjectionDeep
  public final JmsDelegate jmsDelegate;



  public JmsProducerMeta( JmsDelegate jmsDelegate ) {
    this.jmsDelegate = jmsDelegate;
  }

  @SuppressWarnings( "deprecated" )
  public String getDialogClassName() {
    return "org.pentaho.di.trans.step.jms.ui.JmsProducerDialog";
  }

  @Override
  public void setDefault() {
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                                Trans trans ) {
    return new JmsProducer( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new GenericStepData();
  }
}
