/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.step;

import com.google.common.base.Preconditions;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.resource.ResourceReference;
import org.pentaho.di.trans.StepWithMappingMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseStreamingMeta extends StepWithMappingMeta implements StepMetaInterface {
  public static final String CLUSTER_NAME = "clusterName";
  public static final String CONSUMER_GROUP = "consumerGroup";
  public static final String TRANSFORMATION_PATH = "transformationPath";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION = "batchDuration";
  public static final String ADVANCED_CONFIG = "advancedConfig";
  public static final String CONFIG_OPTION = "option";
  public static final String OPTION_PROPERTY = "property";
  public static final String OPTION_VALUE = "value";

  private static Class<?> PKG = BaseStreamingMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  @Injection( name = "TRANSFORMATION_PATH" )
  private String transformationPath;

  @Injection( name = "NUM_MESSAGES" )
  private String batchSize;

  @Injection( name = "DURATION" )
  private String batchDuration;

  @Injection( name = "NAMES", group = "CONFIGURATION_PROPERTIES" )
  protected transient List<String> injectedConfigNames;

  @Injection( name = "VALUES", group = "CONFIGURATION_PROPERTIES" )
  protected transient List<String> injectedConfigValues;

  private Map<String, String> config = new LinkedHashMap<>();

  public BaseStreamingMeta() {
    super(); // allocate BaseStepMeta
    setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData( Node stepnode ) {
    setTransformationPath( XMLHandler.getTagValue( stepnode, TRANSFORMATION_PATH ) );
    setFileName( XMLHandler.getTagValue( stepnode, TRANSFORMATION_PATH ) );
    setBatchSize( XMLHandler.getTagValue( stepnode, BATCH_SIZE ) );
    setBatchDuration( XMLHandler.getTagValue( stepnode, BATCH_DURATION ) );

    config = new LinkedHashMap<>();

    Optional.ofNullable( XMLHandler.getSubNode( stepnode, ADVANCED_CONFIG ) ).map( node -> node.getChildNodes() )
      .ifPresent( nodes -> IntStream.range( 0, nodes.getLength() ).mapToObj( nodes::item )
        .filter( node -> node.getNodeType() == Node.ELEMENT_NODE )
        .forEach( node -> {
          if ( CONFIG_OPTION.equals( node.getNodeName() ) ) {
            config.put( node.getAttributes().getNamedItem( OPTION_PROPERTY ).getTextContent(),
              node.getAttributes().getNamedItem( OPTION_VALUE ).getTextContent() );
          } else {
            config.put( node.getNodeName(), node.getTextContent() );
          }
        } ) );
  }

  public void setDefault() {
    batchSize = "1000";
    batchDuration = "1000";
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {

    setTransformationPath( rep.getStepAttributeString( id_step, TRANSFORMATION_PATH ) );
    setFileName( rep.getStepAttributeString( id_step, TRANSFORMATION_PATH ) );
    setBatchSize( rep.getStepAttributeString( id_step, BATCH_SIZE ) );
    setBatchDuration( rep.getStepAttributeString( id_step, BATCH_DURATION ) );

    config = new LinkedHashMap<>();

    for ( int i = 0; i < rep.getStepAttributeInteger( id_step, ADVANCED_CONFIG + "_COUNT" ); i++ ) {
      config.put( rep.getStepAttributeString( id_step, i, ADVANCED_CONFIG + "_NAME" ),
          rep.getStepAttributeString( id_step, i, ADVANCED_CONFIG + "_VALUE" ) );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId )
    throws KettleException {

    rep.saveStepAttribute( transId, stepId, TRANSFORMATION_PATH, transformationPath );
    rep.saveStepAttribute( transId, stepId, BATCH_SIZE, batchSize );
    rep.saveStepAttribute( transId, stepId, BATCH_DURATION, batchDuration );

    rep.saveStepAttribute( transId, stepId, ADVANCED_CONFIG + "_COUNT", getConfig().size() );

    int i = 0;
    for ( String propName : getConfig().keySet() ) {
      rep.saveStepAttribute( transId, stepId, i, ADVANCED_CONFIG + "_NAME", propName );
      rep.saveStepAttribute( transId, stepId, i++, ADVANCED_CONFIG + "_VALUE", getConfig().get( propName ) );
    }
  }

  public abstract void getFields(
    RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
    Repository repository, IMetaStore metaStore ) throws KettleStepException;

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
                     StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
                     RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    long duration = Long.MIN_VALUE;
    try {
      duration = Long.parseLong( space.environmentSubstitute( getBatchDuration() ) );
    } catch ( NumberFormatException e ) {
      remarks.add( new CheckResult(
        CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.CheckResult.NaN", "Duration" ),
        stepMeta ) );
    }

    long size = Long.MIN_VALUE;
    try {
      size = Long.parseLong( space.environmentSubstitute( getBatchSize() ) );
    } catch ( NumberFormatException e ) {
      remarks.add( new CheckResult(
        CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.CheckResult.NaN", "Number of records" ),
        stepMeta ) );
    }

    if ( duration == 0 && size == 0 ) {
      remarks.add( new CheckResult(
        CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.CheckResult.NoBatchDefined" ),
        stepMeta ) );
    }
  }

  public abstract StepInterface getStep(
    StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans );

  public abstract StepDataInterface getStepData();

  public abstract String getDialogClassName();

  public String getTransformationPath() {
    return transformationPath;
  }

  public String getBatchSize() {
    return batchSize;
  }

  public String getBatchDuration() {
    return batchDuration;
  }


  public void setTransformationPath( String transformationPath ) {
    this.transformationPath = transformationPath;
  }

  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  public void setBatchDuration( String batchDuration ) {
    this.batchDuration = batchDuration;
  }

  @Override public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " ).append( XMLHandler.addTagValue( TRANSFORMATION_PATH, transformationPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( BATCH_SIZE, batchSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( BATCH_DURATION, batchDuration ) );

    getFieldDefinitions().forEach( field ->
      retval.append( "    " ).append(
        XMLHandler.addTagValue( OUTPUT_FIELD_TAG_NAME, field.getOutputName(), true,
          KAFKA_NAME_ATTRIBUTE, field.getKafkaName().toString(),
          TYPE_ATTRIBUTE, field.getOutputType().toString() ) ) );

    retval.append( "    " ).append( XMLHandler.openTag( ADVANCED_CONFIG ) ).append( Const.CR );
    getConfig().forEach( ( key, value ) -> retval.append( "        " )
        .append( XMLHandler.addTagValue( CONFIG_OPTION, "", true,
                              OPTION_PROPERTY, (String) key, OPTION_VALUE, (String) value ) ) );
    retval.append( "    " ).append( XMLHandler.closeTag( ADVANCED_CONFIG ) ).append( Const.CR );

    return retval.toString();
  }

  public void setConfig( Map<String, String> config ) {
    this.config = config;
  }

  public Map<String, String> getConfig() {
    applyInjectedProperties();
    return config;
  }

  @Override
  public List<ResourceReference> getResourceDependencies( TransMeta transMeta, StepMeta stepInfo ) {
    List<ResourceReference> references = new ArrayList<ResourceReference>( 5 );
    String realFilename = transMeta.environmentSubstitute( transformationPath );
    ResourceReference reference = new ResourceReference( stepInfo );
    references.add( reference );

    if ( !Utils.isEmpty( realFilename ) ) {
      // Add the filename to the references, including a reference to this step
      // meta data.
      //
      reference.getEntries().add( new ResourceEntry( realFilename, ResourceEntry.ResourceType.ACTIONFILE ) );
    }

    return references;
  }

  @Override public String[] getReferencedObjectDescriptions() {
    return new String[] {
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.ReferencedObject.SubTrans.Description" ) };
  }

  @Override public boolean[] isReferencedObjectEnabled() {
    return new boolean[] { !Utils.isEmpty( transformationPath ) };
  }

  @Override public Object loadReferencedObject( int index, Repository rep, IMetaStore metaStore, VariableSpace space )
      throws KettleException {
    return loadMappingMeta( this, rep, metaStore, space );
  }

  protected void applyInjectedProperties() {
    if ( injectedConfigNames != null || injectedConfigValues != null ) {
      Preconditions.checkState( injectedConfigNames != null, "Options names were not injected" );
      Preconditions.checkState( injectedConfigValues != null, "Options values were not injected" );
      Preconditions.checkState( injectedConfigNames.size() == injectedConfigValues.size(),
          "Injected different number of options names and value" );

      setConfig( IntStream.range( 0, injectedConfigNames.size() ).boxed().collect( Collectors
          .toMap( injectedConfigNames::get, injectedConfigValues::get, ( v1, v2 ) -> v1,
              LinkedHashMap::new ) ) );

      injectedConfigNames = null;
      injectedConfigValues = null;
    }
  }
}
