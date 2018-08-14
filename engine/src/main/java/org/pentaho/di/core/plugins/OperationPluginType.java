package org.pentaho.di.core.plugins;

import org.pentaho.di.core.annotations.OperationPlugin;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.engine.model.Operation;

import java.lang.annotation.Annotation;
import java.util.Map;

@PluginMainClassType ( Operation.class )
@PluginAnnotationType ( OperationPlugin.class )
public class OperationPluginType extends BasePluginType {

  private static final OperationPluginType instance = new OperationPluginType();

  public static OperationPluginType getInstance() {
    return instance;
  }

  private OperationPluginType() {
    super( OperationPlugin.class, "OPERATION_PLUGIN", "AEL Operation Plugin" );
  }

  @Override protected void registerXmlPlugins() throws KettlePluginException {

  }

  @Override protected String extractID( Annotation annotation ) {
    return null;
  }

  @Override protected String extractName( Annotation annotation ) {
    return null;
  }

  @Override protected String extractDesc( Annotation annotation ) {
    return null;
  }

  @Override protected String extractCategory( Annotation annotation ) {
    return null;
  }

  @Override protected String extractImageFile( Annotation annotation ) {
    return null;
  }

  @Override protected boolean extractSeparateClassLoader( Annotation annotation ) {
    return false;
  }

  @Override protected String extractI18nPackageName( Annotation annotation ) {
    return null;
  }

  @Override protected String extractDocumentationUrl( Annotation annotation ) {
    return null;
  }

  @Override protected String extractSuggestion( Annotation annotation ) {
    return null;
  }

  @Override protected String extractCasesUrl( Annotation annotation ) {
    return null;
  }

  @Override protected String extractForumUrl( Annotation annotation ) {
    return null;
  }

  @Override protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, Annotation annotation ) {

  }


}
