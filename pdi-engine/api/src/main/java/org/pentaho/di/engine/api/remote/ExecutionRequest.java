/*
 * ! ******************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 * ******************************************************************************
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * *****************************************************************************
 */

package org.pentaho.di.engine.api.remote;

import com.google.common.collect.ImmutableMap;
import org.pentaho.di.engine.api.ExecutionContext;
import org.pentaho.di.engine.api.model.Configurable;
import org.pentaho.di.engine.api.model.Transformation;
import org.pentaho.di.engine.api.reporting.LogEntry;
import org.pentaho.di.engine.api.reporting.LogLevel;

import java.io.Serializable;
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A request for execution by a remote Engine. All fields should be Serializable.
 * <p>
 * Created by hudak on 1/25/17.
 */
public final class ExecutionRequest extends Configurable implements Serializable {
  private static final long serialVersionUID = -7835121168360407191L;
  private final Transformation transformation;
  private final Map<String, Set<Class<? extends Serializable>>> reportingTopics;
  private final Principal actingPrincipal;
  private final LogLevel loggingLogLevel;

  @Deprecated
  public ExecutionRequest( Map<String, Object> parameters, Map<String, Object> environment,
                           Transformation transformation,
                           Map<String, Set<Class<? extends Serializable>>> reportingTopics,
                           LogLevel loggingLogLevel,
                           Principal actingPrincipal ) {
    this( transformation, reportingTopics, actingPrincipal, loggingLogLevel );

    // Copy environment and parameters into config
    Stream.of( environment, parameters ).flatMap( map -> map.entrySet().stream() ).forEach( entry -> {
      Object value = entry.getValue();
      if ( value instanceof Serializable ) {
        setConfig( entry.getKey(), (Serializable) value );
      }
    } );
  }

  public ExecutionRequest( ExecutionContext context, Map<String, Set<Class<? extends Serializable>>> reportingTopics ) {
    this( context.getTransformation(), reportingTopics, context.getActingPrincipal(), context.getLoggingLogLevel() );

    // Copy configuration
    setConfig( context.getConfig() );
  }

  public ExecutionRequest( Transformation transformation,
                           Map<String, Set<Class<? extends Serializable>>> reportingTopics,
                           Principal actingPrincipal, LogLevel loggingLogLevel ) {
    this.transformation = transformation;
    this.reportingTopics = reportingTopics;
    this.loggingLogLevel = loggingLogLevel;
    this.actingPrincipal = actingPrincipal;
  }

  /**
   * @deprecated use {@link #getConfig()}
   */
  @Deprecated
  public Map<String, Object> getParameters() {
    return ImmutableMap.copyOf( getConfig() );
  }

  /**
   * @deprecated use {@link #getConfig()}
   */
  @Deprecated
  public Map<String, Object> getEnvironment() {
    return ImmutableMap.copyOf( getConfig() );
  }

  public Transformation getTransformation() {
    return transformation;
  }

  public Map<String, Set<Class<? extends Serializable>>> getReportingTopics() {
    return reportingTopics;
  }

  public Principal getActingPrincipal() {
    return actingPrincipal;
  }

  public LogLevel getLoggingLogLevel() {
    return loggingLogLevel;
  }
}
