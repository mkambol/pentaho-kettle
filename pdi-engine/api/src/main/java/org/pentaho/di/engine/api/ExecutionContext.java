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

package org.pentaho.di.engine.api;

import com.google.common.collect.ImmutableMap;
import org.pentaho.di.engine.api.converter.RowConversionManager;
import org.pentaho.di.engine.api.model.Transformation;
import org.pentaho.di.engine.api.reporting.LogLevel;
import org.pentaho.di.engine.api.reporting.SubscriptionManager;

import java.io.Serializable;
import java.security.Principal;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Created by nbaker on 5/31/16.
 */
public interface ExecutionContext extends SubscriptionManager, HasConfig {

  /**
   * @deprecated use {@link #getConfig()}
   */
  @Deprecated
  default Map<String, Object> getParameters() {
    return ImmutableMap.copyOf( getConfig() );
  }

  /**
   * @deprecated use {@link #getConfig()}
   */
  @Deprecated default Map<String, Object> getEnvironment() {
    return ImmutableMap.copyOf( getConfig() );
  }

  /**
   * @deprecated use {@link #setConfig(Map)}
   */
  @Deprecated
  default void setParameters( Map<String, Object> parameters ) {
    parameters.forEach( this::setParameter );
  }

  /**
   * @deprecated use {@link #setConfig(Map)}
   */
  @Deprecated
  default void setEnvironment( Map<String, Object> environment ) {
    environment.forEach( this::setEnvironment );
  }

  /**
   * @deprecated use {@link #setConfig(String, Serializable)}
   */
  @Deprecated
  default void setParameter( String key, Object value ) {
    setEnvironment( key, value );
  }

  /**
   * @deprecated use {@link #setConfig(String, Serializable)}
   */
  @Deprecated default void setEnvironment( String key, Object value ) {
    if ( value instanceof Serializable ) {
      setConfig( key, (Serializable) value );
    }
  }

  Transformation getTransformation();

  CompletableFuture<ExecutionResult> execute();

  @Deprecated default RowConversionManager getConversionManager() {
    return null;
  }

  Principal getActingPrincipal();

  void setActingPrincipal( Principal actingPrincipal );

  void setLoggingLogLevel( LogLevel logLevel );

  LogLevel getLoggingLogLevel();
}
