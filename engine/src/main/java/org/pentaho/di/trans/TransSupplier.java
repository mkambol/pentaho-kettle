package org.pentaho.di.trans;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.EnginePluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.engine.api.Engine;
import org.pentaho.di.trans.ael.adapters.TransEngineAdapter;
import org.pentaho.di.trans.ael.websocket.TransWebSocketEngineAdapter;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class TransSupplier implements Supplier<Trans> {

  private final TransMeta transMeta;
  private final LogChannelInterface log;
  private final Supplier<Trans> fallbackSupplier;

  public TransSupplier( TransMeta transMeta, LogChannelInterface log, Supplier<Trans> fallbackSupplier ) {
    this.transMeta = transMeta;
    this.log = log;
    this.fallbackSupplier = fallbackSupplier;
  }

  public Trans get() {
    if ( Utils.isEmpty( transMeta.getVariable( "engine" ) ) ) {
      log.logBasic( "Using legacy execution engine" );
      return fallbackSupplier.get();
    }
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );
    //default for now is AEL Engine RSA
    String version = variables.getVariable( "KETTLE_AEL_PDI_DAEMON_VERSION", "1.0" );
    if ( Const.toDouble( version, 1 ) >= 2 ) {
      String host = transMeta.getVariable( "engine.host" );
      String port = transMeta.getVariable( "engine.port" );
      boolean ssl = false;
      return new TransWebSocketEngineAdapter( transMeta, host, port, ssl );
    } else {
      try {
        return PluginRegistry.getInstance().getPlugins( EnginePluginType.class ).stream()
          .filter( useThisEngine() )
          .findFirst()
          .map( plugin -> (Engine) loadPlugin( plugin ) )
          .map( engine -> {
            log.logBasic( "Using execution engine " + engine.getClass().getCanonicalName() );
            return (Trans) new TransEngineAdapter( engine, transMeta );
          } )
          .orElseThrow(
            () -> new KettleException( "Unable to find engine [" + transMeta.getVariable( "engine" ) + "]" ) );
      } catch ( KettleException e ) {
        log.logError( "Failed to load engine", e );
        throw new RuntimeException( e );
      }

    }
  }

  /**
   * Uses a trans variable called "engine" to determine which engine to use.
   */
  private Predicate<PluginInterface> useThisEngine() {
    return plugin  -> Arrays.stream( plugin.getIds() )
      .filter( id -> id.equals( ( transMeta.getVariable( "engine" ) ) ) )
      .findAny()
      .isPresent();
  }


  private Object loadPlugin( PluginInterface plugin ) {
    try {
      return PluginRegistry.getInstance().loadClass( plugin );
    } catch ( KettlePluginException e ) {
      throw new RuntimeException( e );
    }
  }
}
