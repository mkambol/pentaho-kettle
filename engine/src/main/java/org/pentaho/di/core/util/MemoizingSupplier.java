package org.pentaho.di.core.util;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implementation of a memoizing supplier with defined expiration time, and a hook to call cleanup code on expiration.
 * <p>
 * This wraps the guava LoadingCache, which has almost all required functionality, but will only handle expiration on
 * reads and writes, which means cleanup may never get called if an object is no longer used.
 *
 * @param <T>
 */
@SuppressWarnings( "UnstableApiUsage" )
// the Cache api we use is unchanged with guava 19+, no longer beta.
public class MemoizingSupplier<T> implements Supplier<T> {
  private final LoadingCache<Object, T> loadingCache;
  private static final Object KEY = new Object(); // since this is a cache for 1 object, use a fixed key.

  private final Function<T, Void> onExpire;

  // holds the expiration thread.
  private static final ScheduledExecutorService cleanupExecutor =
    Executors.newScheduledThreadPool( 1 );

  // future associated with the fixed rate polling
  private ScheduledFuture<?> scheduledFuture;

  private static final LogChannelInterface LOGGER =
    KettleLogStore.getLogChannelInterfaceFactory().create( MemoizingSupplier.class );

  /**
   * @param delegate         the Supplier of the raw value, which will be loaded and cached.
   * @param expirationMillis Milliseconds (after access) till expiration.  Renewed with each read.
   * @param onExpire         The function to invoke on expiration.
   */
  public MemoizingSupplier( Supplier<T> delegate, long expirationMillis, Function<T, Void> onExpire ) {
    this.onExpire = onExpire;
    loadingCache = CacheBuilder.newBuilder()
      .expireAfterAccess( expirationMillis, MILLISECONDS )
      .<Object, T>removalListener( listener -> onExpire( listener.getValue() ) )
      .build( CacheLoader.from( object -> uncachedRetrieval( delegate ) ) );
  }

  public T get() {
    try {
      return loadingCache.get( KEY );
    } catch ( ExecutionException e ) {
      throw new IllegalStateException( e );
    }
  }

  /**
   * Initializes and caches the value using the provided delegate, as well as init'ing the cleanup scheduler.
   * synchronized to avoid any collision with onExpire
   */
  private synchronized T uncachedRetrieval( Supplier<T> delegate ) {
    scheduledFuture = cleanupExecutor.scheduleAtFixedRate( loadingCache::cleanUp, 1, 1, SECONDS );
    T value = delegate.get();
    LOGGER.logBasic( "Caching value: " + value );
    return value;
  }

  //TODO- loglevel to Debug.

  /**
   * Invoke the cleanup handler (onExpire), and cancel the scheduler.
   */
  private synchronized void onExpire( T memoizedValue ) {
    LOGGER.logBasic( "Expiring value:  " + memoizedValue );
    Preconditions.checkNotNull( scheduledFuture, "Expire shouldn't happen before an uncachedRetrieval." );
    scheduledFuture.cancel( false );
    onExpire.apply( memoizedValue );
  }
}
