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

package org.pentaho.di.trans.streaming.common;

import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.SubtransExecutor;
import org.pentaho.di.trans.streaming.api.StreamWindow;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FixedTimeStreamWindow<I extends List> implements StreamWindow<I, Result> {

  private final RowMetaInterface rowMeta;
  private final long millis;
  private final int batchSize;
  private SubtransExecutor subtransExecutor;

  public FixedTimeStreamWindow( SubtransExecutor subtransExecutor, RowMetaInterface rowMeta, long millis,
                                int batchSize ) {
    this.subtransExecutor = subtransExecutor;
    this.rowMeta = rowMeta;
    this.millis = millis;
    this.batchSize = batchSize;
  }

  @Override public Iterable<Result> buffer( Iterable<I> rowIterator ) {
    return Observable.fromIterable( rowIterator )
      .subscribeOn( Schedulers.newThread() )
      .buffer( millis, TimeUnit.MILLISECONDS, batchSize )
      .filter( list -> !list.isEmpty() )

      // future enhancement - this will allow the sendBuffer to be run in a separate thread.
      //      .flatMap( list -> Observable.just( list )
      //        .subscribeOn( Schedulers.newThread() )
      //        .map( this::sendBufferToSubtrans ) )
      //      .blockingIterable();

      .map( this::sendBufferToSubtrans )
      .blockingIterable();
  }

  private Result sendBufferToSubtrans( List<I> input ) throws KettleException {
    System.out.println( "sendBuffer thread:  " + Thread.currentThread() );
    Optional<Result> result = subtransExecutor.execute(
      input.stream()
        .map( row -> row.toArray( new Object[ 0 ] ) )
        .map( objects -> new RowMetaAndData( rowMeta, objects ) )
        .collect( Collectors.toList() )
    );
    return result.orElseThrow( () -> new KettleException( "Failed to get results" ) ); // TODO messagify
  }

}
