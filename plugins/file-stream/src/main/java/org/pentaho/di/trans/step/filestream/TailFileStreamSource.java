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

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.streaming.api.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TailFileStreamSource implements StreamSource<List<String>> {

  private static Class<?> PKG = FileStream.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private final BufferedReader reader;
  private final AtomicBoolean paused = new AtomicBoolean( false );
  private final AtomicBoolean closed = new AtomicBoolean( false );

  private final Logger logger = LoggerFactory.getLogger( getClass() );

  public TailFileStreamSource( String filename ) throws FileNotFoundException {
    reader = new BufferedReader( new FileReader( filename ) );
  }

  @Override public Iterable<List<String>> rows() {

    return () -> fileIterator();
  }

  @Override public void close() {
    closed.set( true );
    try {
      if ( reader != null ) {
        reader.close();
      }
    } catch ( IOException e ) {
      logger.error( BaseMessages.getString( PKG, "FileStream.Error.FileCloseError" ) , e );

    }
  }

  @Override public void pause() {
    this.paused.set( true );
  }

  @Override public void resume() {
    this.paused.set( false );
  }


  private Iterator<List<String>> fileIterator() {
    return new Iterator<List<String>>() {

      @Override public boolean hasNext() {
        return true;
      }

      @Override public List<String> next() {
        String currentLine = null;
        try {

          while ( paused.get() || ( currentLine = reader.readLine() ) == null ) {
            Thread.sleep( 500 );
          }
        } catch ( IOException | InterruptedException e ) {
          logger.error( BaseMessages.getString( PKG, "FileStream.Error.FileStreamError" ) , e );
        }
        return Collections.singletonList( currentLine );
      }
    };

  }
}
