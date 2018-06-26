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

package org.pentaho.di.trans.step.filestream;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.apache.commons.vfs2.util.RandomAccessMode;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;

/**
 * A simple example implementation of StreamSource which streams rows from a specified file to an iterable.
 * <p>
 * Note that this class is strictly meant as an example and not intended for real use. It uses a simplistic strategy of
 * leaving a BufferedReader open in order to load rows as they come in, without real consideration of error conditions.
 */
public class TailFileStreamSource extends BlockingQueueStreamSource<List<Object>> {

  private static Class<?> PKG = FileStream.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private final String filename;

  private LogChannelInterface logChannel = new LogChannel( this );
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private Future<?> future;

  public TailFileStreamSource( String filename, FileStream fileStream ) throws FileNotFoundException {
    super( fileStream );
    this.filename = filename;
  }


  @Override public void open() {
    if ( future != null ) {
      logChannel.logError( "open() called more than once" );
      return;
    }
    future = executorService.submit( this::fileReadLoop );
  }

  @Override public void close() {
    super.close();
    future.cancel( true );
  }

  private void fileReadLoop() {

    AtomicLong index = new AtomicLong( 0 );
    try {

      FileObject filObj =
        KettleVFS.getFileObject( filename );

      RandomAccessContent rand =
        filObj.getContent().getRandomAccessContent( RandomAccessMode.READ );
      DefaultFileMonitor fm = new DefaultFileMonitor( new FileListener() {
        @Override public void fileCreated( FileChangeEvent fileChangeEvent ) throws Exception {
          System.out.println( "File created" );
        }

        @Override public void fileDeleted( FileChangeEvent fileChangeEvent ) throws Exception {
          System.out.println( "File deleted" );
        }

        AtomicBoolean loading = new AtomicBoolean( false );

        @Override public void fileChanged( FileChangeEvent fileChangeEvent ) throws Exception {
          System.out.println( "File Changed.  Length=" + rand.length() );
          if ( loading.getAndSet( true ) ) {
            return;
          }

          long offset = index.get();
          List<List<Object>> lines = new ArrayList<>();
          rand.seek( offset );
          while ( offset < rand.length() ) {
            byte c = rand.readByte();
            String next = new String( new byte[] { rand.readByte() } );
            StringBuilder builder = new StringBuilder();
            while ( !next.equals( "\n" ) ) {
              builder.append( next );

              next = new String( new byte[] { rand.readByte() } );
            }

            lines.add( singletonList( builder.toString() ) );
            offset = rand.getFilePointer();
          }
          System.out.println( "CU" );
          index.set( offset );
          acceptRows( lines );
          loading.set( false );
        }

      } );
      fm.addFile( filObj );
      fm.start();
    } catch ( KettleFileException | FileSystemException e ) {
      e.printStackTrace();
    }


    //    try ( BufferedReader reader = new BufferedReader( new FileReader( filename ) ) ) {
    //      while ( true ) {
    //        acceptRows( singletonList( singletonList( getNextLine( reader ) ) ) );
    //      }
    //    } catch ( IOException | InterruptedException e ) {
    //      logChannel.logError( BaseMessages.getString( PKG, "FileStream.Error.FileStreamError" ), e );
    //    }
  }

  private String getNextLine( BufferedReader reader ) throws IOException, InterruptedException {
    String currentLine;
    while ( ( currentLine = reader.readLine() ) == null ) {
      Thread.sleep( 500 );
    }
    return currentLine;
  }


}
