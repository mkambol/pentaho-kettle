package org.pentaho.pdi.engine.base.event;

import org.junit.Test;
import org.pentaho.di.engine.api.reporting.Status;

/**
 * Created by hudak on 6/14/17.
 */
public class StatusTest {
  private final JsonTester<Status> jsonTester = new JsonTester<>( Status.class );

  @Test
  public void json() throws Exception {
    for ( Status status : Status.values() ) {
      jsonTester.verifyEncode( status );
    }
  }
}
