package org.pentaho.pdi.engine.base.event;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by hudak on 6/14/17.
 */
public class MetricsTest {

  private final JsonTester<Metrics> jsonTester = new JsonTester<>( Metrics.class );
  private List<Metrics> metrics;

  @Before
  public void setUp() throws Exception {
    metrics = Stream.generate( () -> {
      Random random = new Random();
      return new Metrics( random.nextLong(), random.nextLong(), random.nextLong(), random.nextLong() );
    } ).limit( 10 ).collect( Collectors.toList() );
  }

  @Test
  public void json() throws Exception {
    for ( Metrics metric : this.metrics ) {
      jsonTester.verifyEncode( metric );
      jsonTester.verifyTypeInfo( metric );
    }
  }

}