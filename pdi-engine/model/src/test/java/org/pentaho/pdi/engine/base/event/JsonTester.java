package org.pentaho.pdi.engine.base.event;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Created by hudak on 6/14/17.
 */
class JsonTester<T> {
  private final Class<T> type;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public JsonTester( Class<T> type ) {
    this.type = type;
  }

  void verifyEncode( T object ) throws IOException {
    String json = objectMapper.writeValueAsString( object );
    T result = objectMapper.readValue( json, type );
    assertThat( result, equalTo( object ) );
  }

  void verifyTypeInfo( T object ) {
    HashMap<String, Object> map = objectMapper.convertValue( object, new HashMapType() );
    assertThat( map, hasEntry( "class", type.getName() ) );
  }

  private static class HashMapType extends TypeReference<HashMap<String, Object>> {
  }
}
