package org.pentaho.di.trans.step.jms.context;

import org.junit.Test;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.step.jms.JmsDelegate;

import javax.jms.JMSException;
import javax.jms.JMSProducer;
import java.util.Collections;

public class ActiveMQProviderTest {

  // TODO - remove.  Just an easy way to have a producer while manually testing consumer.
  @Test public void testProducer() throws JMSException, InterruptedException {
    JmsProvider jmsProvider = new ActiveMQProvider();

    JmsDelegate del = new JmsDelegate( Collections.emptyList() );
    del.url = "tcp://localhost:61616";
    del.username = "admin";
    del.password = "admin";
    del.destinationName = "newTest";
    del.destinationType = "QUEUE";

    JMSProducer producer = jmsProvider
      .getContext( del, new Variables() ).createProducer();

    for ( int i = 0; i < 1000; i++ ) {
      producer.send( jmsProvider.getDestination( del, new Variables() ), "foo" );
      Thread.sleep( 1000 );
    }

    System.out.println( "foo" );
  }

}