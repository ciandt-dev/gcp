package com.ciandt.gcp.poc.spark.xml;

import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class ParseXML implements Function<String, ExampleXML> {
  @Override
  public ExampleXML call(String input) throws Exception {
    JAXBContext jaxb = JAXBContext.newInstance(ExampleXML.class);
    Unmarshaller um = jaxb.createUnmarshaller();
    Object xml = um.unmarshal(new StringReader(input));
    return (ExampleXML) xml;
  }
}
