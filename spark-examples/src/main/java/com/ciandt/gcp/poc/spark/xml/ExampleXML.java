package com.ciandt.gcp.poc.spark.xml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExampleXML {
  String property1;
  
  public String getProperty1() {
    return property1;
  }

  @XmlElement
  public void setProperty1(String property1) {
    this.property1 = property1;
  }
}
