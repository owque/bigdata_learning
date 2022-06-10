package com.owen.bigdata.v2.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.commons.lang3.StringUtils

@JsonIgnoreProperties(ignoreUnknown = true)
class BaseArgument extends Serializable {
  var stateOutputUri = ""
  var inputFilePath = ""
  var outputPath = ""
  var header = ""
  var delimiter = ""

  def validate(): Unit ={
    if(StringUtils.isBlank(stateOutputUri)){
      throw new IllegalArgumentException("stateOutputUri not found.")
    }
    if(StringUtils.isBlank(inputFilePath)){
      throw new IllegalArgumentException("inputFilePath not found.")
    }
  }
}
