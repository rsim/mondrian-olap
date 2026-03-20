# frozen_string_literal: true

require 'java'
require 'nokogiri'

{
  # Do not register MondrianOlap4jDriver
  "mondrian.olap4j.registerDriver" => false,
  # Do not register log3j2 MBean
  "log4j2.disable.jmx" => true
}.each do |key, value|
  if java.lang.System.getProperty(key).nil?
    java.lang.System.setProperty(key, value.to_s)
  end
end

directory = File.expand_path("../jars", __FILE__)
if (mondrian_olap_jar_path = ENV['MONDRIAN_OLAP_JAR_PATH'])
  unless File.exist?(mondrian_olap_jar_path) && File.basename(mondrian_olap_jar_path) =~ /\Amondrian-.*\.jar\z/
    mondrian_olap_jar_path = nil
  end
end
Dir["#{directory}/*.jar"].each do |file|
  next if mondrian_olap_jar_path && File.basename(file) =~ /\Amondrian-/
  require file
end
require mondrian_olap_jar_path if mondrian_olap_jar_path

%w(error connection query result schema schema_udf cube).each do |file|
  require "mondrian/olap/#{file}"
end
