#!/usr/bin/env ruby

require 'rubygems'
require 'thrift'

GEM_ROOT = File.expand_path(File.join(File.dirname(__FILE__), '..'))
require File.join(GEM_ROOT, 'lib', 'rbhive')

hive_server = ENV['HIVE_SERVER'] || 'localhost'
hive_port = (ENV['HIVE_PORT'] || 10_000).to_i

puts "Connecting to #{hive_server}:#{hive_port} using SASL..."
RBHive.tcli_connect(hive_server, hive_port, nil) do |conn|
  puts "Creating a table..."
  conn.execute("create table foo_test_tbl(a int, b string)")

  puts "Dropping a table..."
  conn.execute("drop table foo_test_tbl")

  raised = false
  begin
    puts "Executing a query with invalid syntax, should raise an exception..."
    conn.execute("drop foo table foo foo_test_tbl?")
  rescue => e
    puts "Exception raised: #{e}"
    raised = true
  end

  if raised
    puts "Ok, exception has been raised as expected"
  else
    puts "ERROR: we've expected an exception to be raised from exec"
  end
end
