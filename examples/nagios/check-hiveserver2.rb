#!/usr/bin/env ruby

require 'rubygems'
require 'thrift'
require 'getoptlong'
require 'timeout'

CUR_DIR = File.dirname(__FILE__)
GEM_ROOT = File.expand_path(File.join(CUR_DIR, '../..'))
require File.join(GEM_ROOT, 'lib', 'rbhive')

#---------------------------------------------------------------------------------------------------
class ErrorOnlyLogger
  def error(message)
    STDERR.puts(message)
  end

  def fatal(message)
    STDERR.puts(message)
  end

  %w(warn info debug).each do |level|
    define_method level.to_sym do |message|
      # noop
    end
  end
end

#---------------------------------------------------------------------------------------------------
def nagios_status(status, msg)
  exit_status = {
    'OK' => 0,
    'WARNING' => 1,
    'CRITICAL' => 2,
    'UNKNOWN' => 3
  }[status]

  nagios_status('CRITICAL', "Invalid nagios status: '#{status}, message: '#{msg}'") unless exit_status

  puts("#{status}: #{msg}")
  exit(exit_status)
end

#---------------------------------------------------------------------------------------------------
def get_app_revision(host, port, domain)
  http = Net::HTTP.new(host, port)
  request = Net::HTTP::Get.new("http://#{domain}/swiftype-app-version")
  response = http.request(request)
  nagios_status('UNKNOWN', "Could not get current revision from the app (HTTP #{response.code})") unless response.code == "200"
  return response.body.strip
end

#---------------------------------------------------------------------------------------------------
def get_cur_revision(base_dir)
  cur_link = "#{base_dir}/current"
  release_file = "#{cur_link}/REVISION"

  nagios_status('UNKNOWN', "Current release directory does not exist!") unless File.exist?(cur_link)
  nagios_status('UNKNOWN', "Current release directory does not have a REVISION file!") unless File.readable?(release_file)

  return File.read(release_file).strip
end

#---------------------------------------------------------------------------------------------------
def show_help
  puts "Usage: #{$0} [args]"
  puts 'Where args are:'
  puts '  --host=host      | -H host    HiveServer2 host to send requests to (default: localhost)'
  puts '  --port=port      | -p port    HiveServer2 port to send requests to (default: 10000)'
  puts '  --user=user      | -u user    SASLUsername to use for SASL authorization (default: admin)'
  puts '  --password=pwd   | -P pwd     Password to use for SASL authorization (default: admin)'
  puts '  --query=query    | -q query   Query to user for the check (default: show tables)'
  puts '  --no-sasl        | -s         Disables the use of SASL (default: use sasl for all checks)'
  puts '  --help           | -h         This help'
  puts
  exit(0)
end

#---------------------------------------------------------------------------------------------------
host = "localhost"
port = 10000
use_sasl = true
sasl_username = "admin"
sasl_password = "admin"
query = "show tables"
debug = false
connection_timeout = 5

# Parse options
opts = GetoptLong.new(
  [ '--host',     '-H', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--port',     '-p', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--user',     '-u', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--password', '-P', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--query',    '-q', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--no-sasl',  '-s', GetoptLong::NO_ARGUMENT ],
  [ '--debug',    '-d', GetoptLong::NO_ARGUMENT ],
  [ '--help',     '-h', GetoptLong::NO_ARGUMENT ]
)

# Process options
opts.each do |opt, arg|
  case opt
    when "--host"
      host = arg
    when "--port"
      port = arg.to_i
    when "--user"
      sasl_username = arg.strip
    when "--password"
      sasl_password = arg.strip
    when "--query"
      query = arg.strip
    when "--no-sasl"
      use_sasl = false
    when "--debug"
      debug = true
    when "--help"
      show_help
  end
end

#---------------------------------------------------------------------------------------------------
sasl_params = use_sasl ? { :username => sasl_username, :password => sasl_password } : nil
logger = debug ? RBHive::StdOutLogger : ErrorOnlyLogger

# Create connection to hiveserver
conn = RBHive::TCLIConnection.new(host, port, sasl_params, logger.new)

begin
  # Try to open the connection
  timeout(connection_timeout) do
    conn.open
    conn.open_session
  end

  # Disable concurrency to make sure hiveserver2 would not leak ZK connections
  conn.set("hive.support.concurrency", false)

  # Try to execute the query
  rows = conn.fetch(query)
  if rows
    nagios_status('OK', "Query succeeded with #{rows.count} results (#{query})")
  else
    nagios_status('CRITICAL', "Query returned nil!")
  end

rescue Timeout::Error => e
  nagios_status('CRITICAL', "Connection timeout after #{connection_timeout} seconds")

rescue => e
  nagios_status('CRITICAL', "Query failed: #{e}")

ensure
  # Try to close the session and our connection if those are still open, ignore io errors
  begin
    conn.close_session if conn.session
    conn.close
  rescue IOError => e
    # noop
  end

end
