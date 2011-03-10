$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems'
require 'spec'
require 'spec/autorun'
require 'rr'
require 'ews_pull_connector/ews_pull_connector'

Spec::Runner.configure do |config|
  config.mock_with RR::Adapters::Rspec
end
