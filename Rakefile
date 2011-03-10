require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "sonar-ews-pull-connector"
    gem.summary = %Q{Exchange Web Services connector for Exchange 2007/2010}
    gem.description = %Q{A sonar-connector for extracting emails from Exchange 2007/2010 through Exchange Web Services}
    gem.email = "craig@trampolinesystems.com"
    gem.homepage = "http://github.com/trampoline/sonar-ews-pull-connector"
    gem.authors = ["mccraigmccraig"]
    gem.add_dependency "sonar_connector", ">= 0.7.2"
    gem.add_dependency "savon", ">= 0.8.6"
    gem.add_dependency "ntlm-http", ">= 0.1.1"
    gem.add_dependency "httpclient", ">= 2.1.6.1.1"
    gem.add_dependency "fetch_in", ">= 0.2.0"
    gem.add_development_dependency "rspec", ">= 1.2.9"
    gem.add_development_dependency "rr", ">= 0.10.5"
    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'spec/rake/spectask'
Spec::Rake::SpecTask.new(:spec) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.spec_files = FileList['spec/**/*_spec.rb']
end

Spec::Rake::SpecTask.new(:rcov) do |spec|
  spec.libs << 'lib' << 'spec'
  spec.pattern = 'spec/**/*_spec.rb'
  spec.rcov = true
end

task :spec => :check_dependencies

task :default => :spec

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "sonar-ews-pull-connector #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
