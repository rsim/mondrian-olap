require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

require 'rake'

require 'jeweler'
Jeweler::Tasks.new do |gem|
  gem.name = "mondrian-olap"
  gem.summary = "JRuby API for Mondrian OLAP Java library"
  gem.description = <<-EOS
JRuby gem for performing multidimensional queries of relational database data using Mondrian OLAP Java library
EOS
  gem.email = "raimonds.simanovskis@gmail.com"
  gem.homepage = "http://github.com/rsim/mondrian-olap"
  gem.authors = ["Raimonds Simanovskis"]
  gem.extra_rdoc_files = ['README.md']
end
Jeweler::RubygemsDotOrgTasks.new

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec)

RSpec::Core::RakeTask.new(:rcov) do |t|
  t.rcov = true
  t.rcov_opts =  ['--exclude', '/Library,spec/']
end

task :default => :spec

require 'rdoc/task'
RDoc::Task.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'doc'
  rdoc.title = "mondrian-olap #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

require 'spec/rake_tasks'
