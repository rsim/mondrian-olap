require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "mondrian-olap"
    gem.summary = "Ruby DSL for Mondrian OLAP Java library"
    gem.description = <<-EOS
mondrian-olap provides OLAP queries from relational databases using Mondrian OLAP engine.
EOS
    gem.email = "raimonds.simanovskis@gmail.com"
    gem.homepage = "http://github.com/rsim/mondrian-olap"
    gem.authors = ["Raimonds Simanovskis"]
    gem.add_dependency "nokogiri", ">= 1.4.3"
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec)

RSpec::Core::RakeTask.new(:rcov) do |t|
  t.rcov = true
  t.rcov_opts =  ['--exclude', '/Library,spec/']
end

task :spec => :check_dependencies

task :default => :spec

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'doc'
  rdoc.title = "mondrian-olap #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
