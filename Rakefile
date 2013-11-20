require "bundler/gem_tasks"
require "rspec/core/rake_task"

desc "Run specs"
RSpec::Core::RakeTask.new(:spec)
RSpec::Core::RakeTask.new(:rcov) do |t|
  t.rcov = true
  t.rcov_opts =  ['--exclude', '/Library,spec/']
end

desc "Run specs (default)"
task :default => :spec

#require 'rubygems'
#require 'bundler'
#begin
#  Bundler.setup(:default, :development)
#rescue Bundler::BundlerError => e
#  $stderr.puts e.message
#  $stderr.puts "Run `bundle install` to install missing gems"
#  exit e.status_code
#end


require 'rdoc/task'
RDoc::Task.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'doc'
  rdoc.title = "mondrian-olap #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

require 'spec/rake_tasks'
