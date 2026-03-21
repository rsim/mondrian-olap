require "bundler/gem_tasks"

desc "Run tests (default)"
task :default => :test

require 'rdoc/task'
RDoc::Task.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION').chomp : ""

  rdoc.rdoc_dir = 'doc'
  rdoc.title = "mondrian-olap #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

require_relative 'test/rake_tasks'

desc "Copy Mondrian JAR from mondrian-olap-java build to lib/mondrian/jars"
task :copy_mondrian_jar do
  Dir.chdir(File.dirname(__FILE__)) do
    target_dir = "lib/mondrian/jars"
    Dir.glob("#{target_dir}/mondrian*.jar").each { |f| rm f }
    source_files = Dir.glob("../mondrian-olap-java/mondrian/target/mondrian*.jar")
    raise "No mondrian*.jar found in ../mondrian-olap-java/mondrian/target/" if source_files.empty?
    cp source_files, target_dir
  end
end

Dir["lib/tasks/**/*.rake"].each { |ext| load ext } if defined?(Rake)
