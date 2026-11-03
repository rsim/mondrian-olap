require "bundler/gem_tasks"
require "date"

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

namespace :release do
  desc "Check that the current version is ready for the release"
  task :check do
    version = File.read("VERSION").chomp
    errors = []

    today = Date.today.iso8601
    changelog_date = File.read("CHANGELOG.md")[/^## \[#{Regexp.escape(version)}\] - (\d{4}-\d{2}-\d{2})$/, 1]
    if changelog_date.nil?
      errors << "CHANGELOG.md does not have the \"## [#{version}] - YYYY-MM-DD\" entry"
    elsif changelog_date != today
      errors << "CHANGELOG.md #{version} entry date #{changelog_date} is not the release date #{today}"
    end

    snapshot_jars = Dir["lib/mondrian/jars/*SNAPSHOT*.jar"]
    unless snapshot_jars.empty?
      errors << "Snapshot jars cannot be released: #{snapshot_jars.join(', ')}"
    end

    current_branch = `git rev-parse --abbrev-ref HEAD`.strip
    unless current_branch == "master"
      errors << "Release from the master branch, the current branch is #{current_branch}"
    end

    # The gemspec includes files from the directories and not from git, including untracked and ignored files
    gem_files = Gem::Specification.load("mondrian-olap.gemspec").files.select { |file| File.file?(file) }
    untracked_gem_files = gem_files - `git ls-files`.split("\n")
    unless untracked_gem_files.empty?
      errors << "Files not tracked by git would be included in the gem: #{untracked_gem_files.join(', ')}"
    end

    abort errors.map { |error| "ERROR: #{error}" }.join("\n") unless errors.empty?
  end
end

# Check the release before the git tag and the gem are pushed by the Bundler release task
task "release:guard_clean" => "release:check"

Dir["lib/tasks/**/*.rake"].each { |ext| load ext } if defined?(Rake)
