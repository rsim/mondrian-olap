# frozen_string_literal: true

require 'rake/testtask'

Rake::TestTask.new(:test) do |t|
  t.libs << 'test'
  t.pattern = 'test/**/*_test.rb'
  t.warning = false
end

namespace :test do
  %w(mysql jdbc_mysql postgresql oracle sqlserver vertica snowflake clickhouse mariadb).each do |driver|
    desc "Run tests with #{driver} driver"
    task driver do
      ENV['MONDRIAN_DRIVER'] = driver
      Rake::Task['test'].reenable
      Rake::Task['test'].invoke
    end
  end

  desc "Run tests with all primary database drivers"
  task :all do
    %w(mysql jdbc_mysql postgresql oracle sqlserver).each do |driver|
      Rake::Task["test:#{driver}"].invoke
    end
  end
end
