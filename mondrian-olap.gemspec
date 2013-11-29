# encoding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mondrian/olap/version'

Gem::Specification.new do |gem|
  gem.name          = "mondrian-olap"
  gem.version       = ::Mondrian::OLAP::VERSION
  gem.authors       = ["Raimonds Simanovskis"]
  gem.email         = ["raimonds.simanovskis@gmail.com"]
  gem.description   = "JRuby gem for performing multidimensional queries of relational database data using Mondrian OLAP Java library\n"
  gem.summary       = "JRuby API for Mondrian OLAP Java library"
  gem.homepage      = "http://github.com/rsim/mondrian-olap"
  gem.date          = "2013-11-29"
  gem.license       = 'MIT'

  gem.files         = Dir['Changelog.md', 'LICENSE*', 'README.md', 'VERSION', 'lib/**/*', 'spec/**/*']
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.extra_rdoc_files = [ "README.md" ]

  gem.platform = Gem::Platform::RUBY # as otherwise rubygems.org are not showing latest version
  gem.add_dependency "nokogiri"

  gem.add_development_dependency "bundler"
  gem.add_development_dependency "rake"
  gem.add_development_dependency "rspec"
  gem.add_development_dependency "rdoc"
  gem.add_development_dependency "jdbc-mysql"
  gem.add_development_dependency "jdbc-postgres"
  gem.add_development_dependency "jdbc-luciddb"
  gem.add_development_dependency "jdbc-jtds", "~> 1.2.8" # version 1.3 is not compatible with Java 6
  gem.add_development_dependency "activerecord"
  gem.add_development_dependency "activerecord-jdbc-adapter"
  gem.add_development_dependency "activerecord-oracle_enhanced-adapter"
  gem.add_development_dependency "pry"
  gem.add_development_dependency "therubyrhino"
  gem.add_development_dependency "coffee-script"
end
