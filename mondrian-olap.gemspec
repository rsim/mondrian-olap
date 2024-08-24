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
  gem.date          = "2023-06-02"
  gem.license       = 'MIT'

  gem.files         = Dir['Changelog.md', 'LICENSE*', 'README.md', 'VERSION', 'lib/**/*', 'spec/**/*'] -
                      Dir['spec/support/jars/*']
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.extra_rdoc_files = Dir["README.md"]

  gem.platform = Gem::Platform::RUBY # as otherwise rubygems.org are not showing latest version
  gem.add_dependency "nokogiri"

  gem.add_development_dependency "bundler"
  gem.add_development_dependency "rake", "~> 13.0.6"
  gem.add_development_dependency "rspec", "~> 3.12.0"
  gem.add_development_dependency "rdoc", "~> 6.5.0"
  gem.add_development_dependency "jdbc-mysql", "~> 8.0.27"
  gem.add_development_dependency "jdbc-postgres", "~> 42.2.25"
  gem.add_development_dependency "activerecord", "~> 6.1.7.2"
  gem.add_development_dependency "activerecord-jdbc-adapter", "~> 61.3"
  gem.add_development_dependency "activerecord-oracle_enhanced-adapter", "~> 6.1.6"
  gem.add_development_dependency "pry"
end
