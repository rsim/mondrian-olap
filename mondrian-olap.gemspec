# encoding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mondrian/olap/version'

Gem::Specification.new do |gem|
  gem.name          = "mondrian-olap"
  gem.version       = ::Mondrian::OLAP::VERSION
  gem.authors       = ["Raimonds Simanovskis"]
  gem.email         = ["raimonds.simanovskis@gmail.com"]
  gem.description   = "JRuby gem for performing multidimensional queries of relational database data using Mondrian OLAP Java library"
  gem.summary       = "JRuby API for Mondrian OLAP Java library"
  gem.homepage      = "http://github.com/rsim/mondrian-olap"
  gem.date          = "2023-06-02"
  gem.license       = 'MIT'

  gem.files         = Dir['Changelog.md', 'LICENSE*', 'README.md', 'VERSION', 'lib/**/*', 'spec/**/*', 'test/**/*'] -
                      Dir['spec/support/jars/*']
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.extra_rdoc_files = Dir["README.md"]

  gem.platform = Gem::Platform::RUBY # as otherwise rubygems.org are not showing latest version
  gem.add_dependency "nokogiri"

end
