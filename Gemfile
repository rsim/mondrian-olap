source 'https://rubygems.org'

ruby RUBY_VERSION, engine: 'jruby', engine_version: JRUBY_VERSION

gemspec

# nokogiri 1.13 requires Ruby 2.6 compatibility byt JRuby 9.2 has Ruby 2.5 compatibility
gem 'nokogiri', '~> 1.12.5' if JRUBY_VERSION.start_with?('9.2.')

