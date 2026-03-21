source 'https://rubygems.org'

ruby RUBY_VERSION, engine: 'jruby', engine_version: JRUBY_VERSION

gemspec

gem 'rake', '~> 13.3.0'
gem 'minitest', '~> 5.25'
gem 'minitest-hooks', '~> 1.5'
gem 'minitest-reporters', '~> 1.7'
gem 'rdoc', '~> 6.5.0'
gem 'jdbc-mysql', '~> 8.0.30'
gem 'jdbc-postgres', '~> 42.7.8'
gem 'activerecord', '~> 6.1.7.10'
gem 'activerecord-jdbc-adapter', '~> 61.3'
gem 'activerecord-oracle_enhanced-adapter', '~> 6.1.6'
gem 'pry', '~> 0.14.1'

if JRUBY_VERSION.to_i >= 10
  gem 'mutex_m'
  gem 'base64'
  gem 'bigdecimal'
  gem 'drb'
end
