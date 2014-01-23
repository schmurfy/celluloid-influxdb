# -*- encoding: utf-8 -*-
require File.expand_path('../lib/celluloid-influxdb/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Julien Ammous"]
  gem.email         = ["schmurfy@gmail.com"]
  gem.description   = %q{..}
  gem.summary       = %q{...}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.name          = "celluloid-influxdb"
  gem.license       = 'MIT'
  gem.require_paths = ["lib"]
  gem.version       = CelluloidInfluxdb::VERSION
  
  
  gem.add_dependency 'celluloid-io'
  gem.add_dependency 'http'
  gem.add_dependency 'multi_json'

end
