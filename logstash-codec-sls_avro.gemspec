Gem::Specification.new do |s|

  s.name            = 'logstash-codec-sls_avro'
  s.version         = '1.0.0'
  s.licenses        = ['Apache-2.0']
  s.summary         = "Encode and decode avro formatted data."
  s.description     = "See summary."
  s.authors         = ["Elastic","Stunlock Studios"]
  s.email           = 'oss@stunlockstudios.com'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths   = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "codec" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core", ">= 2.0.0", "<= 6.0.0"
  #s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"

  s.add_runtime_dependency "avro", '~> 1.0', '>= 1.0.0'  #(Apache 2.0 license)

  s.add_development_dependency "logstash-devutils", "~> 0"
end

