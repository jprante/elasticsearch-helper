#
# A basic, synthetic benchmark of the impact HTTP client has
# on the speed of talking to ElasticSearch in the Tire gem.
#
# In general, Curb seems to be more then two times faster the RestClient, in some cases it's three
# to five times faster. I wonder if keep-alive has anything to do with it, but it probably does.
#
# Run me with:
#
#  gem install yajl-ruby
#  gem install rest-client
#  gem install hashr
#  gem install curb
#     $ git clone git://github.com/karmi/tire.git
#     $ cd tire
#     $ curl -o tire_http_clients_benchmark.rb https://raw.github.com/gist/1204159
#     $ time ruby tire_http_clients_benchmark.rb
#
#

$LOAD_PATH.unshift File.expand_path('../lib', __FILE__)

require 'rubygems'
require 'yajl/json_gem'
require 'benchmark'

require 'tire'

# ===============================
# Require the Curb implementation
#
require 'curb'
require 'tire/http/clients/curb'
# ===============================

extend Tire::DSL

content   = DATA.read
documents = (1..10000).map { |i| { :id => i, :title => "Document #{i}", :content => content } }

curb_elapsed = Benchmark.realtime do

  configure do
    # logger STDOUT
    client Tire::HTTP::Client::Curb
  end

  index 'import-curb' do
    delete
    create
    import documents, :per_page => 1000
    refresh
  end

  s = search('import-curb') { query { all } }
  s = search('import-curb') { query { string 'hey' } }
  1000.times do
    ('a'..'z').each do |letter|
      search('import-curb') { query { string letter } }
    end
  end
  # p s.results

end

restclient_elapsed = Benchmark.realtime do

  configure do
    # logger STDOUT
    client Tire::HTTP::Client::RestClient
  end

  index 'import-restclient' do
    delete
    create
    import documents, :per_page => 1000
    refresh
  end

  s = search('import-restclient') { query { all } }
  s = search('import-restclient') { query { string 'hey' } }
  1000.times do
    ('a'..'z').each do |letter|
      search('import-restclient') { query { string letter } }
    end
  end
  # p s.results

end

puts '='*80

p    ["RestClient", restclient_elapsed]
puts '-'*80, ""


p    ["Curb", curb_elapsed]
puts '-'*80, ""


__END__
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.