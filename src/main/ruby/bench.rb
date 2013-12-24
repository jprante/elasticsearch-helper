
require 'rubygems'
require 'yajl/json_gem'
require 'benchmark'

require 'tire'
require 'curb'
require 'tire/http/clients/curb'

def rand_hexstring(length=32)
  ((0..length).map{rand(256).chr}*"").unpack("H*")[0][0,length]
end

extend Tire::DSL

words = (1..1000).map { rand_hexstring }

documents = (1..10000).map { |i| { :id => i, 
          :a => words[rand(words.length)],
          :b => words[rand(words.length)],
          :c => words[rand(words.length)],
          :d => words[rand(words.length)],
          :e => words[rand(words.length)],
          :f => words[rand(words.length)],
          :g => words[rand(words.length)],
          :h => words[rand(words.length)],
          :i => words[rand(words.length)],
          :j => words[rand(words.length)],
          :k => words[rand(words.length)],
          :l => words[rand(words.length)],
          :m => words[rand(words.length)],
          :n => words[rand(words.length)],
          :o => words[rand(words.length)],
          :p => words[rand(words.length)],
          :q => words[rand(words.length)],
          :r => words[rand(words.length)],
          :s => words[rand(words.length)],
          :t => words[rand(words.length)],
          :u => words[rand(words.length)],
          :v => words[rand(words.length)],
          :w => words[rand(words.length)],
          :x => words[rand(words.length)],
          :y => words[rand(words.length)],
          :z => words[rand(words.length)]          
} }

elapsed = Benchmark.realtime do

  configure do
    client Tire::HTTP::Client::Curb
  end

  index 'rubybench' do
    delete
    create
    import documents, :per_page => 100
    refresh
  end

end

p    ["Benchmark", elapsed]
puts '-'*80, ""
