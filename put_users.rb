#!/usr/bin/env ruby
require 'parallel'
require 'riak'
require 'multi_json'

DIR = ARGV[0]
BUCKET = ARGV[1]
#WORKERS = 9

#nodes = [8081, 8082, 8083, 8084].map {|port| {host: '127.0.0.1', pb_port: port}}
=begin
# remove existing keys
client = Riak::Client.new(protocol: "pbc", nodes: nodes)
bucket = client.bucket(BUCKET)
keys = bucket.keys()
n = keys.length
batches = keys.each_slice(n/WORKERS)

def delete(batch)
  client = Riak::Client.new(protocol: "pbc", nodes: nodes)
  bucket = client.bucket(BUCKET)
  batch.each do |key|
    bucket.delete(key)
  end
end

STDERR.puts("Deleting #{n} keys from #{BUCKET}")
Parallel.each(batches) {|batch| delete(batch)}
STDERR.puts("Finished deleting!")
=end

paths = Dir.entries(DIR).grep(/\.json$/)
n = paths.length
FIN = File.join(DIR, 'finished')
BDIR = File.join(DIR, 'bad')
WORKERS = 8
`mkdir -p #{FIN} #{BDIR}`
failed = []
bad = []
#batches = paths.each_slice(n/WORKERS)

def move_bad_helper(batch)
  bad = []
  c = 0
  x = rand
  STDERR.puts "Start of move_bad_helper, id #{x}, #{batch.length} items"
  batch.each do |filename|
    path = File.join(DIR, filename)
    bpath = File.join(BDIR, filename)
    unless filename.match(/^[\w\-.]+$/)
      STDERR.puts("Shit bad filename #{filename}.  Moving...")
      STDERR.flush
      File.rename(path, bpath)
      next
    end
    contents =
      begin
        File.read(path)
      rescue Exception => e
        STDERR.puts "Can't read #{path}."
        STDERR.puts(e, e.message, e.backtrace)
        STDERR.flush
        bad.push(filename)
        File.rename(path, bpath)
        nil
      end
    next if contents.nil?
    hash =
      begin
        MultiJson.load(contents)
      rescue Exception => e
        STDERR.puts("Can't decode #{path}.")
        STDERR.puts(e, e.message, e.backtrace)
        STDERR.flush
        bad.push(filename)
        File.rename(path, bpath)
        nil
      end
    c += 1
  end
  STDERR.puts("made it to the end of move_bad_helper, id #{x} c is #{c}")
  bad
end

def move_bad(paths, bad)
  nbatches = paths.length/WORKERS
  batches = 
    if nbatches > 0
      paths.each_slice(nbatches)
    else
      [paths]
    end
  _bad = Parallel.map(batches) {|batch| move_bad_helper(batch)}
  bad += _bad.flatten
end

def put(batch, failed)
  nodes = [8081, 8082, 8083, 8084].map {|port| {host: '127.0.0.1', pb_port: port}}
	client = Riak::Client.new(protocol: "pbc", nodes: nodes)
	bucket = client.bucket(BUCKET)
	batch.each do |filename|
    path = File.join(DIR, filename)
    rpath = File.join(FIN, filename)
		# drop .json
		key = filename[0..-6]
    begin
      obj = bucket.get_or_new(key)
    rescue Exception => e
      STDERR.puts("Shit error GETing #{filename}")
      STDERR.puts(e, e.message, e.backtrace)
      STDERR.flush
      raise e
    end
		obj.raw_data = File.read(path)
		obj.content_type = "application/json"
    begin
      obj.store()
    rescue Exception => e
      STDERR.puts("Shit error PUTing #{filename}")
      STDERR.puts(e, e.message, e.backtrace)
      STDERR.flush
      failed << path
      sleep(15)
    else
      File.rename(path, rpath)
    end
	end
end

STDERR.puts("Checking for bad input data in #{DIR}")
move_bad(paths, bad)
STDERR.puts("Finished checking for bad input data, moved bad files:\n#{bad.join("\n")}")
STDERR.puts("Adding #{n} items to bucket #{BUCKET} from #{DIR}")
#Parallel.each(batches) {|batch| put(batch)}
put(paths, failed)
STDERR.puts("Finished adding!")
STDERR.puts("The following #{failed.length} files failed:\n#{failed.join("\n")}\n")
