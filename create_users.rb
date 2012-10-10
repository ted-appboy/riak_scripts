#!/usr/bin/env ruby
require 'parallel'
require 'benchmark'

MAX_FACEBOOK_FRIENDS = 2000
MAX_TWITTER_FOLLOWERS = 10000
MAX_TWITTER_FOLLOWING = 10000
MAX_TWITTER_SHARE_COUNT = 1000
MAX_TWEETS = 100000
MAX_FACEBOOK_SHARE_COUNT = 1000
MAX_PURCHASE_TOTAL = 1000.0
MAX_SESSIONS = 100
MAX_USAGE_COUNT = 100
MAX_FEEDBACK_COUNT = 100

MIN_TIMES = 4
MAX_ADDITIONAL_TIMES = 10.0
LAST_TIME = 1346262613.941292
TIME_INTERVAL = 365*24*60*60
N_APPS = 4
IOS_VERSIONS = ['iOS 4.0', 'iOS 5.0', 'iOS 6.0']
COUNTRY_CODES = ["ABW", "AFG", "AGO", "AIA", "ALA", "ALB", "AND", "ARE", "ARG", "ARM", "ASM", "ATA", "ATF", "ATG", "AUS", "AUT", "AZE", "BDI", "BEL", "BEN", "BES", "BFA", "BGD", "BGR", "BHR", "BHS", "BIH", "BLM", "BLR", "BLZ", "BMU", "BOL", "BRA", "BRB", "BRN", "BTN", "BVT", "BWA", "CAF", "CAN", "CCK", "CHE", "CHL", "CHN", "CIV", "CMR", "COD", "COG", "COK", "COL", "COM", "CPV", "CRI", "CUB", "CUW", "CXR", "CYM", "CYP", "CZE", "DEU", "DJI", "DMA", "DNK", "DOM", "DZA", "ECU", "EGY", "ERI", "ESH", "ESP", "EST", "ETH", "FIN", "FJI", "FLK", "FRA", "FRO", "FSM", "GAB", "GBR", "GEO", "GGY", "GHA", "GIB", "GIN", "GLP", "GMB", "GNB", "GNQ", "GRC", "GRD", "GRL", "GTM", "GUF", "GUM", "GUY", "HKG", "HMD", "HND", "HRV", "HTI", "HUN", "IDN", "IMN", "IND", "IOT", "IRL", "IRN", "IRQ", "ISL", "ISR", "ITA", "JAM", "JEY", "JOR", "JPN", "KAZ", "KEN", "KGZ", "KHM", "KIR", "KNA", "KOR", "KWT", "LAO", "LBN", "LBR", "LBY", "LCA", "LIE", "LKA", "LTU", "LUX", "LVA", "MAC", "MAF", "MAR", "MCO", "MDA", "MDG", "MDV", "MEX", "MHL", "MKD", "MLI", "MLT", "MMR", "MNE", "MNG", "MNP", "MOZ", "MRT", "MSR", "MTQ", "MUS", "MWI", "MYS", "MYT", "NAM", "NCL", "NER", "NFK", "NGA", "NIC", "NIU", "NLD", "NOR", "NPL", "NRU", "NZL", "OMN", "PAK", "PAN", "PCN", "PER", "PHL", "PLW", "PNG", "POL", "PRI", "PRK", "PRT", "PRY", "PSE", "PYF", "QAT", "REU", "ROU", "RUS", "RWA", "SAU", "SDN", "SEN", "SGP", "SGS", "SHN", "SJM", "SLB", "SLE", "SLV", "SMR", "SOM", "SPM", "SRB", "SSD", "STP", "SUR", "SVK", "SVN", "SWE", "SWZ", "SXM", "SYC", "SYR", "TCA", "TCD", "TGO", "THA", "TJK", "TKL", "TKM", "TLS", "TON", "TTO", "TUN", "TUR", "TUV", "TWN", "TZA", "UGA", "UKR", "UMI", "URY", "USA", "UZB", "VAT", "VCT", "VEN", "VGB", "VIR", "VNM", "VUT", "WLF", "WSM", "YEM", "ZAF", "ZMB", "ZWE"] 
GENDERS = [Constants::Genders::MALE, Constants::Genders::FEMALE, Constants::Genders::OTHER, Constants::Genders::UNKNOWN]
SEED = 12345
DIR = ARGV[0]
N = ARGV[1].to_i
#N = 5021219
#N = 50000
WORKERS = 16
BATCH_SIZE = 10000

apps = App.collection.find.take(N_APPS)
APP_IDS = apps.map { |app| app["_id"] }

def gen(n, seed, dir)
  user_dir = File.join(dir, 'user')
	rng = Random.new(seed)

  c = 0
  batches = (0..n-1).each_slice(BATCH_SIZE)
  #idss = batches.map do |batch|
  batches.each do |batch|
	  user_jsons = batch.map do |i|

      date_last = Time.at(LAST_TIME)
      date_first = date_last - TIME_INTERVAL
      ntimes = MIN_TIMES + (MAX_ADDITIONAL_TIMES * rng.rand).to_i
      # will lean towards larger gaps in the beginning than the end, probably not a problem
      f_first = date_first.to_f
      interval = date_last.to_f - f_first
      times = (1..ntimes).map { Time.at(interval * rng.rand + f_first) }.sort
      email = ['foo@boo.hoo', 'ma.po@to.fu', 'hum3@bo.gart', 'agamemnon@ar.gos'].sample(random: rng)

      device_id = SecureRandom.uuid
      user_id = SecureRandom.uuid
      user_profile_id = SecureRandom.uuid
      user_app_stat_id = SecureRandom.uuid
      app_id = APP_IDS[i % APP_IDS.length]
=begin
      device = {
        "_id" => device_id,
        # app ids, 1 per device mod through
        "a"=>
          [app_id],
        # same as updated_at
        "created_at"=>times.last,
        # leave for now, vary later to measure table walk times?
        "os"=>IOS_VERSIONS.sample(random: rng),
        # device secret id must be present and unique string
        "s"=>i.to_s(16),
        # user ids, has_many_and_belongs_to_many
        "u_ids"=>[user_id],
        # a few seconds after user updated_at
        "updated_at"=>times[-1]
      }
=end

      user = {
        "_id"=>user_id,
        # app id
        "a_id"=>app_id,
        "created_at"=>times[-2],
        # device ids
        "d_ids"=>[device_id],
        "fn"=>i.to_s(16),
        "ln"=>SecureRandom.uuid,
        # increment
        "un"=>i,
        # randomize (same as updated_at below)
        "updated_at"=>times[-2],
        "user_profile"=>
          {"_id"=>user_profile_id,
            "an"=>false,
            "cc"=>COUNTRY_CODES.sample(random: rng),
            # randomize (same as created_at below)
            "created_at"=>times[-2],
            "eu"=>false,
            "g"=>GENDERS.sample(random: rng),
            # randomize (might be better to use a gaussian and get a normal distribution)
            "ks"=>1.0 + 99.0 * rng.rand,
            "kt"=>[],
            # randomize
            "nf"=>(MAX_FACEBOOK_FRIENDS * rng.rand).to_i,
            # randomize
            "nt"=>(MAX_TWEETS * rng.rand).to_i,
            # randomize
            "tf"=>(MAX_TWITTER_FOLLOWERS * rng.rand).to_i,
            # randomize
            "tfg"=>(MAX_TWITTER_FOLLOWING * rng.rand).to_i,
            "twl"=>"Twitter location",
            "uoi"=>false,
            # randomize (same as updated_at above)
            "updated_at"=>times[-2],
            "user_app_stat"=>
              {"_id"=>user_app_stat_id,
                # randomize (same as created_at above)
                "created_at"=>times[-2],
                # gen from device
                "d"=>["Debug, iOS 4.1"],
                "dp"=>[],
                # randomize
                "f"=>(MAX_FEEDBACK_COUNT * rng.rand).to_i,
                # randomize (somehow before created_at)
                "fs"=>times[-4],
                # randomize
                "fsh"=>(MAX_FACEBOOK_SHARE_COUNT * rng.rand).to_i,
                # randomize (somehow before created_at, but after fs)
                "ls"=>times[-3],
                # gen from device
                "m"=>"Debug, iOS 4.1",
                # gen from device (random in 
                "md"=>["iOS 4.1", "iOS 5.0", "iOS 6.0"].sample(random: rng),
                # gen from device
                "mv"=>["Debug", "1.0"].sample(random: rng),
                "n"=>[],
                # randomize $
                "pt"=>MAX_PURCHASE_TOTAL * rng.rand,
                # randomize # sessions
                "s"=>(MAX_SESSIONS * rng.rand).to_i,
                # session durations, may want to run queries against this at some point, leave empty for now
                "sdh"=>[],
                # list of timestamps, start with fs end with last update time
                "sh"=> times[0..-5].reverse,
                # twitter share count randomize
                "tsh"=>(MAX_TWITTER_SHARE_COUNT * rng.rand).to_i,
                # usage count randomize
                "u"=>(MAX_USAGE_COUNT * rng.rand).to_i,
                # same as prev updated_at
                "updated_at"=>times[-2]
              }
          }
      }
      c += 1
      [user_id, MultiJson.dump(user, :pretty => true)]
    end

    user_jsons.each do |_id, user_json|
      #device_ret = Device.collection.driver.insert(device)
      #to_dump = {
      #	'device' => device,
      #  'user' => user_json
      #}
      #to_dump.each do |name, json|
      #  File.open(File.join(dir, name, "#{_id}.json"), 'w') {|f| f.write(json)}
      #end
      File.open(File.join(user_dir, "#{_id}.json"), 'w') {|f| f.write(user_json)}
      #user_ret = User.collection.driver.insert(user)
    end  

      #pp device_ret
      #pp user_ret
    #user_jsons.map {|_id, _| _id}
	end
  puts "Worker wrote #{c} files."
  #idss.flatten
end

#['device', 'user'].each do |dirname|
['user'].each do |dirname|
	path = File.join(DIR, dirname)
	Dir.mkdir(path) unless Dir.exist?(path)
end

rngo = Random.new(SEED)
left = N
batch_size = N/WORKERS
args = (1..WORKERS).map do
	seed = rngo.rand
	n = left < 2*batch_size ? left : batch_size
	left -= n
	[n, seed, DIR]
end


#idss = []
time = Benchmark.realtime do
  #idss = Parallel.map(args) {|argv| gen(*argv)}
  Parallel.each(args) {|argv| gen(*argv)}
end
#ids = idss.flatten
#puts "ids.length: #{ids.length}"
#puts "ids.uniq.length: #{ids.uniq.length}"
puts "Time elapsed for generation #{time*1000}ms, #{time*1000/N}ms/user "

#puts "genders: "
#pp GENDERS
