# encoding: utf-8
require "open-uri"
require "avro"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"
require "net/http"
require "json"

# Documentation and rest of functionality will follow, but for now this will only
# decode/deserialize a serialized avro record prefixed by a magic byte followed by
# the avro schema id encoded as a four (4) byte integer.

# Possibly will allow via configuration to function either in old or new way.


# Read serialized Avro records as Logstash events
#
# This plugin is used to serialize Logstash events as 
# Avro datums, as well as deserializing Avro datums into 
# Logstash events.
#
# ==== Encoding
# 
# This codec is for serializing individual Logstash events 
# as Avro datums that are Avro binary blobs. It does not encode 
# Logstash events into an Avro file.
#
#
# ==== Decoding
#
# This codec is for deserializing individual Avro records. It is not for reading
# Avro files. Avro files have a unique format that must be handled upon input.
#
#
# ==== Usage
# Example usage with Kafka input.
#
# [source,ruby]
# ----------------------------------
# input {
#   kafka {
#     codec => {
#       avro => {
#         schema_uri => "/tmp/schema.avsc"
#       }
#     }
#   }
# }
# filter {
#   ...
# }
# output {
#   ...
# }
# ----------------------------------
class LogStash::Codecs::Avro < LogStash::Codecs::Base

  config_name "sls_avro"


  # schema path to fetch the schema from.
  # This can be a 'http' or 'file' scheme URI
  # example:
  #
  # * http - `http://example.com/schema.avsc`
  # * file - `/path/to/schema.avsc`
  #config :schema_uri, :validate => :string, :required => true

  #def open_and_read(uri_string)
  #  open(uri_string).read
  #end

  # URL of where to fetch the Avro schema. The schema id will be appended directly at
  # the end of the URL so make sure to get it right with ending slashes et cetera.
  config :schema_registry, :validate => :string, :required => true

  # A byte identifier at the top/beginning of the blob.
  config :magic_byte, :validate => :number, :default => 255, :required => false

  # Size in bytes of the schema id. Up to four (4) bytes.
  config :schema_id_size, :validate => :number, :default => 4, :required => false

  config :flatten, :validate => :boolean, :default => false, :required => false

  public
  def register
    @schema_list = { }
  end # def register

  public
  def decode(data)

    magic = data.getbyte(0)
    return unless magic == @magic_byte
    @schema_id_size = 4 if @schema_id_size < 0 || @schema_id_size > 4

    # A bit messy. Maybe just support schema_id as a four (4) byte integer.
    schema_id = -1
    schema_id = data.getbyte(1)                     if @schema_id_size > 0
    schema_id = schema_id | (data.getbyte(2) << 8)  if @schema_id_size > 1
    schema_id = schema_id | (data.getbyte(3) << 16) if @schema_id_size > 2
    schema_id = schema_id | (data.getbyte(4) << 24) if @schema_id_size > 3

    avro_schema = get_schema(schema_id)
    return if avro_schema == nil

    avro_data = data[@schema_id_size+1..-1]
    datum = StringIO.new(avro_data)
    decoder = Avro::IO::BinaryDecoder.new(datum)
    datum_reader = Avro::IO::DatumReader.new(avro_schema)
    parsed = datum_reader.read(decoder)

    root = {}

    # This is a very specific flattening and should not be in here.
    # Need to figure out how to do the same (splitting up into multiple events) in a filter instead.
    if @flatten

      #if fields.include?(k)

      data = {}
      #fields = ["gauges", "counters", "histograms", "meters", "timers"]
      parsed.each { |k,v|
        if v.is_a?(Hash) || v.is_a?(Array)
          data[k] = v
        else
          root[k] = v
        end
      }

      #root["data"] = data;

      data.each { |k,v|                         # For each type (gauges, counter, ...) do:
        if v.is_a? Array                        # Is it an Array?
          v.each { |x|                          # For each element in the Array do:
            if x.is_a? Hash                     # Is it a Hash?
              if x.key?("name")                 # Does it have "name" key?
                name = x["name"]                # Copy and fix the name.
                name.gsub! ".", "_"
                x.delete("name")                # Delete the "name" key.
                event = root.clone              # Create a new "event" with all the root content.
                event["es_subindex"] = k
                x.each { |key,value|            # For each remaining key|value do:
                  event[name+"_"+key] = value
                }
                yield LogStash::Event.new(event)
              end
            end
          }
        elsif v.is_a? Hash                      # Is it a Hash?
          v.each { |name,hash|
            if hash.is_a? Hash
              name.gsub! ".", "_"               # Fix the name (elasticsearch does not like dots).
              event = root.clone
              event["es_subindex"] = k
              hash.each { |key,value|
                event[name+"_"+key] = value
              }
              yield LogStash::Event.new(event)
            end
          }
        end
      }
    else
      parsed.each{|k,v| root[k] = v}
      yield LogStash::Event.new(root)
    end

  end

  #public
  #def encode(event)
  #  dw = Avro::IO::DatumWriter.new(@schema)
  #  buffer = StringIO.new
  #  encoder = Avro::IO::BinaryEncoder.new(buffer)
  #  dw.write(event.to_hash, encoder)
  #  @on_event.call(event, buffer.string)
  #end


  private
  def get_schema(id)
    if @schema_list.key?(id)
      return @schema_list[id]
    else
      schema_file = download_schema_file(id)
      if schema_file != nil && schema_file != ""
        json = JSON.parse(schema_file)
        if json != nil && json.key?("schema")
          avro_schema = Avro::Schema.parse(json["schema"].to_s)
          @schema_list[id] = avro_schema
          return avro_schema
        end
      end
    end
    return nil
  end

  private
  def download_schema_file(id)
    uri = URI.parse("#{@schema_registry}#{id.to_s}")
    http_object = Net::HTTP.new(uri.host, uri.port)
    http_object.use_ssl = true if uri.scheme == 'https'
    begin
      http_object.start do |http|
        request = Net::HTTP::Get.new uri.request_uri
        http.read_timeout = 30
        http.request request do |response|
          schema = ""
          response.read_body do |chunk|
            schema += chunk
          end
          #json = JSON.parse(schema)
          #return json["schema"].to_s;
          return schema;
        end
      end
    rescue Exception => e
      #puts "=> Exception: '#{e}'. Skipping download."
    end
    return nil
  end

end # class LogStash::Codecs::Avro
