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
#
# ==== Usage
# Example usage with Kafka input.
#
# [source,ruby]
# ----------------------------------
# input {
#   kafka {
#     codec => sls_avro {
#       debug_mode => false
#       schema_registry => "http://127.0.0.1:8181/schemas/ids/"
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

  # URL of where to fetch the Avro schema. The schema id will be appended directly at
  # the end of the URL so make sure to get it right with ending slashes et cetera.
  config :schema_registry, :validate => :string, :required => true

  # Used for extra debug output and stuff.
  config :debug_mode, :validate => :boolean, :default => false, :required => false

  public
  def register
    @schema_list = { }
  end # def register

  public
  def decode(data)

    puts "New Message Received!" if @debug_mode

    datum = StringIO.new(data)
    magic = datum.read(1).unpack("C")[0]
    puts "MAGIC BYTE (1): #{magic}" if @debug_mode
    return unless (magic == 0 || magic == 255)

    schema_id = -1

    if magic == 0
      schema_id = datum.read(4).unpack("I>")[0]
    elsif magic == 255
      schema_id = datum.read(4).unpack("I<")[0]
    end
    puts "Schema ID: #{schema_id}" if @debug_mode


    puts "Exiting due to Unexpected Schema ID!" if schema_id < 0 && @debug_mode
    return if schema_id < 0

    avro_schema = get_schema(schema_id)
    puts "GOT NULL SCHEMA!" if avro_Schema = nil && @debug_mode
    return if avro_schema == nil

    decoder = Avro::IO::BinaryDecoder.new(datum)
    datum_reader = Avro::IO::DatumReader.new(avro_schema)
    parsed = datum_reader.read(decoder)
    yield LogStash::Event.new(parsed)
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
