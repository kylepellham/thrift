require "log"
require "./protocol/base_protocol.cr"
require "./types.cr"

module Thrift
  module Processor
    @handler : Nil
    def initialize(handler, logger = nil)
      @handler = handler
      if logger.nil?
        @logger = Log.for(Processor, Log::Severity::Warn)
      else
        @logger = logger
      end
    end

    def process(iprot : BaseProtocol, oprot : BaseProtocol)
      name, type, seqid  = iprot.read_message_begin
      if self.responds?("process_#{name}")
        begin
          send("process_#{name}", seqid, iprot, oprot)
        rescue ex
          x = ApplicationException.new(ApplicationException::INTERNAL_ERROR, "Internal error")
          @logger.debug "Internal error : #{e.message}\n#{ex.backtrace.join("\n")}"
          write_error(x, oprot, name, seqid)
        end
        true
      else
        iprot.skip(Types::STRUCT)
        iprot.read_message_end
        x = ApplicationException.new(ApplicationException::UNKNOWN_METHOD, "Unknown function "+name)
        write_error(x, oprot, name, seqid)
        false
      end
    end

    def read_args(iprot, args_class)
      args = args_class.new
      args.read(iprot)
      iprot.read_message_end
      args
    end

    def write_result(result, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::REPLY, seqid)
      result.write(oprot)
      oprot.write_message_end
      oprot.trans.flush
    end

    def write_error(err, oprot, name, seqid)
      oprot.write_message_begin(name, MessageTypes::EXCEPTION, seqid)
      err.write(oprot)
      oprot.write_message_end
      oprot.trans.flush
    end
    macro included
      puts "included"
      def self.responds?(method_check : String)
        methods = [
          {% for method in @type.methods %}
            method.name.stringify
          {% end %}
        ] of String
        return methods.includes? method_check
      end
    end
  end
end