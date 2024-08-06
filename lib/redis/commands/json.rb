# frozen_string_literal: true

require 'json'

class Redis
  module Commands
    module JSON
      class JSONDecoder < ::JSON::Ext::Parser
        def parse
          result = super
          symbolize_keys(result)
        end

        private

        def symbolize_keys(obj)
          case obj
          when Hash
            obj.transform_keys(&:to_sym).transform_values { |v| symbolize_keys(v) }
          when Array
            obj.map { |v| symbolize_keys(v) }
          else
            obj
          end
        end
      end

      def json_set(key, path, value, nx: false, xx: false)
        args = ['JSON.SET', key, path, value.to_json]
        args << 'NX' if nx
        args << 'XX' if xx
        send_command(args)
      end

      def json_get(key, *paths)
        args = ['JSON.GET', key]
        args.concat(paths) unless paths.empty?
        parse_json(send_command(args))
      end

      def json_mget(keys, path)
        args = ['JSON.MGET'].concat(keys) << path
        send_command(args).map { |item| parse_json(item) }
      end

      def json_del(key, path = '$')
        send_command(['JSON.DEL', key, path])
      end

      def json_type(key, path = '$')
        send_command(['JSON.TYPE', key, path])
      end

      def json_numincrby(key, path, number)
        parse_json(send_command(['JSON.NUMINCRBY', key, path, number.to_s]))
      end

      def json_nummultby(key, path, number)
        parse_json(send_command(['JSON.NUMMULTBY', key, path, number.to_s]))
      end

      def json_strappend(key, path, value)
        send_command(['JSON.STRAPPEND', key, path, value.to_json])
      end

      def json_strlen(key, path = '$')
        send_command(['JSON.STRLEN', key, path])
      end

      def json_arrappend(key, path, *values)
        send_command(['JSON.ARRAPPEND', key, path].concat(values.map(&:to_json)))
      end

      def json_arrindex(key, path, value, start = 0, stop = 0)
        send_command(['JSON.ARRINDEX', key, path, value.to_json, start.to_s, stop.to_s])
      end

      def json_arrinsert(key, path, index, *values)
        send_command(['JSON.ARRINSERT', key, path, index.to_s].concat(values.map(&:to_json)))
      end

      def json_arrlen(key, path = '$')
        send_command(['JSON.ARRLEN', key, path])
      end

      def json_arrpop(key, path = '$', index = -1)
        parse_json(send_command(['JSON.ARRPOP', key, path, index.to_s]))
      end

      def json_arrtrim(key, path, start, stop)
        send_command(['JSON.ARRTRIM', key, path, start.to_s, stop.to_s])
      end

      def json_objkeys(key, path = '$')
        send_command(['JSON.OBJKEYS', key, path])
      end

      def json_objlen(key, path = '$')
        send_command(['JSON.OBJLEN', key, path])
      end

      def json_mset(triplets)
        pieces = []
        triplets.each do |key, path, value|
          pieces.concat([key, path.to_s, value.to_json])
        end
        send_command(["JSON.MSET", *pieces])
      end

      def json_merge(key, path, value)
        send_command(["JSON.MERGE", key, path, value.to_json])
      end

      def json_toggle(key, path)
        send_command(['JSON.TOGGLE', key, path])
      end

      def json_clear(key, path = '$')
        send_command(['JSON.CLEAR', key, path])
      end

      def json_resp(key, path = '$')
        send_command(['JSON.RESP', key, path])
      end

      def json_debug(subcommand, key, path = '$')
        send_command(['JSON.DEBUG', subcommand, key, path])
      end

      private

      def parse_json(value)
        case value
        when String
          result = JSONDecoder.new(value).parse
          result.is_a?(Array) && result.length == 1 ? result.first : result
        when Array
          value.map { |v| parse_json(v) }
        else
          value
        end
      rescue ::JSON::ParserError
        value
      end
    end
  end
end
