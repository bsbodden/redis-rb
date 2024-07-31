# frozen_string_literal: true

class Redis
  module Commands
    module Search
      class Field
        attr_reader :name, :type, :options, :alias_name
        attr_accessor :query

        def initialize(name, type, query = nil, **options)
          @name = name.to_s
          @type = type
          @query = query
          @options = options
          @alias_name = options.delete(:as)
        end

        def to_args
          args = [@name]
          args << "AS" << @alias_name if @alias_name
          args << @type.to_s.upcase
          @options.each do |k, v|
            next if k == :phonetic # Skip phonetic here, it's handled in TextField

            case v
            when true
              args << k.to_s.upcase
            when false
              next
            else
              args << k.to_s.upcase << v.to_s
            end
          end
          args
        end
      end

      class TagField < Field
        def initialize(name, query = nil, **options)
          super(name, :tag, query, **options)
        end

        def eq(value)
          query.add_predicate(TagEqualityPredicate.new(@alias || name, value))
        end
      end

      class TextField < Field
        def initialize(name, query = nil, **options)
          super(name, :text, query, **options)
          if options[:phonetic]
            valid_matchers = ['dm:en', 'dm:fr', 'dm:pt', 'dm:es']
            unless valid_matchers.include?(options[:phonetic])
              raise ArgumentError, "Invalid phonetic matcher. Supported matchers are: #{valid_matchers.join(', ')}"
            end
          end
        end

        def to_args
          args = super
          args << "PHONETIC" << @options[:phonetic] if @options[:phonetic]
          args
        end

        def match(pattern)
          query.add_predicate(TextMatchPredicate.new(@alias || name, pattern))
        end
      end

      class NumericField < Field
        def initialize(name, query = nil, **options)
          super(name, :numeric, query, **options)
        end

        def gt(value)
          query.add_predicate(RangePredicate.new(@alias || name, "(#{value}", "+inf"))
        end

        def lt(value)
          query.add_predicate(RangePredicate.new(@alias || name, "-inf", "(#{value}"))
        end

        def between(min, max)
          query.add_predicate(RangePredicate.new(@alias || name, min, max))
        end
      end

      class GeoField < Field
        def initialize(name, query = nil, **options)
          super(name, :geo, query, **options)
        end
      end

      class VectorField < Field
        attr_reader :algorithm, :attributes

        def initialize(name, algorithm:, **options)
          super(name, :vector, **options)
          @algorithm = algorithm.to_s.upcase
          @attributes = {}
        end

        def add_attribute(key, value)
          @attributes[key.to_s.upcase] = value.to_s
        end

        def to_args
          [name, 'VECTOR', @algorithm, @attributes.size * 2] + @attributes.flat_map { |k, v| [k, v] }
        end
      end

      class Schema
        include Enumerable

        attr_reader :fields

        def initialize(fields = [])
          @fields = fields
        end

        def field(name)
          @fields.find { |f| f.name.to_s == name.to_s }
        end

        def each(&block)
          @fields.each(&block)
        end

        def to_args
          ['SCHEMA'] + @fields.flat_map(&:to_args)
        end

        def self.build(&block)
          definition = SchemaDefinition.new
          begin
            definition.instance_eval(&block)
          rescue ArgumentError => e
            raise Redis::CommandError, e.message
          end
          new(definition.fields)
        end
      end

      class SchemaDefinition
        attr_reader :fields

        def initialize
          @fields = []
        end

        def text_field(name, **options)
          valid_options = %i[weight sortable no_index as phonetic]
          invalid_options = options.keys - valid_options
          if invalid_options.any?
            raise ArgumentError, "Invalid options for text field: #{invalid_options.join(', ')}"
          end

          @fields << TextField.new(name, **options)
        end

        def numeric_field(name, **options)
          @fields << NumericField.new(name, **options)
        end

        def tag_field(name, **options)
          @fields << TagField.new(name, **options)
        end

        def geo_field(name, **options)
          @fields << GeoField.new(name, **options)
        end

        def vector_field(name, algorithm:, **_options, &block)
          field = VectorField.new(name, algorithm: algorithm)
          VectorFieldDefinition.new(field).instance_eval(&block) if block_given?
          @fields << field
        end
      end

      class VectorFieldDefinition
        def initialize(field)
          @field = field
        end

        def type(value)
          @field.add_attribute(:type, value)
        end

        def dim(value)
          @field.add_attribute(:dim, value)
        end

        def distance_metric(value)
          @field.add_attribute(:distance_metric, value)
        end
      end

      class IndexDefinition
        attr_reader :prefix, :index_type

        def initialize(prefix: [], index_type: :hash)
          @prefix = Array(prefix)
          @index_type = index_type
        end

        def to_args
          args = []
          args += ['ON', index_type.to_s.upcase] if index_type
          args += ['PREFIX', prefix.size] + prefix if prefix.any?
          args
        end
      end

      class Predicate
        attr_reader :field

        def initialize(field)
          @field = field
        end

        def to_s
          raise NotImplementedError
        end
      end

      class TagEqualityPredicate < Predicate
        def initialize(field, value)
          super(field)
          @value = value
        end

        def to_s
          "(@#{@field}:{#{@value}})"
        end
      end

      class TextMatchPredicate < Predicate
        def initialize(field, pattern)
          super(field)
          @pattern = pattern
        end

        def to_s
          "(@#{@field}:#{@pattern})"
        end
      end

      class RangePredicate < Predicate
        def initialize(field, min, max)
          super(field)
          @min = min
          @max = max
        end

        def to_s
          "(@#{@field}:[#{@min} #{@max}])"
        end
      end

      class PredicateCollection
        attr_reader :type, :predicates

        def initialize(type)
          @type = type
          @predicates = []
        end

        def add(predicate)
          @predicates << predicate
        end

        def to_s
          joiner = @type == :or ? ' | ' : ' '
          "(#{@predicates.join(joiner)})"
        end
      end

      class Query
        attr_reader :options

        def initialize(base = nil)
          @base = base
          @predicate_collection = [PredicateCollection.new(:and)]
          @filters = []
          @options = {}
          @return_fields = []
          @summarize_options = nil
          @highlight_options = nil
          @language = nil
          @verbatim = false
          @no_stopwords = false
          @with_payloads = false
          @slop = nil
          @in_order = false
          @no_content = false
        end

        def self.build(&block)
          instance = new
          instance.instance_eval(&block)
          instance
        end

        def filter(field, min, max = nil)
          max ||= min
          @filters << [field, min, max]
          self
        end

        def paging(offset, limit)
          @options[:limit] = [offset, limit]
          self
        end

        def sort_by(field, order = :asc)
          @options[:sortby] = [field, order.to_s.upcase]
          self
        end

        def return(*fields)
          @return_fields = fields
          self
        end

        def language(lang)
          @language = lang
          self
        end

        def verbatim
          @verbatim = true
          self
        end

        def no_stopwords
          @no_stopwords = true
          self
        end

        def with_scores
          @options[:withscores] = true
          self
        end

        def with_payloads
          @with_payloads = true
          self
        end

        def slop(value)
          @slop = value
          self
        end

        def in_order
          @in_order = true
          self
        end

        def no_content
          @no_content = true
          self
        end

        def highlight(fields: nil, tags: ["<b>", "</b>"])
          @highlight_options = {
            fields: Array(fields),
            tags: tags
          }
          self
        end

        def summarize(fields: nil, separator: "...", len: 20, frags: 3)
          @summarize_options = {
            fields: Array(fields),
            separator: separator,
            len: len,
            frags: frags
          }
          self
        end

        ## -------------
        ## query builder
        ## -------------

        def or_(&block)
          new_collection(:or, &block)
        end

        def and_(&block)
          new_collection(:and, &block)
        end

        def add_predicate(predicate)
          @predicate_collection.last.add(predicate)
          self
        end

        def new_collection(type)
          collection = PredicateCollection.new(type)
          @predicate_collection << collection
          yield if block_given?
          @predicate_collection.pop
          @predicate_collection.last.add(collection)
          self
        end

        def tag(field)
          TagField.new(field, self)
        end

        def text(field)
          TextField.new(field, self)
        end

        def numeric(field)
          NumericField.new(field, self)
        end

        def to_redis_args
          args = []
          args << if @predicate_collection.first.predicates.empty?
            @base || "*"
          else
            @predicate_collection.first.to_s
          end

          args << "NOCONTENT" if @no_content
          args << "VERBATIM" if @verbatim
          args << "NOSTOPWORDS" if @no_stopwords
          args << "WITHSCORES" if @options[:withscores]
          args << "WITHPAYLOADS" if @with_payloads

          @filters.each do |field, min, max|
            args.concat(["FILTER", field, min, max])
          end

          if @return_fields && !@return_fields.empty?
            args << "RETURN" << @return_fields.size
            args.concat(@return_fields)
          end

          if @summarize_options
            args << "SUMMARIZE"
            if @summarize_options[:fields].any?
              args << "FIELDS" << @summarize_options[:fields].size
              args.concat(@summarize_options[:fields].map(&:to_s))
            end
            args << "FRAGS" << @summarize_options[:frags]
            args << "LEN" << @summarize_options[:len]
            args << "SEPARATOR" << @summarize_options[:separator]
          end

          if @highlight_options
            args << "HIGHLIGHT"
            if @highlight_options[:fields].any?
              args << "FIELDS" << @highlight_options[:fields].size
              args.concat(@highlight_options[:fields].map(&:to_s))
            end
            args << "TAGS" << @highlight_options[:tags][0] << @highlight_options[:tags][1]
          end

          args << "SLOP" << @slop if @slop
          args << "LANGUAGE" << @language if @language
          args << "INORDER" if @in_order

          args << "SORTBY" << @options[:sortby][0] << @options[:sortby][1] if @options[:sortby]
          args << "LIMIT" << @options[:limit][0] << @options[:limit][1] if @options[:limit]

          args
        end

        def evaluate(&block)
          if block_given?
            instance_eval(&block)
          end
        end
      end

      class Index
        attr_reader :name, :prefix

        def initialize(redis, name, schema, storage_type, prefix: nil, stopwords: nil)
          @redis = redis
          @name = name
          @prefix = prefix
          @schema = schema
          @storage_type = storage_type
          @stopwords = stopwords
        end

        def self.create(redis, name, schema, storage_type, prefix: nil, stopwords: nil, **options)
          raise ArgumentError, "Invalid schema" unless schema.is_a?(Schema)

          redis.ft_create(name, schema, storage_type, prefix: prefix, stopwords: stopwords, **options)
          new(redis, name, schema, storage_type, prefix: prefix, stopwords: stopwords)
        end

        def add(doc_id, **fields)
          key = @prefix ? "#{@prefix}:#{doc_id}" : doc_id

          # Validate fields
          fields.each do |field_name, value|
            field = @schema.field(field_name)
            if field.is_a?(NumericField) && !value.is_a?(Numeric)
              raise Redis::CommandError, "Invalid value for numeric field '#{field_name}': #{value}"
            end
          end

          begin
            @redis.hset(key, fields)
          rescue Redis::CommandError => e
            raise Redis::CommandError, "Error adding document: #{e.message}"
          end
        end

        def search(query = nil, query_params: nil, params: nil, dialect: nil, &block)
          if block_given?
            query = Query.build(&block)
          elsif query.is_a?(String)
            query = Query.new(query)
          end

          raise ArgumentError, "Invalid query" unless query.is_a?(Query)

          redis_args = query.to_redis_args
          query_string = redis_args.shift

          options = query.options
          options[:filter] = query.instance_variable_get(:@filters)
          options[:sortby] = query.instance_variable_get(:@options)[:sortby]

          options[:params] = params if params
          options[:dialect] = dialect if dialect

          options[:return] = query.instance_variable_get(:@return_fields)

          options[:highlight] = query.instance_variable_get(:@highlight_options)
          options[:summarize] = query.instance_variable_get(:@summarize_options)
          options[:verbatim] = query.instance_variable_get(:@verbatim)
          options[:no_stopwords] = query.instance_variable_get(:@no_stopwords)

          if query_params
            options[:params] = query_params.flatten
          end

          result = @redis.ft_search(@name, query_string, **options)

          # Strip prefix from document IDs if a prefix is set
          if @prefix
            result[1..-1] = result[1..-1].map do |item|
              item.is_a?(String) && item.start_with?("#{@prefix}:") ? item.sub("#{@prefix}:", "") : item
            end
          end

          result
        end

        def info
          @redis.ft_info(@name)
        end

        def drop(delete_documents: false)
          @redis.ft_dropindex(@name, delete_documents: delete_documents)
        end

        def aggregate(query, *args)
          @redis.ft_aggregate(@name, query, *args)
        end

        def explain(query)
          @redis.ft_explain(@name, query)
        end

        def alter(*args)
          @redis.ft_alter(@name, *args)
        end

        def spellcheck(query, *args)
          @redis.ft_spellcheck(@name, query, *args)
        end

        def synupdate(group_id, *terms)
          @redis.ft_synupdate(@name, group_id, *terms)
        end

        def syndump
          @redis.ft_syndump(@name)
        end

        def tagvals(field_name)
          @redis.ft_tagvals(@name, field_name)
        end

        def profile(*args)
          @redis.ft_profile(@name, *args)
        end

        private

        def create_from_schema(schema)
          @redis.ft_create(@name, schema)
        end
      end

      def ft_create(index_name, schema, storage_type, prefix: nil, stopwords: nil, **options)
        raise ArgumentError, "schema must be a Schema object" unless schema.is_a?(Schema)

        args = [index_name]
        args += ["ON", storage_type]
        args += ["PREFIX", 1, "#{prefix}:"] if prefix

        # Stopwords
        if stopwords
          args += ['STOPWORDS', stopwords.size]
          stopwords.each do |stopword|
            args << stopword
          end
        end

        # Schema Fields
        args += ["SCHEMA"]
        schema.fields.each do |field|
          args += field.to_args
        end
        args += options[:definition].to_args if options[:definition]

        call('FT.CREATE', *args)
      end

      def create_index(name, schema, storage_type: "hash", prefix: nil, stopwords: nil)
        raise Redis::CommandError, "schema must be a Schema object" unless schema.is_a?(Schema)

        begin
          Index.create(self, name, schema, storage_type, prefix: prefix, stopwords: stopwords)
        rescue ArgumentError => e
          raise Redis::CommandError, e.message
        end
      end

      def ft_search(index_name, query, **options)
        args = [index_name, query]

        args << "WITHSCORES" if options[:withscores]
        args << "LIMIT" << options[:limit][0] << options[:limit][1] if options[:limit]

        if options[:sortby]
          args << "SORTBY" << options[:sortby][0] << options[:sortby][1]
        end

        options[:filter]&.each do |field, min, max|
          args << "FILTER" << field << min << max
        end

        args << "PARAMS" << options[:params].length << options[:params] if options[:params]
        args << "DIALECT" << options[:dialect] if options[:dialect]
        args << "VERBATIM" if options[:verbatim]
        args << "NOSTOPWORDS" if options[:no_stopwords]

        if options[:return] && !options[:return].empty?
          args << "RETURN" << options[:return].size
          args.concat(options[:return])
        end

        if options[:summarize]
          args << "SUMMARIZE"
          if options[:summarize][:fields]&.any?
            args << "FIELDS" << options[:summarize][:fields].size
            args.concat(options[:summarize][:fields].map(&:to_s))
          end
          args << "FRAGS" << options[:summarize][:frags]
          args << "LEN" << options[:summarize][:len]
          args << "SEPARATOR" << options[:summarize][:separator]
        end

        if options[:highlight]
          args << "HIGHLIGHT"
          if options[:highlight][:fields]&.any?
            args << "FIELDS" << options[:highlight][:fields].size
            args.concat(options[:highlight][:fields].map(&:to_s))
          end
          args << "TAGS" << options[:highlight][:tags][0] << options[:highlight][:tags][1]
        end

        send_command(["FT.SEARCH"] + args.flatten.compact)
      end

      def ft_add(index_name, doc_id, score, options = {})
        args = ["FT.ADD", index_name, doc_id, score]
        args << 'REPLACE' if options[:replace]
        args << 'LANGUAGE' << options[:language] if options[:language]
        args << 'FIELDS' << options[:fields] if options[:fields]
        send_command(args)
      end

      def ft_info(index_name)
        result = send_command(["FT.INFO", index_name])
        result.each_slice(2).to_h
      end

      def ft_dropindex(index_name, delete_documents: false)
        args = ["FT.DROPINDEX", index_name]
        args << 'DD' if delete_documents
        send_command(args)
      end

      def ft_aggregate(index_name, query, *args)
        send_command(["FT.AGGREGATE", index_name, query, *args])
      end

      def ft_explain(index_name, query)
        send_command(["FT.EXPLAIN", index_name, query])
      end

      def ft_alter(index_name, *args)
        send_command(["FT.ALTER", index_name, *args])
      end

      def ft_cursor_read(index_name, cursor_id)
        send_command(["FT.CURSOR", "READ", index_name, cursor_id])
      end

      def ft_cursor_del(index_name, cursor_id)
        send_command(["FT.CURSOR", "DEL", index_name, cursor_id])
      end

      def ft_profile(index_name, *args)
        send_command(["FT.PROFILE", index_name] + args)
      end

      def ft_sugadd(key, string, score, options = {})
        args = ["FT.SUGADD", key, string, score]
        args << 'INCR' if options[:incr]
        args << 'PAYLOAD' << options[:payload] if options[:payload]
        send_command(args)
      end

      def ft_sugget(key, prefix, options = {})
        args = ["FT.SUGGET", key, prefix]
        args << 'FUZZY' if options[:fuzzy]
        args << 'WITHSCORES' if options[:with_scores]
        args << 'WITHPAYLOADS' if options[:with_payloads]
        args << 'MAX' << options[:max] if options[:max]
        send_command(args)
      end

      def ft_spellcheck(index_name, query, *args)
        send_command(["FT.SPELLCHECK", index_name, query] + args)
      end

      def ft_synupdate(index_name, group_id, *terms)
        send_command(["FT.SYNUPDATE", index_name, group_id] + terms)
      end

      def ft_syndump(index_name)
        send_command(["FT.SYNDUMP", index_name])
      end

      def ft_tagvals(index_name, field_name)
        send_command(["FT.TAGVALS", index_name, field_name])
      end

      def ft_aliasadd(alias_name, index_name)
        send_command(["FT.ALIASADD", alias_name, index_name])
      end

      def ft_aliasupdate(alias_name, index_name)
        send_command(["FT.ALIASUPDATE", alias_name, index_name])
      end

      def ft_aliasdel(alias_name)
        send_command(["FT.ALIASDEL", alias_name])
      end
    end
  end
end
