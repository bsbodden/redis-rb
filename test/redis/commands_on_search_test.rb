# frozen_string_literal: true

require "helper"

class TestCommandsOnSearch < Minitest::Test
  include Helper::Client
  include Redis::Commands::Search

  def setup
    super
    @index_name = "test_index"
    r.select(0)
    begin
      r.ft_dropindex(@index_name, delete_documents: true)
    rescue
      nil
    end
  end

  def test_ft_create_and_info
    schema = Schema.build do
      text_field :title, weight: 5.0
      text_field :body
    end

    index = r.create_index(@index_name, schema, prefix: "hsh1")
    assert_equal @index_name, index.info['index_name']

    info = index.info
    assert_kind_of Hash, info
    assert_equal @index_name, info['index_name']
    attributes = info['attributes']
    assert_kind_of Array, attributes

    identifiers = attributes.map { |attr| attr[1] }
    assert_equal ['title', 'body'], identifiers

    title_attribute = attributes.find { |attr| attr[1] == 'title' }
    assert_includes title_attribute, 'WEIGHT'
    assert_equal '5', title_attribute[title_attribute.index('WEIGHT') + 1]
  end

  def test_ft_add_and_search
    schema = Schema.build do
      text_field :title
      text_field :body
    end
    index = r.create_index(@index_name, schema, prefix: "hsh2")

    index.add('doc1', title: 'Hello', body: 'World')
    index.add('doc2', title: 'Goodbye', body: 'World')

    result = index.search('Hello')
    assert_equal 1, result[0]
    assert_equal 'doc1', result[1]

    result = index.search('World')
    assert_equal 2, result[0]
    assert_includes result, 'doc1'
    assert_includes result, 'doc2'
  end

  def test_ft_search_with_options
    schema = Schema.build do
      text_field :title
      text_field :body
      numeric_field :score
    end
    index = r.create_index(@index_name, schema, prefix: "hsh3")

    index.add('doc1', title: 'Hello', body: 'World', score: 0.5)
    index.add('doc2', title: 'Hello', body: 'Redis', score: 1.0)

    query = Query.new('Hello').with_scores.paging(0, 1)
    result = index.search(query)
    assert_equal 2, result[0]
    assert_equal 1, result[1..-1].length / 3 # Each result now includes id, score, and fields
    assert_kind_of String, result[1]  # Document ID
    assert_kind_of String, result[2]  # Score (as string)
    assert_kind_of Array, result[3]   # Fields

    # Test returning specific fields
    query = Query.new('Hello').return(:title, :score)
    result = index.search(query)
    assert_equal 2, result[0]
    assert_equal 4, result[2].length # Only title and score should be returned
    assert_includes result[2], 'title'
    assert_includes result[2], 'score'
    refute_includes result[2], 'body'
  end

  def test_ft_search_with_filters
    schema = Schema.build do
      text_field :title
      numeric_field :score
    end
    index = r.create_index(@index_name, schema, prefix: "hsh4")

    index.add('doc1', title: 'Hello', score: 0.5)
    index.add('doc2', title: 'Hello', score: 1.0)

    query = Query.new.filter(:score, 0.7, 1.5)
    result = index.search(query)
    assert_equal 1, result[0]
    assert_equal 'doc2', result[1]
  end

  def test_ft_search_with_sorting
    schema = Schema.build do
      text_field :title
      numeric_field :score, sortable: true
    end
    index = r.create_index(@index_name, schema, prefix: "hsh5")

    index.add('doc1', title: 'Hello', score: 0.5)
    index.add('doc2', title: 'Hello', score: 1.0)
    index.add('doc3', title: 'Hello', score: 0.75)

    query = Query.new('Hello').sort_by(:score, :desc)
    result = index.search(query)
    assert_equal 3, result[0]
    assert_equal 'doc2', result[1]
    assert_equal 'doc3', result[3]
    assert_equal 'doc1', result[5]
  end

  def test_ft_fuzzy_search
    schema = Schema.build do
      text_field :name
    end
    index = r.create_index(@index_name, schema, prefix: "hsh21")

    index.add('doc1', name: 'John')
    index.add('doc2', name: 'Jon')
    index.add('doc3', name: 'Jahn')
    index.add('doc4', name: 'Jan')

    # Test with Levenshtein distance of 1
    query = Query.new('%john%')
    result = index.search(query)
    assert_equal 3, result[0]
    assert_includes result, 'doc1'  # Exact match
    assert_includes result, 'doc2'  # One character difference
    assert_includes result, 'doc3'  # Two character difference
    refute_includes result, 'doc4'  # Not included with single %

    # Test with Levenshtein distance of 2
    query = Query.new('%%john%%')
    result = index.search(query)
    assert_equal 4, result[0]
    assert_includes result, 'doc1'
    assert_includes result, 'doc2'
    assert_includes result, 'doc3'
    assert_includes result, 'doc4'

    # Test with maximum Levenshtein distance of 3
    query = Query.new('%%%john%%%')
    result = index.search(query)
    assert_equal 4, result[0]
    assert_includes result, 'doc1'
    assert_includes result, 'doc2'
    assert_includes result, 'doc3'
    assert_includes result, 'doc4'

    # Test case sensitivity
    query = Query.new('%JOHN%')
    result = index.search(query)
    assert_equal 3, result[0]
    assert_includes result, 'doc1'
    assert_includes result, 'doc2'
    assert_includes result, 'doc3'

    # Test with a more distant term
    query = Query.new('%smith%')
    result = index.search(query)
    assert_equal 0, result[0]
  end

  def test_ft_prefix_search
    schema = Schema.build do
      text_field :name
    end
    index = r.create_index(@index_name, schema, prefix: "hsh22")

    index.add('doc1', name: 'John')
    index.add('doc2', name: 'Jonathan')
    index.add('doc3', name: 'Bob')

    result = index.search('jo*')
    assert_equal 2, result[0]
    assert_includes result, 'doc1'
    assert_includes result, 'doc2'
  end

  def test_ft_pagination
    schema = Schema.build do
      text_field :title
    end
    index = r.create_index(@index_name, schema, prefix: "hsh23")

    10.times { |i| index.add("doc#{i}", title: "Title #{i}") }

    query = Query.new('*').paging(0, 5)
    result = index.search(query)
    assert_equal 10, result[0] # Total number of documents
    assert_equal 5, (result.length - 1) / 2 # Number of returned documents

    query = Query.new('*').paging(5, 5)
    result = index.search(query)
    assert_equal 10, result[0] # Total number of documents
    assert_equal 5, (result.length - 1) / 2 # Number of returned documents
    refute_equal result[1], "doc0" # First document of second page should not be the first document overall
  end

  def test_ft_aggregate
    schema = Schema.build do
      text_field :title
      text_field :body
    end
    index = r.create_index(@index_name, schema, prefix: "hsh6")

    index.add('doc1', title: 'Hello', body: 'World')
    index.add('doc2', title: 'Hello', body: 'Redis')

    result = index.aggregate('*', 'GROUPBY', 1, '@title', 'REDUCE', 'COUNT', 0, 'AS', 'count')
    assert_equal 1, result[0]
    assert_equal ['title', 'Hello', 'count', '2'], result[1]
  end

  def test_ft_explain
    schema = Schema.build do
      text_field :title
      text_field :body
    end
    index = r.create_index(@index_name, schema, prefix: "hsh7")

    explanation = index.explain('@title:Hello @body:World')
    assert_includes explanation, "@title:UNION"
    assert_includes explanation, "@title:hello"
    assert_includes explanation, "@body:UNION"
    assert_includes explanation, "@body:world"
  end

  def test_ft_alter
    schema = Schema.build do
      text_field :title
    end
    index = r.create_index(@index_name, schema, prefix: "hsh8")

    assert_equal 'OK', index.alter('SCHEMA', 'ADD', 'body', 'TEXT')

    info = index.info
    attributes = info['attributes']
    identifiers = attributes.map { |attr| attr[1] }
    assert_includes identifiers, 'body'
  end

  def test_ft_sugadd_and_sugget
    r.ft_sugadd('suggestions', 'Redis', 1.0)
    r.ft_sugadd('suggestions', 'Redisearch', 1.0)

    result = r.ft_sugget('suggestions', 'r')
    assert_equal ['Redis', 'Redisearch'], result

    result = r.ft_sugget('suggestions', 'r', with_scores: true)
    assert_equal 4, result.size
    assert_equal 'Redis', result[0]
    assert_equal 'Redisearch', result[2]
    assert result[1].to_f > 0
    assert result[3].to_f > 0
  end

  def test_ft_dropindex
    schema = Schema.build do
      text_field :title
      text_field :body
    end
    index = r.create_index(@index_name, schema, prefix: "hsh9")

    assert_equal 'OK', index.drop
    assert_raises(Redis::CommandError) { index.info }
  end

  def test_ft_aggregate_complex
    schema = Schema.build do
      text_field :title
      tag_field :category
    end
    index = r.create_index(@index_name, schema, prefix: "hsh10")

    index.add('doc1', title: 'Hello', category: 'A')
    index.add('doc2', title: 'World', category: 'B')
    index.add('doc3', title: 'Redis', category: 'A')

    result = index.aggregate('*', 'GROUPBY', 1, '@category', 'REDUCE', 'COUNT', 0, 'AS', 'count', 'SORTBY', 2, '@count', 'DESC')
    assert_equal [2, ["category", "A", "count", "2"], ["category", "B", "count", "1"]], result
  end

  def test_ft_spellcheck
    schema = Schema.build do
      text_field :title
    end
    index = r.create_index(@index_name, schema, prefix: "hsh11")

    index.add('doc1', title: 'Hello World')

    result = index.spellcheck('Helo')

    assert_kind_of Array, result
    assert_equal 1, result.length

    term_results = result[0]
    assert_equal 'TERM', term_results[0]  # The literal string "TERM"
    assert_equal 'helo', term_results[1]  # The misspelled term

    suggestions = term_results[2]
    assert_kind_of Array, suggestions
    assert_operator suggestions.length, :>, 0, "Expected at least one suggestion"

    first_suggestion = suggestions[0]
    assert_kind_of Array, first_suggestion
    assert_equal 2, first_suggestion.length
    assert_equal '1', first_suggestion[0] # The score (as a string)
    assert_equal 'hello', first_suggestion[1] # The suggested correction
  end

  def test_ft_synupdate_and_syndump
    schema = Schema.build do
      text_field :title
    end
    index = r.create_index(@index_name, schema, prefix: "hsh12")

    index.synupdate('group1', 'hello', 'hallo')

    result = index.syndump
    assert_equal({ 'hello' => ['group1'], 'hallo' => ['group1'] }, Hash[*result])
  end

  def test_ft_tagvals
    schema = Schema.build do
      tag_field :category
    end
    index = r.create_index(@index_name, schema, prefix: "hsh13")

    index.add('doc1', category: 'a')
    index.add('doc2', category: 'b')

    result = index.tagvals('category')
    assert_equal ['a', 'b'], result.sort
  end

  def test_ft_alias
    schema = Schema.build do
      text_field :title
    end
    r.create_index(@index_name, schema, prefix: "hsh14")

    assert_equal 'OK', r.ft_aliasadd('alias1', @index_name)
    assert_equal 'OK', r.ft_aliasupdate('alias1', @index_name)
    assert_equal 'OK', r.ft_aliasdel('alias1')
  end

  def test_cursor_api
    schema = Schema.build do
      text_field :title
      text_field :content
    end
    index = r.create_index(@index_name, schema, prefix: "hsh15")

    100.times do |i|
      index.add("doc#{i}", title: "Title #{i}", content: "Content #{i}")
    end

    # Test cursor creation
    aggregate_result = index.aggregate('*', 'WITHCURSOR', 'COUNT', 30)
    assert_kind_of Array, aggregate_result
    assert_equal 2, aggregate_result.length
    results, cursor_id = aggregate_result

    assert_kind_of Array, results
    assert_operator results.length, :<=, 31 # It might return fewer than 30 results
    assert_kind_of Integer, cursor_id
    assert cursor_id > 0

    # Test cursor read
    next_results = r.ft_cursor_read(@index_name, cursor_id)
    assert_kind_of Array, next_results
    assert_equal 2, next_results.length
    next_results_data, next_cursor_id = next_results
    assert_kind_of Array, next_results_data
    assert_kind_of Integer, next_cursor_id

    # Test cursor deletion
    assert_equal 'OK', r.ft_cursor_del(@index_name, cursor_id)
  end

  def test_vector_similarity
    schema = Schema.build do
      text_field :title
      vector_field :vec, algorithm: :hnsw do
        type :float32
        dim 4
        distance_metric :l2
      end
    end
    index = r.create_index(@index_name, schema, prefix: "hsh16")

    index.add('doc1', title: 'Vector 1', vec: [1.0, 2.0, 3.0, 4.0].pack('f*'))
    index.add('doc2', title: 'Vector 2', vec: [2.0, 3.0, 4.0, 5.0].pack('f*'))

    query_vector = [1.0, 2.0, 3.0, 4.0].pack('f*')
    query = Query.new("*=>[KNN 2 @vec $query_vector]")
    result = index.search(query, params: ["query_vector", query_vector], dialect: 2)

    assert_equal 2, result[0]
    assert_equal 'doc1', result[1]
    assert_equal 'doc2', result[3]
  end

  def test_profile_search
    schema = Schema.build do
      text_field :title
    end
    index = r.create_index(@index_name, schema, prefix: "hsh17")

    index.add('doc1', title: 'Hello World')

    result = index.profile('SEARCH', 'QUERY', '@title:Hello')
    assert_kind_of Array, result
    assert_equal 2, result.length

    search_result, profile_info = result
    assert_kind_of Array, search_result
    assert_kind_of Array, profile_info

    total_profile_time = profile_info.find { |item| item[0] == "Total profile time" }
    refute_nil total_profile_time, "Total profile time not found in profile info"
    assert_equal 2, total_profile_time.length
    assert_kind_of String, total_profile_time[1]
  end

  def test_advanced_search_features
    schema = Schema.build do
      text_field :title
      tag_field :tag
      numeric_field :num
    end
    index = r.create_index(@index_name, schema, prefix: "hsh18")

    index.add('doc1', title: 'Hello World', tag: 'greeting', num: 100)
    index.add('doc2', title: 'Goodbye World', tag: 'farewell', num: 200)

    # Test tag field
    result = index.search('@tag:{greeting}')
    assert_equal 1, result[0]
    assert_equal 'doc1', result[1]

    # Test numeric range
    result = index.search('@num:[150 300]')
    assert_equal 1, result[0]
    assert_equal 'doc2', result[1]

    # Test combined query
    result = index.search('(@title:World) (@num:[0 150])')
    assert_equal 1, result[0]
    assert_equal 'doc1', result[1]
  end

  def test_error_handling
    assert_raises(Redis::CommandError, "Expected error when schema is not a Schema object") do
      r.create_index(@index_name, "not a schema object")
    end

    assert_raises(Redis::CommandError, "Expected error when field has invalid option") do
      schema = Schema.build do
        text_field :title
        text_field :body, invalid_option: true
      end
      r.create_index(@index_name, schema, prefix: "hsh19")
    end

    valid_schema = Schema.build do
      text_field :title
      numeric_field :score
    end
    index = r.create_index(@index_name, valid_schema, prefix: "hsh20")

    assert_raises(Redis::CommandError, "Expected error when adding document with invalid data type") do
      index.add('doc1', title: 'value', score: 'not_a_number')
    end
  end

  def test_query_builder
    schema = Schema.build do
      text_field :title
      tag_field :category
      numeric_field :score
    end
    index = r.create_index(@index_name, schema, prefix: "hsh24")

    index.add('doc1', title: 'Hello World', category: 'greeting', score: 0.5)
    index.add('doc2', title: 'Goodbye World', category: 'farewell', score: 1.0)

    query = Query.build do
      and_ do
        tag(:category).eq("greeting")
        text(:title).match("Hel*")
      end
    end
    query.filter(:score, 0.3, "+inf")

    result = index.search(query)
    assert_equal 1, result[0]
    assert_equal 'doc1', result[1]
  end

  def test_query_with_multiple_predicates_anded
    schema = Schema.build do
      text_field :title
      tag_field :category
      numeric_field :score
    end
    index = r.create_index(@index_name, schema, prefix: "hsh26")

    index.add('doc1', title: 'Hello World', category: 'greeting', score: 0.5)
    index.add('doc2', title: 'Hello Redis', category: 'tech', score: 0.8)
    index.add('doc3', title: 'Goodbye World', category: 'farewell', score: 1.0)

    query = Query.build do
      and_ do
        text(:title).match("Hello*")
        tag(:category).eq("tech")
      end
    end
    query.filter(:score, 0.4, 0.9)

    result = index.search(query)
    assert_equal 1, result[0]
    assert_equal 'doc2', result[1]
  end

  def test_query_with_multiple_predicates
    schema = Schema.build do
      text_field :title
      tag_field :category
      numeric_field :score
    end
    index = r.create_index(@index_name, schema, prefix: "hsh26")

    index.add('doc1', title: 'Hello World', category: 'greeting', score: 0.5)
    index.add('doc2', title: 'Hello Redis', category: 'tech', score: 0.8)
    index.add('doc3', title: 'Goodbye World', category: 'farewell', score: 1.0)

    query = Query.build do
      text(:title).match("Hello*")
      tag(:category).eq("tech")
    end
    query.filter(:score, 0.4, 0.9)

    result = index.search(query)
    assert_equal 1, result[0]
    assert_equal 'doc2', result[1]
  end

  def test_query_with_or_predicates
    schema = Schema.build do
      text_field :title
      tag_field :category
    end
    index = r.create_index(@index_name, schema, prefix: "hsh27")

    index.add('doc1', title: 'Hello World', category: 'greeting')
    index.add('doc2', title: 'Hello Redis', category: 'tech')
    index.add('doc3', title: 'Goodbye World', category: 'farewell')

    query = Query.build do
      or_ do
        text(:title).match("Hello*")
        tag(:category).eq("farewell")
      end
    end

    result = index.search(query)
    assert_equal 3, result[0]
    assert_includes result, 'doc1'
    assert_includes result, 'doc2'
    assert_includes result, 'doc3'
  end

  def test_complex_query_with_and_or
    schema = Schema.build do
      text_field :title
      tag_field :category
      numeric_field :score
    end
    index = r.create_index(@index_name, schema, prefix: "hsh28")

    index.add('doc1', title: 'Hello World', category: 'greeting', score: 0.5)
    index.add('doc2', title: 'Hello Redis', category: 'tech', score: 0.8)
    index.add('doc3', title: 'Goodbye World', category: 'farewell', score: 1.0)
    index.add('doc4', title: 'Hello Ruby', category: 'tech', score: 0.9)

    query = Query.build do
      or_ do
        and_ do
          text(:title).match("Hello*")
          tag(:category).eq("tech")
        end
        tag(:category).eq("farewell")
      end
    end
    query.filter(:score, 0.7, "+inf")

    result = index.search(query)
    assert_equal 3, result[0]
    assert_includes result, 'doc2'
    assert_includes result, 'doc3'
    assert_includes result, 'doc4'
  end

  def test_complex_nested_query
    schema = Schema.build do
      text_field :title
      tag_field :category
      text_field :author
      numeric_field :score
      numeric_field :year
    end
    index = r.create_index(@index_name, schema, prefix: "book")

    index.add('book1', title: 'Redis in Action', category: 'programming', author: 'Josiah Carlson', score: 4.5, year: 2013)
    index.add('book2', title: 'Redis Essentials', category: 'database', author: 'Maxwell Dayvson Da Silva', score: 4.0, year: 2015)
    index.add('book3', title: 'Redis Cookbook', category: 'programming', author: 'Tiago Macedo', score: 3.5, year: 2011)
    index.add('book4', title: 'Learning Redis', category: 'database', author: 'Vinoo Das', score: 4.2, year: 2015)
    index.add('book5', title: 'Redis Applied Design Patterns', category: 'programming', author: 'Arun Chinnachamy', score: 3.8, year: 2014)

    query = Query.build do
      or_ do
        and_ do
          text(:title).match("Redis*")
          or_ do
            tag(:category).eq("programming")
            and_ do
              tag(:category).eq("database")
              numeric(:year).gt(2014)
            end
          end
          numeric(:score).gt(4.0)
        end
        and_ do
          numeric(:score).between(4.5, 5.0)
          text(:author).match("Josiah Carlson")
        end
      end
    end

    result = index.search(query)

    assert_equal 2, result[0]
    assert_includes result, 'book1'
    assert_includes result, 'book4'
  end

  def test_query_methods
    schema = Schema.build do
      text_field :title
      tag_field :category
      numeric_field :price
    end
    index = r.create_index(@index_name, schema, prefix: "product")

    index.add('prod1', title: 'iPhone', category: 'electronics', price: 999)
    index.add('prod2', title: 'Galaxy', category: 'electronics', price: 799)
    index.add('prod3', title: 'Book', category: 'literature', price: 15)

    query = Query.new("@category:{electronics}")
                 .filter(:price, 0, 800)
                 .paging(0, 10)
                 .sort_by(:price, :desc)
                 .return(:title, :price)
                 .with_scores

    result = index.search(query)

    assert_equal 1, result[0]
    assert_equal 'prod2', result[1]
    assert_kind_of String, result[2] # score
    assert_equal ['price', '799', 'title', 'Galaxy'], result[3]
  end

  def test_language_specific_search
    schema = Schema.build do
      text_field :text
    end
    index = r.create_index(@index_name, schema, prefix: "lang")

    index.add('doc1', text: 'The quick brown fox')
    index.add('doc2', text: 'Le renard brun rapide')

    query_en = Query.new("quick").language(:english)
    result_en = index.search(query_en)

    assert_equal 1, result_en[0]
    assert_equal 'doc1', result_en[1]

    query_fr = Query.new("rapide").language(:french)
    result_fr = index.search(query_fr)

    assert_equal 1, result_fr[0]
    assert_equal 'doc2', result_fr[1]
  end

  def test_verbatim_and_no_stopwords
    schema = Schema.build do
      text_field :text
    end
    index = r.create_index(@index_name, schema, prefix: "verbatim")

    index.add('doc1', text: 'The quick brown fox')
    index.add('doc2', text: 'A slow yellow fox')

    query_normal = Query.new("the quick")
    result_normal = index.search(query_normal)

    assert_equal 1, result_normal[0]

    query_verbatim = Query.new("the quick").verbatim.no_stopwords
    result_verbatim = index.search(query_verbatim)

    assert_equal 0, result_verbatim[0]
  end

  def test_comprehensive_query
    schema = Schema.build do
      text_field :title, weight: 5.0
      tag_field :category
      numeric_field :price, sortable: true
      text_field :description
    end
    index = r.create_index(@index_name, schema, prefix: "comp")

    index.add('prod1', title: 'iPhone 12', category: 'electronics', price: 999, description: 'Latest model')
    index.add('prod2', title: 'Samsung Galaxy', category: 'electronics', price: 799, description: 'Android flagship')
    index.add('prod3', title: 'Kindle', category: 'electronics', price: 129, description: 'E-reader')
    index.add('prod4', title: 'Harry Potter', category: 'books', price: 15, description: 'Fantasy novel')

    query = Query.build do
      or_ do
        and_ do
          text(:title).match("iPhone|Galaxy")
          tag(:category).eq("electronics")
        end
        and_ do
          text(:description).match("reader")
          numeric(:price).between(100, 200)
        end
      end
    end

    query.filter(:price, 0, 1000)
         .paging(0, 5)
         .sort_by(:price, :desc)
         .return(:title, :price)
         .with_scores
         .language(:english)
         .slop(0)
         .in_order
         .verbatim
         .no_stopwords

    result = index.search(query)

    assert_equal 3, result[0]

    # Extract document IDs and their corresponding fields
    documents = result[1..-1].each_slice(3).map do |id, _score, fields|
      [id, Hash[*fields]]
    end

    assert_equal 3, documents.length

    document_ids = documents.map { |doc| doc[0] }
    assert_includes document_ids, 'prod1'
    assert_includes document_ids, 'prod2'
    assert_includes document_ids, 'prod3'
    refute_includes document_ids, 'prod4'

    # Check if results are sorted by price in descending order
    prices = documents.map { |doc| doc[1]['price'].to_f }
    assert_equal prices.sort.reverse, prices

    # Verify the order of results
    assert_equal ['prod1', 'prod2', 'prod3'], document_ids
  end

  def test_scores
    schema = Schema.build do
      text_field :txt
    end
    index = r.create_index(@index_name, schema, prefix: "score")

    index.add("doc1", txt: "foo baz")
    index.add("doc2", txt: "foo bar")

    query = Query.new("foo ~bar").with_scores
    result = index.search(query)

    assert_equal 2, result[0]
    assert_equal "doc2", result[1]
    assert_in_delta 3.0, result[2].to_f, 0.1
    assert_equal ["txt", "foo bar"], result[3]
    assert_equal "doc1", result[4]
    assert_in_delta 1.0, result[5].to_f, 0.1
    assert_equal ["txt", "foo baz"], result[6]
  end

  def test_stopwords
    schema = Schema.build do
      text_field :txt
    end
    index = r.create_index(@index_name, schema, stopwords: ["foo", "bar", "baz"])

    index.add("doc1", txt: "foo bar")
    index.add("doc2", txt: "hello world")

    query1 = Query.new("foo bar").no_content
    query2 = Query.new("foo bar hello world").no_content
    res1, res2 = index.search(query1), index.search(query2)

    assert_equal 0, res1[0]
    assert_equal 1, res2[0]
  end

  def test_explain
    schema = Schema.build do
      text_field :title
      text_field :body
    end

    index = r.create_index("idx", schema)

    explanation = index.explain("@title:Hello @body:World")
    assert_includes explanation, "@title:UNION"
    assert_includes explanation, "@title:hello"
    assert_includes explanation, "@body:UNION"
    assert_includes explanation, "@body:world"
  end

  def test_summarize
    schema = Schema.build do
      text_field :play, weight: 5.0
      text_field :txt
    end

    index = r.create_index("idx", schema)

    index.add("doc1", play: "Henry IV", txt: "ACT I SCENE I. London. The palace. Enter KING HENRY, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR WALTER BLUNT, and others.")

    query = Query.new("king henry")
    query.highlight(fields: %i[play txt], tags: ["<b>", "</b>"])
    query.summarize(fields: [:txt])

    result = index.search(query)

    assert_equal 1, result[0] # Total results
    assert_equal "doc1", result[1] # Document ID

    # Extract the fields from the result
    fields = result[2].each_slice(2).to_h

    assert_equal "<b>Henry</b> IV", fields["play"]
    assert_includes fields["txt"], "Enter <b>KING</b> <b>HENRY</b>"
  end

  def test_alias
    schema = Schema.build do
      text_field :title
    end

    index1 = r.create_index("idx1", schema)
    index2 = r.create_index("idx2", schema)

    index1.add("doc1", title: "Hello World")
    index2.add("doc2", title: "Goodbye World")

    assert_equal "OK", r.ft_aliasadd("myalias", "idx1")

    result = r.ft_search("myalias", "Hello")
    assert_equal 1, result[0]
    assert_equal "doc1", result[1]

    assert_raises(Redis::CommandError) do
      r.ft_aliasadd("myalias", "idx2")
    end

    assert_equal "OK", r.ft_aliasupdate("myalias", "idx2")

    result = r.ft_search("myalias", "Goodbye")
    assert_equal 1, result[0]
    assert_equal "doc2", result[1]

    assert_equal "OK", r.ft_aliasdel("myalias")

    assert_raises(Redis::CommandError) do
      r.ft_search("myalias", "Goodbye")
    end
  end

  def test_phonetic_matcher
    schema = Schema.build do
      text_field :name, phonetic: "dm:en"
    end

    index = r.create_index("idx", schema)

    index.add("doc1", name: "Jon")
    index.add("doc2", name: "John")

    result = index.search(Query.new("Jon"))
    assert_equal 2, result[0]
    assert_includes [result[1], result[3]], "doc1"
    assert_includes [result[1], result[3]], "doc2"
  end

  def teardown
    r.ft_dropindex(@index_name, delete_documents: true)
  rescue
    nil
  end
end
